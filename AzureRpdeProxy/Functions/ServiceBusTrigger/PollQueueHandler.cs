using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using NPoco;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Numerics;
using System.Threading.Tasks;
using System.Transactions;
using System.Web;

namespace AzureRpdeProxy
{
    public static class PollFeed
    {
        private static readonly HttpClient httpClient;

        static PollFeed()
        {
            httpClient = new HttpClient();
        }

        [FunctionName("PollQueueHandler")]
        public static async Task Run([ServiceBusTrigger(Utils.FEED_STATE_QUEUE_NAME, Connection = "ServiceBusConnection")] Message message, MessageReceiver messageReceiver, string lockToken,
            [ServiceBus(Utils.FEED_STATE_QUEUE_NAME, Connection = "ServiceBusConnection", EntityType = EntityType.Queue)] IAsyncCollector<Message> queueCollector,
             ILogger log)
        {
            var feedStateItem = FeedState.DecodeFromMessage(message);

            log.LogInformation($"PollFeed queue trigger function processed message: {feedStateItem?.nextUrl}");

            // Increment poll requests before anything else
            feedStateItem.totalPollRequests++;
            feedStateItem.dateModified = DateTime.Now;

            int delaySeconds = 0;
            bool isLastPage = false;

            // Store headers of last page to be passed on
            DateTimeOffset? expires = null;
            TimeSpan? maxAge = null;
            int? recommendedPollInterval = null;

            // Attempt to get next page
            RpdeFeed data = null;
            try
            {
                var sw = new Stopwatch();
                sw.Start();

                var result = await httpClient.GetAsync(feedStateItem.nextUrl);
                if (result.StatusCode == HttpStatusCode.Unauthorized)
                {
                    // Deadletter on 401 (OWS key has changed)
                    log.LogWarning($"Feed attempting poll returned 401 and will be purged: '{feedStateItem.name}'.");
                    delaySeconds = -1;
                }
                else
                {
                    // Store headers for use with last page
                    maxAge = result.Headers.CacheControl.MaxAge;
                    if (result.Headers.TryGetValues(Utils.RECOMMENDED_POLL_INTERVAL_HEADER, out IEnumerable<string> recommendedPollIntervalString)) {
                        if (Int32.TryParse(recommendedPollIntervalString.FirstOrDefault(), out int intervalNumeric))
                        {
                            recommendedPollInterval = intervalNumeric;
                        }
                    }

                    // The Expires from a source may be wildly inaccurate if the source server does not synchronize time with an external NTP server
                    // The date of the source server is compared with the date at the proxy to discern the intended expiry.
                    // If the suggested expiry is too short, the minimum is used.
                    expires = AdjustAndValidateExpires(result.Content.Headers.Expires, result.Headers.Date, recommendedPollInterval);

                    data = JsonConvert.DeserializeObject<RpdeFeed>(await result.Content.ReadAsStringAsync());
                }

                sw.Stop();
                log.LogWarning($"FETCH TIMER {feedStateItem.name}: {sw.ElapsedMilliseconds} ms to fetch {data?.items?.Count ?? 0} items.");
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Error retrieving page: " + feedStateItem.nextUrl);
            }

            if (delaySeconds == -1)
            {
                // Do nothing to immediately deadletter this response
            }
            // check for valid RPDE base properties
            else if (data?.license == Utils.CC_BY_LICENSE && data?.next != null && data?.items != null)
            {
                var cacheItems = data.items.Select(item => item.ConvertToStringId()).Select(item => new CachedRpdeItem
                {
                    id = item.id,
                    modified = item.modified,
                    deleted = item.state == "deleted",
                    // Note must be manually serialised to pass to stored procedure, which ignores object annotations
                    data = JsonConvert.SerializeObject(item, Newtonsoft.Json.Formatting.None,
                    new JsonSerializerSettings
                    {
                        NullValueHandling = NullValueHandling.Ignore
                    }),
                    kind = item.kind,
                    source = feedStateItem.name,
                    expiry = item.state == "deleted" ? DateTime.UtcNow.AddDays(feedStateItem.deletedItemDaysToLive) : (DateTime?)null
                }).ToList();

                if (cacheItems.Count == 0 && feedStateItem.nextUrl == data.next)
                {
                    isLastPage = true;

                    if (feedStateItem.lastPageReads == 0)
                    {
                        // Add headers into data of dummy last item so FeedPage does not need an additional database query
                        // to access them
                        cacheItems.Add(new CachedRpdeItem
                        {
                            id = Utils.LAST_PAGE_ITEM_RESERVED_ID,
                            modified = Utils.LAST_PAGE_ITEM_RESERVED_MODIFIED,
                            deleted = false,
                            data = JsonConvert.SerializeObject(new LastItem
                            {
                                Expires = expires,
                                MaxAge = maxAge,
                                RecommendedPollInterval = recommendedPollInterval

                            }, Newtonsoft.Json.Formatting.None,
                            new JsonSerializerSettings
                            {
                                NullValueHandling = NullValueHandling.Ignore
                            }),
                            kind = string.Empty,
                            source = feedStateItem.name,
                            expiry = null
                        });
                    }

                    feedStateItem.lastPageReads++;
                    feedStateItem.pollRetries = 0;
                }
                else
                {
                    feedStateItem.lastPageReads = 0;
                    feedStateItem.totalPagesRead++;
                    feedStateItem.totalItemsRead += cacheItems.Count;
                    feedStateItem.nextUrl = data.next;
                    feedStateItem.pollRetries = 0;
                }

                // Only write to database if there are items to write
                if (cacheItems.Count > 0)
                {
                    try
                    {
                        var sw = new Stopwatch();
                        sw.Start();

                        int rowsUpdated = 0;

                        using (SqlConnection connection = new SqlConnection(SqlUtils.SqlDatabaseConnectionString))
                        {
                            connection.Open();

                            DataTable table = new DataTable();
                            table.Columns.Add("source", typeof(string));
                            table.Columns.Add("id", typeof(string));
                            table.Columns.Add("modified", typeof(long));
                            table.Columns.Add("kind", typeof(string));
                            table.Columns.Add("deleted", typeof(bool));
                            table.Columns.Add("data", typeof(string));
                            table.Columns.Add("expiry", typeof(DateTime));
                            foreach (var item in cacheItems)
                            {
                                table.Rows.Add(item.source, item.id, item.modified, item.kind, item.deleted, item.data, item.expiry);
                            }

                            SqlCommand cmd = new SqlCommand("UPDATE_ITEM_BATCH", connection);
                            cmd.CommandType = CommandType.StoredProcedure;

                            cmd.Parameters.Add(
                                new SqlParameter()
                                {
                                    ParameterName = "@Tvp",
                                    SqlDbType = SqlDbType.Structured,
                                    TypeName = "ItemTableType",
                                    Value = table,
                                });

                            rowsUpdated = await cmd.ExecuteNonQueryAsync();
                        }
                        
                        // If no rows are updated, message must be a duplicate, as previous message did the work
                        if (rowsUpdated == 0 && !isLastPage)
                        {
                            log.LogWarning($"DUPLICATE MESSAGE DROPPED {feedStateItem.name}: {feedStateItem.id}");
                            await messageReceiver.CompleteAsync(lockToken);
                            return;
                        }

                        sw.Stop();
                        log.LogWarning($"POLL TIMER {feedStateItem.name}: {sw.ElapsedMilliseconds} ms to import {cacheItems.Count} items.");
                    }
                    catch (SqlException ex)
                    {
                        if (SqlUtils.SqlTransientErrorNumbers.Contains(ex.Number))
                        {
                            log.LogWarning($"Throttle on PollFeed, retry after {SqlUtils.SqlRetrySecondsRecommendation} seconds.");
                            delaySeconds = SqlUtils.SqlRetrySecondsRecommendation;
                            feedStateItem.pollRetries = 0;
                        }
                        else
                        {
                            feedStateItem.pollRetries++;
                            feedStateItem.totalErrors++;
                            delaySeconds = (int)BigInteger.Pow(2, feedStateItem.pollRetries);
                            log.LogWarning($"Error writing page to SQL Server {ex.Number}: Retrying '{feedStateItem.name}' attempt {feedStateItem.pollRetries} in {delaySeconds} seconds. Error: " + ex.ToString());
                        }
                    }
                    catch (Exception ex)
                    {
                        feedStateItem.pollRetries++;
                        feedStateItem.totalErrors++;
                        delaySeconds = (int)BigInteger.Pow(2, feedStateItem.pollRetries);
                        log.LogWarning($"Error writing page to SQL Server: Retrying '{feedStateItem.name}' attempt {feedStateItem.pollRetries} in {delaySeconds} seconds. Error: " + ex.ToString());
                    }
                }
            }
            else
            {
                if (feedStateItem.pollRetries > 15) // Retry with exponential backoff for 18 hours, then fail and purge regardless of the error type
                {
                    log.LogError($"Error retrieving page: DEAD-LETTERING '{feedStateItem.name}'");

                    delaySeconds = -1;
                }
                else
                {
                    feedStateItem.pollRetries++;
                    feedStateItem.totalErrors++;
                    delaySeconds = (int)BigInteger.Pow(2, feedStateItem.pollRetries);
                    log.LogWarning($"Error retrieving page: Retrying '{feedStateItem.name}' attempt {feedStateItem.pollRetries} in {delaySeconds} seconds");
                }
            }

            // Move all to DeadLetter if ClearProxyCache is enabled
            if (delaySeconds < 0 || Environment.GetEnvironmentVariable("ClearProxyCache")?.ToString() == "true")
            {
                await messageReceiver.DeadLetterAsync(lockToken);
            } else
            {
                Message newMessage;
                // If immediate poll is specified for last page, respect any Expires header provided for throttling
                if (delaySeconds == 0 && isLastPage)
                {
                    if (expires != null)
                    {
                        newMessage = feedStateItem.EncodeToMessage(expires);
                    }
                    else if (maxAge != null)
                    {
                        newMessage = feedStateItem.EncodeToMessage((int)maxAge?.TotalSeconds);
                    } else
                    {
                        // Default last page polling interval
                        newMessage = feedStateItem.EncodeToMessage(Utils.DEFAULT_POLL_INTERVAL);
                    }
                } else
                {
                    // If not last page, follow delaySeconds
                    newMessage = feedStateItem.EncodeToMessage(delaySeconds);
                }
                
                // These two operations should be in a transaction, but to save cost they ordered so that a failure will result in the polling stopping,
                // and a reregistration being required to restart it (24 hrs later)
                //using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
                //{
                await messageReceiver.CompleteAsync(lockToken);
                await queueCollector.AddAsync(newMessage);
                    
                //    scope.Complete(); // declare the transaction done
                //}
                
            }
        }

        private static DateTimeOffset? AdjustAndValidateExpires(DateTimeOffset? expires, DateTimeOffset? date, int? recommendedPollIntervalSeconds)
        {
            // Treat the expires as non-existant if it cannot be adjusted
            if (!expires.HasValue || !date.HasValue) return null;

            var originExpires = (DateTimeOffset)expires;
            var originDate = (DateTimeOffset)date;
            
            // Get timespan until expiry, based on origin time
            var timespanUntilExpiry = originExpires.Subtract(originDate);

            // Add timespan to proxy time to get adjusted date
            var proxyExpires = DateTimeOffset.UtcNow.Add(timespanUntilExpiry);

            // Validate the result to ensure proxy is not held hostage by inaccurate data provided
            var expiresFromNowInSeconds = DateTimeOffset.UtcNow.Subtract(proxyExpires).TotalSeconds;
            var maxInterval = (recommendedPollIntervalSeconds ?? Utils.MAX_POLL_INTERVAL) * 1.5; // 1.5 allows for adjustment to be made to back to sync
            var minInterval = Utils.MIN_POLL_INTERVAL;
            
            if (expiresFromNowInSeconds < 0)
            {
                // If we've been given an expiry date in the response that is already in the past, even after adjustment,
                // throw it directly in the bin
                return null;
            }
            else if (expiresFromNowInSeconds > maxInterval)
            {
                // Limit the maximum exposure to inaccurate expires, so we don't get out of sync
                return DateTimeOffset.UtcNow.AddSeconds(maxInterval);
            }
            else if (expiresFromNowInSeconds < minInterval)
            {
                // Limit the minimum to control load on the origin server
                return DateTimeOffset.UtcNow.AddSeconds(minInterval);
            }
            else
            {
                // If within range then valid
                return proxyExpires;
            }
        }
    }
}
