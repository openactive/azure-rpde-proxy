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

            log.LogInformation($"PollQueueHandler queue trigger function processed message: {feedStateItem?.nextUrl}");

            // Increment poll requests before anything else
            feedStateItem.totalPollRequests++;
            feedStateItem.dateModified = DateTime.Now;

            SourcePage sourcePage;

            try
            {
                sourcePage = await ExecutePoll(feedStateItem.name, feedStateItem.nextUrl, feedStateItem.lastPageReads == 0, feedStateItem.deletedItemDaysToLive, log);
            }
            catch (Exception ex)
            {
                var expectedException = ExpectedPollException.ExpectTheUnexpected(ex);
                feedStateItem.retryStategy = new RetryStrategy(expectedException, feedStateItem?.retryStategy);
                feedStateItem.totalErrors++;

                if (feedStateItem.retryStategy.DeadLetter)
                {
                    log.LogError(expectedException.RenderMessageWithFullContext(feedStateItem, $"DEAD-LETTERING: '{feedStateItem.name}'."));
                    await messageReceiver.DeadLetterAsync(lockToken);
                }
                if (feedStateItem.retryStategy.DropImmediately)
                {
                    log.LogWarning(expectedException.RenderMessageWithFullContext(feedStateItem, $"Dropped message for '{feedStateItem.name}'."));
                    await messageReceiver.CompleteAsync(lockToken);
                }
                else
                {
                    feedStateItem.lastError = expectedException.RenderMessageWithFullContext(feedStateItem, $"Retrying '{feedStateItem.name}' attempt {feedStateItem.retryStategy.RetryCount} in {feedStateItem.retryStategy.DelaySeconds} seconds.");
                    log.LogWarning(feedStateItem.lastError);
                    
                    var retryMsg = feedStateItem.EncodeToMessage(feedStateItem.retryStategy.DelaySeconds);

                    // Check lock exists, as close to a transaction as we can get
                    if (await messageReceiver.RenewLockAsync(lockToken) != null)
                    {
                        await messageReceiver.CompleteAsync(lockToken);
                        await queueCollector.AddAsync(retryMsg);
                    }
                }

                return;
            }

            // Update counters on success
            if (sourcePage.IsLastPage)
            {
                feedStateItem.lastPageReads++;
            } else
            {
                feedStateItem.lastPageReads = 0;
            }
            feedStateItem.retryStategy = null;
            feedStateItem.lastError = null;
            feedStateItem.totalPagesRead++;
            feedStateItem.totalItemsRead += sourcePage.Content.items.Count;
            feedStateItem.nextUrl = sourcePage.Content.next;

            Message newMessage;
            // If immediate poll is specified for last page, respect any Expires header provided for throttling
            if (sourcePage.IsLastPage)
            {
                if (sourcePage.LastPageDetails?.Expires != null)
                {
                    newMessage = feedStateItem.EncodeToMessage(sourcePage.LastPageDetails.Expires);
                }
                else if (sourcePage.LastPageDetails?.MaxAge != null)
                {
                    newMessage = feedStateItem.EncodeToMessage((int)sourcePage.LastPageDetails.MaxAge?.TotalSeconds);
                } else
                {
                    // Default last page polling interval
                    newMessage = feedStateItem.EncodeToMessage(Utils.DEFAULT_POLL_INTERVAL);
                }
            } else
            {
                // If not last page, get the next page immediately
                newMessage = feedStateItem.EncodeToMessage(0);
            }

            // These two operations should be in a transaction, but to save cost they ordered so that a failure will result in the polling stopping,
            // and the ResyncDroppedFeeds timer trigger will hence be required to ensure the system is still robust
            // using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            // {
            //    await messageReceiver.CompleteAsync(lockToken);
            //    await queueCollector.AddAsync(newMessage);
            //    scope.Complete(); // declare the transaction done
            // }

            // Check lock exists, as close to a transaction as we can get
            if (await messageReceiver.RenewLockAsync(lockToken) != null)
            {
                await messageReceiver.CompleteAsync(lockToken);
                await queueCollector.AddAsync(newMessage);
            }
        }

        
        private static async Task<SourcePage> ExecutePoll(string name, string nextUrl, bool IsFirstLastPage, int deletedItemDaysToLive, ILogger log)
        {
            var sw = new Stopwatch();

            if (Environment.GetEnvironmentVariable("ClearProxyCache")?.ToString() == "true")
            {
                throw new ExpectedPollException(ExpectedErrorCategory.ForceClearProxyCache);
            }

            sw.Start();
            var sourcePage = await GetSourcePage(nextUrl);
            sw.Stop();
            log.LogWarning($"FETCH TIMER {name}: {sw.ElapsedMilliseconds} ms to fetch {sourcePage?.Content?.items?.Count ?? 0} items.");

            var cacheItems = ConvertToCachedRpdeItems(sourcePage.Content, name, deletedItemDaysToLive);

            // Only write to the database on the first last page read
            if (sourcePage.IsLastPage && IsFirstLastPage)
            {
                // Add headers into data of dummy last item so FeedPage does not need an additional database query
                // to access them
                cacheItems.Add(LastPageCachedRpdeItem(sourcePage.LastPageDetails, name));
            }

            // Only write to database if there are items to write
            if (cacheItems.Count > 0)
            {
                sw.Start();
                var rowsUpdated = await WriteCachedRpdeItemsToDatabase(cacheItems);
                sw.Stop();
                log.LogWarning($"POLL TIMER {name}: {sw.ElapsedMilliseconds} ms to import {cacheItems.Count} items to database (writing {rowsUpdated} rows).");
            }

            return sourcePage;
        }
   
        private static async Task<int> WriteCachedRpdeItemsToDatabase(List<CachedRpdeItem> cacheItems)
        {
            try
            {
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
                if (rowsUpdated == 0 && cacheItems.Count > 0)
                {
                    throw new ExpectedPollException(ExpectedErrorCategory.DuplicateMessageDetected);
                }

                return rowsUpdated;
            }
            catch (SqlException ex)
            {
                if (SqlUtils.SqlTransientErrorNumbers.Contains(ex.Number) || ex.Message.Contains("timeout", StringComparison.InvariantCultureIgnoreCase))
                {
                    throw new ExpectedPollException(ExpectedErrorCategory.SqlTransientError, ex);
                }
                else
                {
                    throw new ExpectedPollException(ExpectedErrorCategory.SqlUnexpectedError, ex);
                }
            }
            catch (Exception ex)
            {
                throw new ExpectedPollException(ExpectedErrorCategory.UnexpectedErrorDuringDatabaseWrite, ex);
            }
        }

        private static CachedRpdeItem LastPageCachedRpdeItem(LastItem lastItem, string source)
        {
            return new CachedRpdeItem
            {
                id = Utils.LAST_PAGE_ITEM_RESERVED_ID,
                modified = Utils.LAST_PAGE_ITEM_RESERVED_MODIFIED,
                deleted = false,
                data = JsonConvert.SerializeObject(lastItem, Newtonsoft.Json.Formatting.None,
                new JsonSerializerSettings
                {
                    NullValueHandling = NullValueHandling.Ignore
                }),
                kind = string.Empty,
                source = source,
                expiry = null
            };
        }

        private static List<CachedRpdeItem> ConvertToCachedRpdeItems(RpdeFeed data, string source, int deletedItemDaysToLive)
        {
            return data.items.Select(item => item.ConvertToStringId()).Select(item => new CachedRpdeItem
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
                source = source,
                expiry = item.state == "deleted" ? DateTime.UtcNow.AddDays(deletedItemDaysToLive) : (DateTime?)null
            }).ToList();
        }

        private static async Task<SourcePage> GetSourcePage(string nextUrl)
        {
            // Attempt to get next page
            SourcePage sourcePage = new SourcePage();
            try
            {
                var reponse = await httpClient.GetAsync(nextUrl);

                // Record time of response, to help calculate last page details
                var dateTimeReceived = DateTimeOffset.UtcNow;

                if (reponse.StatusCode == HttpStatusCode.Unauthorized)
                {
                    // 401 most likely indicates OWS key has changed
                    throw new ExpectedPollException(ExpectedErrorCategory.Unauthorised401);
                }

                // Attempt to Deserialize
                var data = JsonConvert.DeserializeObject<RpdeFeed>(await reponse.Content.ReadAsStringAsync());

                if (data?.license != Utils.CC_BY_LICENSE || data?.next == null || data?.items == null)
                {
                    // Invalid RPDE Page indicates error that should be retried
                    throw new ExpectedPollException(ExpectedErrorCategory.InvalidRPDEPage);
                }

                // Only store last page details if this is the last page
                if (data?.items?.Count == 0 && data?.next == nextUrl)
                {
                    sourcePage.LastPageDetails = GetLastPageDetailsFromHeader(reponse, dateTimeReceived);
                }

                sourcePage.Content = data;

                return sourcePage;
            }
            catch (Exception ex)
            {
                throw new ExpectedPollException(ExpectedErrorCategory.PageFetchError, ex);
            }
        }

        private static LastItem GetLastPageDetailsFromHeader(HttpResponseMessage response, DateTimeOffset dateTimeReceived)
        {
            int? recommendedPollInterval = null;
            if (response.Headers.TryGetValues(Utils.RECOMMENDED_POLL_INTERVAL_HEADER, out IEnumerable<string> recommendedPollIntervalString))
            {
                if (Int32.TryParse(recommendedPollIntervalString.FirstOrDefault(), out int intervalNumeric))
                {
                    recommendedPollInterval = intervalNumeric;
                }
            }

            return new LastItem
            {
                // Store headers for use with last page
                MaxAge = response.Headers.CacheControl.MaxAge,
                RecommendedPollInterval = recommendedPollInterval,
                // The Expires from a source may be wildly inaccurate if the source server does not synchronize time with an external NTP server
                // The date of the source server is compared with the date at the proxy to discern the intended expiry.
                // If the suggested expiry is too short, the minimum is used.
                Expires = AdjustAndValidateExpires(response.Content.Headers.Expires, response.Headers.Date, dateTimeReceived, recommendedPollInterval)
            };
        }

        private static DateTimeOffset? AdjustAndValidateExpires(DateTimeOffset? expires, DateTimeOffset? dateTimeSent, DateTimeOffset dateTimeRecieved, int? recommendedPollIntervalSeconds)
        {
            // Treat the expires as non-existant if it cannot be adjusted
            if (!expires.HasValue || !dateTimeSent.HasValue) return null;

            var originExpires = (DateTimeOffset)expires;
            var originDate = (DateTimeOffset)dateTimeSent;
            
            // Get timespan until expiry, based on origin time
            var timespanUntilExpiry = originExpires.Subtract(originDate);
            
            // Validate the result to ensure proxy is not held hostage by inaccurate data provided
            var expiresFromNowInSeconds = timespanUntilExpiry.TotalSeconds;
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
                return dateTimeRecieved.AddSeconds(maxInterval);
            }
            else if (expiresFromNowInSeconds < minInterval)
            {
                // Limit the minimum to control load on the origin server
                return dateTimeRecieved.AddSeconds(minInterval);
            }
            else
            {
                // Add timespan to proxy time to get adjusted date
                return dateTimeRecieved.Add(timespanUntilExpiry);
            }
        }
    }

    public class SourcePage
    {
        public bool IsLastPage
        {
            get { return LastPageDetails != null; }
        }
        public LastItem LastPageDetails { get; set; }
        public RpdeFeed Content { get; set; }
    }

    public enum ExpectedErrorCategory
    {
        Unauthorised401,
        InvalidRPDEPage,
        PageFetchError,
        DuplicateMessageDetected,
        SqlTransientError,
        SqlUnexpectedError,
        UnexpectedErrorDuringDatabaseWrite,
        ForceClearProxyCache,
        UnexpectedError
    }

    public class ExpectedPollException : Exception
    {
        public static ExpectedPollException ExpectTheUnexpected(Exception ex)
        {
            return ex as ExpectedPollException ?? new ExpectedPollException(ExpectedErrorCategory.UnexpectedError, ex);
        }

        public ExpectedPollException(ExpectedErrorCategory errorCategory, Exception innerException = null) : base("ExpectedPollException was thrown without rendering", innerException)
        {
            this.ErrorCategory = errorCategory;
        }
           
        public ExpectedErrorCategory ErrorCategory { get; set; }

        public string RenderMessageWithFullContext(FeedState feedStateItem, string retryContext)
        {
            return $"{RenderMessageWithContext(feedStateItem)}. {retryContext} {this.InnerException?.ToString()}";
        }

        public string RenderMessageWithContext(FeedState feedStateItem)
        {
            switch (ErrorCategory)
            {
                case ExpectedErrorCategory.Unauthorised401:
                    return $"Feed attempting poll returned 401 and will be purged: '{feedStateItem.name}'.";
                case ExpectedErrorCategory.DuplicateMessageDetected:
                    return $"Duplicate message detected for '{feedStateItem.name}': {feedStateItem.id}, and will be dropped.";
                case ExpectedErrorCategory.InvalidRPDEPage:
                    return $"Invalid RPDE page received for '{feedStateItem.name}': {feedStateItem.nextUrl}";
                case ExpectedErrorCategory.PageFetchError:
                    return $"Error retrieving page for '{feedStateItem.name}': {feedStateItem.nextUrl}";
                case ExpectedErrorCategory.SqlTransientError:
                    return $"Throttle on PollFeed, retry after {SqlUtils.SqlRetrySecondsRecommendation} seconds.";
                case ExpectedErrorCategory.SqlUnexpectedError:
                    return $"SQL Error {(this.InnerException as SqlException)?.Number} writing page to SQL Server.";
                case ExpectedErrorCategory.UnexpectedErrorDuringDatabaseWrite:
                    return $"Unexpected error writing page to SQL Server.";
                case ExpectedErrorCategory.UnexpectedError:
                    return $"Unexpected error.";
                case ExpectedErrorCategory.ForceClearProxyCache:
                    return $"ClearProxyCache has been set in App Config. Deadlettering all feeds.";
                default:
                    return $"Unexpected error for '{feedStateItem.name}': {feedStateItem.nextUrl}";
            }
        }
    }

    public class RetryStrategy
    {
        // Default constructor used when deserialising from queue
        [JsonConstructor]
        public RetryStrategy() { }

        public RetryStrategy(ExpectedPollException ex, RetryStrategy lastRetryStategy)
        {
            this.ErrorCategory = ex.ErrorCategory;

            if (this.ErrorCategory != lastRetryStategy?.ErrorCategory)
            {
                this.RetryCount = 0;
            }
            else
            {
                this.RetryCount = (lastRetryStategy?.RetryCount ?? 0) + 1;
            }

            switch (this.ErrorCategory)
            {
                case ExpectedErrorCategory.Unauthorised401:
                case ExpectedErrorCategory.DuplicateMessageDetected:
                    this.DropImmediately = true;
                    break;
                case ExpectedErrorCategory.InvalidRPDEPage:
                case ExpectedErrorCategory.PageFetchError:
                case ExpectedErrorCategory.UnexpectedErrorDuringDatabaseWrite:
                case ExpectedErrorCategory.UnexpectedError:
                    // Exponential backoff
                    if (this.RetryCount > 15)
                    {
                        this.DeadLetter = true;
                    } else
                    {
                        this.DelaySeconds = (int)BigInteger.Pow(2, this.RetryCount);
                    }
                    break;
                case ExpectedErrorCategory.ForceClearProxyCache:
                    this.DeadLetter = true;
                    break;
                case ExpectedErrorCategory.SqlTransientError:
                    this.DelaySeconds = SqlUtils.SqlRetrySecondsRecommendation;
                    break;
                default:
                    throw new InvalidOperationException("Unknown Retry Strategy");
            }
        }
        
        // String useful for status endpoint output
        public string ErrorCategoryInfo
        {
            get { return ErrorCategory.ToString(); }
        }
        public ExpectedErrorCategory ErrorCategory;
        public bool DeadLetter { get; set; }
        public bool DropImmediately { get; set; }
        public int DelaySeconds { get; set; }
        public int RetryCount { get; set; }
    }
}
