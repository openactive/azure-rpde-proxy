using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using NPoco;
using System;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace AzureRpdeProxy
{
    public static class RegistrationQueue
    {
        private static readonly HttpClient httpClient;

        static RegistrationQueue()
        {
            httpClient = new HttpClient();
        }

        [FunctionName("RegistrationQueueHandler")]
        public static async Task Run([ServiceBusTrigger(Utils.REGISTRATION_QUEUE_NAME, Connection = "ServiceBusConnection")] Message message, MessageReceiver messageReceiver, string lockToken,
            [ServiceBus(Utils.REGISTRATION_QUEUE_NAME, Connection = "ServiceBusConnection", EntityType = EntityType.Queue)] IAsyncCollector<Message> queueCollector,
            [ServiceBus(Utils.FEED_STATE_QUEUE_NAME, Connection = "ServiceBusConnection", EntityType = EntityType.Queue)] IAsyncCollector<Message> feedStateQueueCollector,
            ILogger log)
        {

            var feedStateItem = FeedState.DecodeFromMessage(message);

            // Dead letter queue from PollFeed simply triggers a purge

            log.LogInformation($"Registration Trigger Started: {feedStateItem.name}");

            // Double-check this feed doesn't already exist
            var matchingFeedStateList = await Utils.GetFeedStateFromQueues(feedStateItem.name, true);
            if (matchingFeedStateList.Count > 0)
            {
                if (matchingFeedStateList.First().url != feedStateItem.url)
                {
                    log.LogError($"Conflicting feed already registered with same name '{feedStateItem.name}' using different url '{matchingFeedStateList.First().url}', and will be dropped.");
                } else
                {
                    log.LogError($"Conflicting feed already registered with same name '{feedStateItem.name}' using same url '{matchingFeedStateList.First().url}', and will be dropped.");
                }
                await messageReceiver.CompleteAsync(lockToken);
                return;
            }

            // Attempt to get first page
            try
            {
                var result = await httpClient.GetAsync(feedStateItem.nextUrl);
                if (result.StatusCode == HttpStatusCode.Unauthorized)
                {
                    // Remove from queue on 401 (OWS key has changed)
                    log.LogWarning($"Feed attempting registration returned 401 and will be dropped: '{feedStateItem.name}'.");
                    DeleteFeedFromDatabase(feedStateItem.name);
                    await messageReceiver.CompleteAsync(lockToken);
                    return;
                }
                else
                {
                    var data = JsonConvert.DeserializeObject<RpdeFeed>(await result.Content.ReadAsStringAsync());

                    // check for "license" and valid data: "https://creativecommons.org/licenses/by/4.0/"
                    if (data?.license != Utils.CC_BY_LICENSE)
                    {
                        throw new ApplicationException($"Registration error while validating first page - dropping feed. Error retrieving license for '{feedStateItem.url}'.");
                    }
                }
            }
            catch (Exception ex)
            {
                feedStateItem.lastError = ex.ToString();

                // Retry registration three times over 30 minutes, then fail
                if (feedStateItem.registrationRetries > 3)
                {
                    log.LogError($"Registration error while validating first page. Dropping feed. Error retrieving '{feedStateItem.url}'. {ex.ToString()}");
                    DeleteFeedFromDatabase(feedStateItem.name);
                    await messageReceiver.CompleteAsync(lockToken);
                }
                else
                {
                    feedStateItem.registrationRetries++;
                    feedStateItem.totalErrors++;
                    log.LogWarning($"Registration error while validating first page. Retrying '{feedStateItem.name}' attempt {feedStateItem.registrationRetries}. Error retrieving '{feedStateItem.url}'. {ex.ToString()}");

                    // Check lock exists, as close to a transaction as we can get
                    if (await messageReceiver.RenewLockAsync(lockToken) != null)
                    {
                        await messageReceiver.CompleteAsync(lockToken);
                        await queueCollector.AddAsync(feedStateItem.EncodeToMessage(30));
                    }
                }
                return;
            }

            // Write the successful registration to the feeds table
            using (var db = new Database(SqlUtils.SqlDatabaseConnectionString, DatabaseType.SqlServer2012, SqlClientFactory.Instance))
            {
                db.Save<Feed>(new Feed
                {
                    source = feedStateItem.name,
                    url = feedStateItem.url,
                    datasetUrl = feedStateItem.datasetUrl,
                    initialFeedState = feedStateItem
                });
            }

            // Restart the feed from the beginning
            feedStateItem.nextUrl = feedStateItem.url;
            feedStateItem.ResetCounters();

            // Check lock exists, as close to a transaction as we can get
            if (await messageReceiver.RenewLockAsync(lockToken) != null)
            {
                await messageReceiver.CompleteAsync(lockToken);
                await feedStateQueueCollector.AddAsync(feedStateItem.EncodeToMessage(0));
            }

            log.LogInformation($"Registration Trigger Promoting Feed: {feedStateItem.name}");
        }

        public static void DeleteFeedFromDatabase(string name)
        {
            // Delete the successfully the feed from the feeds table (if a failing feed has just been purged before being sent here, this is the final part of cleanup)
            using (var db = new Database(SqlUtils.SqlDatabaseConnectionString, DatabaseType.SqlServer2012, SqlClientFactory.Instance))
            {
                db.Delete<Feed>(new Feed
                {
                    source = name
                });
            }
        }
    }
}
