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
            [ServiceBus(Utils.REGISTRATION_QUEUE_NAME, Connection = "ServiceBusConnection", EntityType = EntityType.Queue)] IAsyncCollector<Message> registrationQueueCollector,
            [ServiceBus(Utils.FEED_STATE_QUEUE_NAME, Connection = "ServiceBusConnection", EntityType = EntityType.Queue)] IAsyncCollector<Message> queueCollector,
            ILogger log)
        {

            var feedStateItem = FeedState.DecodeFromMessage(message);

            // Dead letter queue from PollFeed simply triggers a purge

            log.LogInformation($"Registration Trigger Started: {feedStateItem.name}");

            dynamic data = null;

            // Attempt to get first page
            try
            {
                var result = await httpClient.GetAsync(feedStateItem.nextUrl);
                if (result.StatusCode == HttpStatusCode.Unauthorized)
                {
                    // Remove from queue on 401 (OWS key has changed)
                    log.LogWarning($"Feed attempting registration returned 401 and will be dropped: '{feedStateItem.name}'.");
                    await messageReceiver.CompleteAsync(lockToken);
                    return;
                }
                else
                {
                    data = JsonConvert.DeserializeObject<RpdeFeed>(await result.Content.ReadAsStringAsync());
                }
            }
            catch (Exception ex)
            {
                // Retry registration three times over 30 minutes, then fail
                if (feedStateItem.pollRetries > 3)
                {
                    log.LogError($"Registration error while validating first page. Dropping feed. Error retrieving '{feedStateItem.url}'. {ex.ToString()}");
                    await messageReceiver.CompleteAsync(lockToken);
                }
                else
                {
                    feedStateItem.pollRetries++;
                    feedStateItem.totalErrors++;
                    log.LogWarning($"Registration error while validating first page. Retrying '{feedStateItem.name}' attempt {feedStateItem.pollRetries}. Error retrieving '{feedStateItem.url}'. {ex.ToString()}");
                    await messageReceiver.CompleteAsync(lockToken);
                    await registrationQueueCollector.AddAsync(feedStateItem.EncodeToMessage(30));
                }
                return;
            }

            // check for "license": "https://creativecommons.org/licenses/by/4.0/"
            if (data?.license != Utils.CC_BY_LICENSE)
            {
                log.LogError($"Registration error while validating first page - dropping feed. Error retrieving license for '{feedStateItem.url}'.");
                await messageReceiver.CompleteAsync(lockToken);
                return;
            }

            // Restart the feed from the beginning
            feedStateItem.nextUrl = feedStateItem.url;

            feedStateItem.ResetCounters();

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

            await messageReceiver.CompleteAsync(lockToken);
            await queueCollector.AddAsync(feedStateItem.EncodeToMessage(0));
            log.LogInformation($"Registration Trigger Promoting Feed: {feedStateItem.name}");
        }
    }
}
