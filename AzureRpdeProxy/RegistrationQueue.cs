using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
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
            [ServiceBus(Utils.QUEUE_NAME, Connection = "ServiceBusConnection", EntityType = EntityType.Queue)] IAsyncCollector<Message> queueCollector,
            ILogger log)
        {

            var feedStateItem = FeedState.DecodeFromMessage(message);

            // Dead letter queue from PollFeed simply triggers a purge

            log.LogInformation($"Registration Trigger Started: {feedStateItem.name}");

            dynamic data = null;

            // Attempt to get first page
            try
            {
                var result = await httpClient.GetStringAsync(feedStateItem.url);
                data = JsonConvert.DeserializeObject(result);
            }
            catch (Exception ex)
            {
                log.LogError($"Registration error while validating first page. Error retrieving '{feedStateItem.url}'. {ex.ToString()}");
                // TODO: Consolidate feed fetch code and make this exponential backoff with a limit of 10 retries
                await registrationQueueCollector.AddAsync(feedStateItem.EncodeToMessage(10));
                await messageReceiver.CompleteAsync(lockToken);
                return;
            }

            // check for "license": "https://creativecommons.org/licenses/by/4.0/"
            if (data?.license != Utils.CC_BY_LICENSE)
            {
                log.LogError($"Registration error while validating first page. Error retrieving license for '{feedStateItem.url}'.");
                await registrationQueueCollector.AddAsync(feedStateItem.EncodeToMessage(10));
                await messageReceiver.CompleteAsync(lockToken);
                return;
            }

            // Restart the feed from the beginning
            feedStateItem.nextUrl = feedStateItem.url;

            await queueCollector.AddAsync(feedStateItem.EncodeToMessage(0));
            await messageReceiver.CompleteAsync(lockToken);
            log.LogInformation($"Registration Trigger Promoting Feed: {feedStateItem.name}");
        }
    }
}
