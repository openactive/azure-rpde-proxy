using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace AzureRpdeProxy
{
    public static class PollFeedDeadLetterHandler
    {
        [FunctionName("PollFeedDeadLetterHandler")]
        public static async Task Run([ServiceBusTrigger(Utils.QUEUE_NAME + "/$DeadLetterQueue", Connection = "ServiceBusConnection")] Message message, MessageReceiver messageReceiver, string lockToken,
            [ServiceBus(Utils.PURGE_QUEUE_NAME, Connection = "ServiceBusConnection", EntityType = EntityType.Queue)] IAsyncCollector<Message> queueCollector,
            ILogger log)
        {

            var feedStateItem = FeedState.DecodeFromMessage(message);

            // Dead letter queue from PollFeed simply triggers a purge

            log.LogInformation($"DeadLetterQueue Trigger Started: {feedStateItem.name}");
            await queueCollector.AddAsync(feedStateItem.EncodeToMessage(0));
            await messageReceiver.CompleteAsync(lockToken);
            log.LogInformation($"DeadLetterQueue Trigger Complete: {feedStateItem.name}");
        }
    }
}
