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
        [FunctionName("PollQueueDeadLetterHandler")]
        public static async Task Run([ServiceBusTrigger(Utils.FEED_STATE_QUEUE_NAME + "/$DeadLetterQueue", Connection = "ServiceBusConnection")] Message message, MessageReceiver messageReceiver, string lockToken,
            [ServiceBus(Utils.PURGE_QUEUE_NAME, Connection = "ServiceBusConnection", EntityType = EntityType.Queue)] IAsyncCollector<Message> purgeQueueCollector,
            ILogger log)
        {
            var feedStateItem = FeedState.DecodeFromMessage(message);

            log.LogInformation($"DeadLetterQueue Trigger Started: {feedStateItem.name}");

            // Dead letter queue from PollFeed simply triggers a purge
            feedStateItem.ResetCounters();
            
            // Check lock exists, as close to a transaction as we can get
            if (await messageReceiver.RenewLockAsync(lockToken) != null)
            {
                await messageReceiver.CompleteAsync(lockToken);
                await purgeQueueCollector.AddAsync(feedStateItem.EncodeToMessage(0));
            }

            log.LogInformation($"DeadLetterQueue Trigger Complete: {feedStateItem.name}");
        }
    }
}
