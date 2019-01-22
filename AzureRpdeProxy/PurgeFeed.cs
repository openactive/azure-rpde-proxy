using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;
using System;
using System.Diagnostics;
using System.Numerics;
using System.Threading.Tasks;

namespace AzureRpdeProxy
{
    public static class PurgeFeed
    {
        public class DeletePartitionResponse
        {
            public int deleted { get; set; }
            public bool continuation { get; set; }
        }

        [FunctionName("PurgeFeed")]
        public static async Task Run([ServiceBusTrigger(Utils.PURGE_QUEUE_NAME, Connection = "ServiceBusConnection")] Message message, MessageReceiver messageReceiver, string lockToken,
            [ServiceBus(Utils.PURGE_QUEUE_NAME, Connection = "ServiceBusConnection", EntityType = EntityType.Queue)] IAsyncCollector<Message> queueCollector,
            [ServiceBus(Utils.REGISTRATION_QUEUE_NAME, Connection = "ServiceBusConnection", EntityType = EntityType.Queue)] IAsyncCollector<Message> registrationQueueCollector,
            [CosmosDB(
                databaseName: "openactive",
                collectionName: "Items",
                ConnectionStringSetting = "CosmosDBConnection")] DocumentClient client, 
            ILogger log)
        {

            var feedStateItem = FeedState.DecodeFromMessage(message);

            log.LogInformation($"Purge Trigger Started: {feedStateItem.name}");

            StoredProcedureResponse<DeletePartitionResponse> sprocResponse = null;

            int delaySeconds = 1;

            try
            {
                var sw = new Stopwatch();
                sw.Start();

            
                sprocResponse = await client.ExecuteStoredProcedureAsync<DeletePartitionResponse>(
                                                                            UriFactory.CreateStoredProcedureUri("openactive", "Items", "deletePartition"),
                                                                            new RequestOptions { PartitionKey = new PartitionKey(feedStateItem.name) },
                                                                            new RetryOptions { MaxRetryAttemptsOnThrottledRequests = 30, MaxRetryWaitTimeInSeconds = 30 }
                                                                        );

                sw.Stop();

                log.LogInformation($"Deleted {sprocResponse?.Response?.deleted} items from partition '{feedStateItem.name}' in {sw.ElapsedMilliseconds} ms using {sprocResponse.RequestCharge} RUs with continuation required: {sprocResponse?.Response?.continuation}");

                delaySeconds = sprocResponse?.Response?.continuation == true && sprocResponse?.Response?.deleted == 0 ? 60 : 1; // Default to 60 seconds if there's contention

                feedStateItem.purgedItems += sprocResponse?.Response?.deleted ?? 0;
            } catch (Exception ex)
            {
                log.LogError("Error during deletePartition stored procedure: " + ex.ToString());
            }

            if (sprocResponse?.Response?.continuation == null)
            {
                feedStateItem.purgeRetries++;
                delaySeconds = (int)BigInteger.Pow(2, feedStateItem.purgeRetries);
                log.LogWarning($"Unexpected error purging items: Retrying '{feedStateItem.name}' attempt {feedStateItem.purgeRetries} in {delaySeconds} seconds");
                await queueCollector.AddAsync(feedStateItem.EncodeToMessage(delaySeconds));
            }
            else if (sprocResponse.Response.continuation == true)
            {
                feedStateItem.purgeRetries = 0;
                await queueCollector.AddAsync(feedStateItem.EncodeToMessage(delaySeconds));
            }
            else
            {
                if (Environment.GetEnvironmentVariable("ClearProxyCache")?.ToString() != "true")
                {
                    feedStateItem.ResetCounters();
                    feedStateItem.totalPurgeCount++;
                    await registrationQueueCollector.AddAsync(feedStateItem.EncodeToMessage(delaySeconds));
                }
            }

            await messageReceiver.CompleteAsync(lockToken);
            log.LogInformation($"Purge Trigger Complete: {feedStateItem.name}");
        }
    }
}
