using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;
using NPoco;
using System;
using System.Data;
using System.Data.SqlClient;
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

        [FunctionName("PurgeQueueHandler")]
        public static async Task Run([ServiceBusTrigger(Utils.PURGE_QUEUE_NAME, Connection = "ServiceBusConnection")] Message message, MessageReceiver messageReceiver, string lockToken,
            [ServiceBus(Utils.PURGE_QUEUE_NAME, Connection = "ServiceBusConnection", EntityType = EntityType.Queue)] IAsyncCollector<Message> queueCollector,
            [ServiceBus(Utils.REGISTRATION_QUEUE_NAME, Connection = "ServiceBusConnection", EntityType = EntityType.Queue)] IAsyncCollector<Message> registrationQueueCollector,
            ILogger log)
        {
            var feedStateItem = FeedState.DecodeFromMessage(message);

            log.LogInformation($"Purge Trigger Started: {feedStateItem.name}");

            try
            {
                int itemCount = 0;

                var sw = new Stopwatch();
                sw.Start();

                using (var db = new Database(SqlUtils.SqlDatabaseConnectionString, DatabaseType.SqlServer2012, SqlClientFactory.Instance))
                {
                    itemCount = await db.ExecuteAsync("DELETE_SOURCE @0", feedStateItem.name);
                }

                sw.Stop();

                log.LogWarning($"PURGE TIMER {feedStateItem.name}: Deleted {itemCount} items from source '{feedStateItem.name}' in {sw.ElapsedMilliseconds} ms");

                feedStateItem.purgedItems = itemCount;

                if (itemCount < 1000)
                {
                    log.LogInformation($"Purge complete for '{feedStateItem.name}'");

                    // Check lock exists, as close to a transaction as we can get
                    if (await messageReceiver.RenewLockAsync(lockToken) != null)
                    {
                        await messageReceiver.CompleteAsync(lockToken);

                        // Attempt re-registration unless the proxy cache is being cleared
                        if (Environment.GetEnvironmentVariable("ClearProxyCache")?.ToString() != "true")
                        {
                            feedStateItem.ResetCounters();
                            feedStateItem.totalPurgeCount++;
                            await registrationQueueCollector.AddAsync(feedStateItem.EncodeToMessage(1));
                        }
                        else
                        {
                            log.LogWarning($"Purge Successfully Cleaned: {feedStateItem.name}");
                        }
                    }
                } else
                {
                    feedStateItem.purgedItems += itemCount;

                    // Check lock exists, as close to a transaction as we can get
                    if (await messageReceiver.RenewLockAsync(lockToken) != null)
                    {
                        await messageReceiver.CompleteAsync(lockToken);
                        await queueCollector.AddAsync(feedStateItem.EncodeToMessage(1));
                    }
                }
            }
            catch (SqlException ex)
            {
                log.LogError($"Error during DELETE_SOURCE stored procedure {ex.Number}: " + ex.ToString());

                feedStateItem.lastError = ex.ToString();

                feedStateItem.purgeRetries++;

                TimeSpan timeSpan = new TimeSpan(1, 0, 0);
                Random randomTest = new Random();
                TimeSpan newSpan = TimeSpan.FromMinutes(randomTest.Next(0, (int)timeSpan.TotalMinutes));

                int delaySeconds = (int)newSpan.TotalSeconds;

                log.LogWarning($"Unexpected error purging items: Retrying '{feedStateItem.name}' attempt {feedStateItem.purgeRetries} in {delaySeconds} seconds");

                // Check lock exists, as close to a transaction as we can get
                if (await messageReceiver.RenewLockAsync(lockToken) != null)
                {
                    await messageReceiver.CompleteAsync(lockToken);
                    await queueCollector.AddAsync(feedStateItem.EncodeToMessage(delaySeconds));
                }
            }

            log.LogInformation($"Purge Trigger Complete: {feedStateItem.name}");
        }
    }
}
