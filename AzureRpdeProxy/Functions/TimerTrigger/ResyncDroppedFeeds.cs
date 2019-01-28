using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using NPoco;
using System.Linq;
using Microsoft.Azure.WebJobs.ServiceBus;

namespace AzureRpdeProxy
{
    public static class ResyncDroppedFeeds
    {
        [FunctionName("ResyncDroppedFeeds")]
        public static async Task Run([TimerTrigger("*/10 * * * * *")]TimerInfo myTimer, ILogger log,
            [ServiceBus(Utils.PURGE_QUEUE_NAME, Connection = "ServiceBusConnection", EntityType = EntityType.Queue)] ICollector<FeedState> queueCollector)
        {
            if (Environment.GetEnvironmentVariable("ClearProxyCache")?.ToString() == "true")
            {
                return;
            }

            log.LogInformation($"Resync trigger function executed at: {DateTime.Now}");

            // Get initial feedstates from feeds in registration table
            List<FeedState> feedStateListFromDatabase;
            using (var db = new Database(SqlUtils.SqlDatabaseConnectionString, DatabaseType.SqlServer2012, SqlClientFactory.Instance))
            {
                feedStateListFromDatabase = (await db.Query<Feed>().ToListAsync()).Select(f => f.initialFeedState).ToList();
            }

            // Get all feedstates from queues (check several times to account for queue state transitions)
            // This would be catastrophic if a queue was missed, so is checked excessively.
            // Note this is still much cheaper than enabling transactions on the Service Bus (which
            // is the ideal way to solve this problem)
            var feedNamesFromQueues = new HashSet<string>();
            for (int i = 0; i < 8; i++)
            {
                feedNamesFromQueues.UnionWith((await Utils.GetFeedStateFromQueues()).Select(f => f.name));
                await Task.Delay(2000);
            }
            
            // Get all feedstates from registration tables that do not match an entry in the queue
            var orphanedFeedStates = feedStateListFromDatabase.Where(f => !feedNamesFromQueues.Contains(f.name)).ToList();

            // Kick off purge for these feeds to cause re-reregistration
            foreach (FeedState feedState in orphanedFeedStates)
            {
                log.LogWarning($"RESYNC: Feed '{feedState.name}' not found in queues. Attempting resync.");
                queueCollector.Add(feedState);
            }

            log.LogInformation($"Resync trigger function completed at: {DateTime.Now}");
        }
    }
}
