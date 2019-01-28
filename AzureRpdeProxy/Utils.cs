using Microsoft.Azure.ServiceBus.Core;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureRpdeProxy
{
    static class Utils
    {
        public const string CC_BY_LICENSE = "https://creativecommons.org/licenses/by/4.0/";
        public const string FEED_STATE_QUEUE_NAME = "feedstate";
        public const string PURGE_QUEUE_NAME = "purge";
        public const string REGISTRATION_QUEUE_NAME = "registration";

        // Note all ingested IDs are URL Encoded, so this ID will never conflict with a real ID
        public const string LAST_PAGE_ITEM_RESERVED_ID = "/@:********!!LASTPAGE!!********:@/";
        public const long LAST_PAGE_ITEM_RESERVED_MODIFIED = long.MaxValue;

        public const string RECOMMENDED_POLL_INTERVAL_HEADER = "x-recommended-poll-interval";
        public const int DEFAULT_POLL_INTERVAL = 8;
        public const int MAX_POLL_INTERVAL = 3600;
        public const int MIN_POLL_INTERVAL = 5;

        public static string GetFeedUrl(string name)
        {
            return Environment.GetEnvironmentVariable("FeedBaseUrl") + "api/feeds/" + name;
        }

        public static async Task<List<FeedState>> GetFeedStateFromQueues(string name = null, bool ignoreRegistrationQueue = false)
        {
            string ServiceBusConnectionString = Environment.GetEnvironmentVariable("ServiceBusConnection");
            var queueClients = new List<IMessageReceiver> {
                new MessageReceiver(ServiceBusConnectionString, Utils.FEED_STATE_QUEUE_NAME),
                new MessageReceiver(ServiceBusConnectionString, Utils.FEED_STATE_QUEUE_NAME + "/$DeadLetterQueue"),
                new MessageReceiver(ServiceBusConnectionString, Utils.PURGE_QUEUE_NAME)
            };
            if (!ignoreRegistrationQueue) queueClients.Add(
                new MessageReceiver(ServiceBusConnectionString, Utils.REGISTRATION_QUEUE_NAME)
                );

            List<FeedState> list = new List<FeedState>();

            foreach (var client in queueClients)
            {
                List<FeedState> lastRetrievedList = null;
                do
                {
                    lastRetrievedList = (await client.PeekAsync(100)).Select(m => FeedState.DecodeFromMessage(m)).ToList();
                    list.AddRange(lastRetrievedList);
                } while (lastRetrievedList.Count > 0);
            }

            if (name == null)
            {
                return list;
            }
            else
            {
                return list.Where(fs => fs.name == name).ToList();
            }
        }
    }

    static class SqlUtils
    {
        // Transient fault error codes
        // https://docs.microsoft.com/en-us/azure/sql-database/sql-database-develop-error-messages
        static public List<int> SqlTransientErrorNumbers =
          new List<int> { 4060, 40197, 40501, 40613, 49918, 49919, 49920, 4221, 11001,
              10928, 10929, 40544, 40549, 40550, 40551, 40552, 40553 };

        // Recommended Azure SQL retry interval
        // https://blogs.msdn.microsoft.com/psssql/2012/10/30/worker-thread-governance-coming-to-azure-sql-database/
        static public int SqlRetrySecondsRecommendation = 10;

        static public string SqlDatabaseConnectionString = Environment.GetEnvironmentVariable("SqlServerConnection")?.ToString();
    }
}
