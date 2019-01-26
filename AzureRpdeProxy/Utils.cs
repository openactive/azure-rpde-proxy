using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureRpdeProxy
{
    class Utils
    {
        public const string CC_BY_LICENSE = "https://creativecommons.org/licenses/by/4.0/";
        public const string QUEUE_NAME = "feedstate";
        public const string PURGE_QUEUE_NAME = "purge";
        public const string REGISTRATION_QUEUE_NAME = "registration";

        public static string GetFeedUrl(string name)
        {
            return Environment.GetEnvironmentVariable("FeedBaseUrl") + "api/feeds/" + name;
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

        // TODO: Move this to app settings if allowed by deployment script
        static public string SqlDatabaseConnectionString = Environment.GetEnvironmentVariable("SqlServerConnection")?.ToString();
    }
}
