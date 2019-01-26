using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace AzureRpdeProxy
{
    public static class PruneFeed
    {
        [FunctionName("PruneFeed")]
        public static async Task Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"Prune trigger function executed at: {DateTime.Now}");
            using (SqlConnection connection = new SqlConnection(SqlUtils.SqlDatabaseConnectionString))
            {
                SqlCommand cmd = new SqlCommand("PRUNE_STALE_ITEMS", connection);
                cmd.CommandType = CommandType.StoredProcedure;
                connection.Open();
                await cmd.ExecuteNonQueryAsync();
            }
        }
    }
}
