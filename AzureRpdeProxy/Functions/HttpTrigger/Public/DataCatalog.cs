using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using NPoco;
using System.Data.SqlClient;
using System.Data;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;

namespace AzureRpdeProxy
{
    public static class Datasets
    {
        [FunctionName("DataCatalog")]
        public static async Task<HttpResponseMessage> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "datacatalog")] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("DataCatalog trigger function processed a request.");

            var datasetUrls = new List<string>();

            // Use stored procedure for maximum efficiency
            using (SqlConnection connection = new SqlConnection(SqlUtils.SqlDatabaseConnectionString))
            {
                SqlCommand cmd = new SqlCommand("READ_DATASETS", connection);
                cmd.CommandType = CommandType.StoredProcedure;
                
                connection.Open();

                // This query will return one additional record (from the previous page) to check that the provided "source" value is valid
                // (So even the last page will return at least 1 record)
                SqlDataReader reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    datasetUrls.Add(reader.GetString(0));
                }
                    
                // Call Close when done reading.
                reader.Close();
            }

            return req.CreateJSONResponse(HttpStatusCode.OK, new DataCatalog
            {
                id = Environment.GetEnvironmentVariable("FeedBaseUrl") + "api/datacatalog",
                dataset = datasetUrls,
                publisher = new Organization
                {
                    name = Environment.GetEnvironmentVariable("OrganizationName"),
                    url = Environment.GetEnvironmentVariable("OrganizationUrl")
                }
            }).AsCachable(TimeSpan.FromMinutes(15));
        }
    }
}
