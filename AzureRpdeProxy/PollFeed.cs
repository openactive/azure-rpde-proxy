using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using NPoco;
using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Numerics;
using System.Threading.Tasks;
using System.Transactions;
using System.Web;

namespace AzureRpdeProxy
{
    public static class PollFeed
    {
        private static readonly HttpClient httpClient;

        static PollFeed()
        {
            httpClient = new HttpClient();
        }

        [FunctionName("PollFeed")]
        public static async Task Run([ServiceBusTrigger(Utils.QUEUE_NAME, Connection = "ServiceBusConnection")] Message message, MessageReceiver messageReceiver, string lockToken,
            [ServiceBus(Utils.QUEUE_NAME, Connection = "ServiceBusConnection", EntityType = EntityType.Queue)] IAsyncCollector<Message> queueCollector,
             ILogger log)
        {
            var feedStateItem = FeedState.DecodeFromMessage(message);

            log.LogInformation($"PollFeed queue trigger function processed message: {feedStateItem?.nextUrl}");

            // Increment poll requests before anything else
            feedStateItem.totalPollRequests++;
            feedStateItem.dateModified = DateTime.Now;

            int delaySeconds = 0;

            // Attempt to get next page
            RpdeFeed data = null;
            try
            {
                var sw = new Stopwatch();
                sw.Start();

                var result = await httpClient.GetAsync(feedStateItem.nextUrl);
                if (result.StatusCode == HttpStatusCode.Unauthorized)
                {
                    // Deadletter on 401 (OWS key has changed)
                    log.LogWarning($"Feed attempting poll returned 401 and will be purged: '{feedStateItem.name}'.");
                    delaySeconds = -1;
                }
                else
                {
                    data = JsonConvert.DeserializeObject<RpdeFeed>(await result.Content.ReadAsStringAsync());
                }

                sw.Stop();
                log.LogWarning($"FETCH TIMER {feedStateItem.name}: {sw.ElapsedMilliseconds} ms to fetch {data?.items?.Count ?? 0} items.");
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Error retrieving page: " + feedStateItem.nextUrl);
            }

            if (delaySeconds == -1)
            {
                // Do nothing to immediately deadletter this response
            }
            // check for "license": "https://creativecommons.org/licenses/by/4.0/"
            else if (data?.license == Utils.CC_BY_LICENSE && data?.next != null)
            {
                if (data?.items?.Count > 0)
                {
                    var cacheItems = data?.items.Select(item => new CachedRpdeItem
                    {
                        id = item.id is int || item.id is long ? ((long)item.id).ToString("D20") : HttpUtility.UrlEncode(item.id),
                        modified = item.modified,
                        deleted = item.state == "deleted",
                        // Note must be manually serialised to pass to stored procedure, which ignores object annotations
                        data = JsonConvert.SerializeObject(item, Newtonsoft.Json.Formatting.None,
                        new JsonSerializerSettings
                        {
                            NullValueHandling = NullValueHandling.Ignore
                        }),
                        kind = item.kind,
                        source = feedStateItem.name,
                        expiry = item.state == "deleted" ? DateTime.UtcNow.AddDays(feedStateItem.deletedItemDaysToLive) : (DateTime?)null
                    }).ToList();

                    feedStateItem.totalPagesRead++;
                    feedStateItem.totalItemsRead += cacheItems.Count;
                    feedStateItem.nextUrl = data.next;

                    try
                    {
                        var sw = new Stopwatch();
                        sw.Start();

                        // Batch if more than a few updates
                        // TODO: Benchmark batch to see if always faster and can always be used
                        if (true) //cacheItems.Count > 2)
                        {
                            using (SqlConnection connection = new SqlConnection(SqlUtils.SqlDatabaseConnectionString))
                            {
                                connection.Open();

                                DataTable table = new DataTable();
                                table.Columns.Add("source", typeof(string));
                                table.Columns.Add("id", typeof(string));
                                table.Columns.Add("modified", typeof(long));
                                table.Columns.Add("kind", typeof(string));
                                table.Columns.Add("deleted", typeof(bool));
                                table.Columns.Add("data", typeof(string));
                                table.Columns.Add("expiry", typeof(DateTime));
                                foreach (var item in cacheItems)
                                {
                                    table.Rows.Add(item.source, item.id, item.modified, item.kind, item.deleted, item.data, item.expiry);
                                }

                                SqlCommand cmd = new SqlCommand("UPDATE_ITEM_BATCH", connection);
                                cmd.CommandType = CommandType.StoredProcedure;

                                cmd.Parameters.Add(
                                    new SqlParameter()
                                    {
                                        ParameterName = "@Tvp",
                                        SqlDbType = SqlDbType.Structured,
                                        TypeName = "ItemTableType",
                                        Value = table,
                                    });

                                await cmd.ExecuteNonQueryAsync();
                            }
                        }
                        else
                        {
                            using (var db = new Database(SqlUtils.SqlDatabaseConnectionString, DatabaseType.SqlServer2012, SqlClientFactory.Instance))
                            {
                                using (var transaction = db.GetTransaction())
                                {
                                    foreach (var item in cacheItems)
                                    {
                                        db.Execute("UPDATE_ITEM", CommandType.StoredProcedure, item);
                                    }
                                    transaction.Complete();
                                }
                            }
                        }

                        sw.Stop();
                        log.LogWarning($"POLL TIMER {feedStateItem.name}: {sw.ElapsedMilliseconds} ms to import {cacheItems.Count} items.");

                        // TODO: Write items to cosmos, only overwritting when modified is newer
                        // TODO: Count how many updates were actually made, if 0 check to see if there are duplicate messages in the queue and drop the one with the greater GUID (so that they don't both drop themselves) was a duplicate and do not add to the queue
                        // Or compare the max modified of the database with the max modified of the items array, to determine if any modification has happened previous to this?
                        feedStateItem.pollRetries = 0;
                        delaySeconds = 0;
                    }
                    catch (SqlException ex)
                    {
                        if (SqlUtils.SqlTransientErrorNumbers.Contains(ex.Number))
                        {
                            log.LogWarning($"Throttle on PollFeed, retry after {SqlUtils.SqlRetrySecondsRecommendation} seconds.");
                            delaySeconds = SqlUtils.SqlRetrySecondsRecommendation;
                            feedStateItem.pollRetries = 0;
                        }
                        else
                        {
                            feedStateItem.pollRetries++;
                            feedStateItem.totalErrors++;
                            delaySeconds = (int)BigInteger.Pow(2, feedStateItem.pollRetries);
                            log.LogWarning($"Error writing page to SQL Server {ex.Number}: Retrying '{feedStateItem.name}' attempt {feedStateItem.pollRetries} in {delaySeconds} seconds. Error: " + ex.ToString());
                        }
                    }
                    catch (Exception ex)
                    {
                        feedStateItem.pollRetries++;
                        feedStateItem.totalErrors++;
                        delaySeconds = (int)BigInteger.Pow(2, feedStateItem.pollRetries);
                        log.LogWarning($"Error writing page to SQL Server: Retrying '{feedStateItem.name}' attempt {feedStateItem.pollRetries} in {delaySeconds} seconds. Error: " + ex.ToString());
                    }
                }
                else
                {
                    feedStateItem.pollRetries = 0;
                    delaySeconds = feedStateItem.recommendedPollInterval;
                }
            }
            else
            {
                if (feedStateItem.pollRetries > 15) // Retry with exponential backoff for 18 hours, then fail and purge regardless of the error type
                {
                    log.LogError($"Error retrieving page: DEAD-LETTERING '{feedStateItem.name}'");

                    //message.
                    //feedStateItem.DeadLetter("Too many retries", $"ResubmitCount is {resubmitCount}");
                    delaySeconds = -1;
                }
                else
                {
                    feedStateItem.pollRetries++;
                    feedStateItem.totalErrors++;
                    delaySeconds = (int)BigInteger.Pow(2, feedStateItem.pollRetries);
                    log.LogWarning($"Error retrieving page: Retrying '{feedStateItem.name}' attempt {feedStateItem.pollRetries} in {delaySeconds} seconds");
                }
            }

            // Move all to DeadLetter if ClearProxyCache is enabled
            if (delaySeconds < 0 || Environment.GetEnvironmentVariable("ClearProxyCache")?.ToString() == "true")
            {
                await messageReceiver.DeadLetterAsync(lockToken);
            } else
            {
                var newMessage = feedStateItem.EncodeToMessage(delaySeconds);

                // These two operations should be in a transaction, but to save cost they ordered so that a failure will result in the polling stopping,
                // and a reregistration being required to restart it (24 hrs later)
                //using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
                //{
                await messageReceiver.CompleteAsync(lockToken);
                await queueCollector.AddAsync(newMessage);
                    
                //    scope.Complete(); // declare the transaction done
                //}
                
            }
        }

    }
}

/*
public static class RegisterFeed
{
    [FunctionName("Function1")]
    public static void Run([CosmosDBTrigger(
            databaseName: "openactive",
            collectionName: "items",
            ConnectionStringSetting = "AccountEndpoint=https://gladstone-openactive-proxy.documents.azure.com:443/;AccountKey=rR4obv27zMJxtJel8ZeDsFSpDcrq0ESIOCbop4gf5aK6F0GvdP4qH2fHW0xuEpG7eScdVM2M2L3R5PSFRaIu7Q==;",
            LeaseCollectionName = "leases")]IReadOnlyList<Document> input, ILogger log)
    {
        if (input != null && input.Count > 0)
        {
            log.LogInformation("Documents modified " + input.Count);
            log.LogInformation("First document Id " + input[0].Id);
        }
    }
}


public static class WriteDocsIAsyncCollector
{
    [FunctionName("WriteDocsIAsyncCollector")]
    public static async Task Run(
        [QueueTrigger("todoqueueforwritemulti")] ToDoItem[] toDoItemsIn,
        [CosmosDB(
                databaseName: "ToDoItems",
                collectionName: "Items",
                ConnectionStringSetting = "CosmosDBConnection")]
                IAsyncCollector<ToDoItem> toDoItemsOut,
        ILogger log)
    {
        log.LogInformation($"C# Queue trigger function processed {toDoItemsIn?.Length} items");

        foreach (ToDoItem toDoItem in toDoItemsIn)
        {
            log.LogInformation($"Description={toDoItem.Description}");
            await toDoItemsOut.AddAsync(toDoItem);
        }
    }
}


[FunctionName("CosmosDbSample")]
public static async Task<HttpResponseMessage> Run(
   [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)]MyClass[] classes,
   TraceWriter log,
   [DocumentDB("ToDoList", "Items", ConnectionStringSetting = "CosmosDB")] IAsyncCollector<MyClass> documentsToStore)
{
    log.Info($"Detected {classes.Length} incoming documents");
    foreach (MyClass aClass in classes)
    {
        await documentsToStore.AddAsync(aClass);
    }

    return new HttpResponseMessage(HttpStatusCode.Created);
}
*/