using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Diagnostics;
using System.Linq;
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

        private static async Task<IBulkExecutor> InitializeBulkExecutor (DocumentClient client)
        {
            client.ConnectionPolicy.ConnectionMode = ConnectionMode.Direct;
            client.ConnectionPolicy.ConnectionProtocol = Protocol.Tcp;

            // Set retry options high during initialization (default values).
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 30;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 9;

            var dataCollection =  client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri("openactive"))
                .Where(c => c.Id == "Items").AsEnumerable().FirstOrDefault();

            IBulkExecutor bulkExecutor = new BulkExecutor(client, dataCollection);
            await bulkExecutor.InitializeAsync();

            // Set retries to 0 to pass complete control to bulk executor.
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 0;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 0;

            return bulkExecutor;
        }

        [FunctionName("PollFeed")]
        public static async Task Run([ServiceBusTrigger(Utils.QUEUE_NAME, Connection = "ServiceBusConnection")] Message message, MessageReceiver messageReceiver, string lockToken,
            [ServiceBus(Utils.QUEUE_NAME, Connection = "ServiceBusConnection", EntityType = EntityType.Queue)] IAsyncCollector<Message> queueCollector,
            [CosmosDB(
                databaseName: "openactive",
                collectionName: "Items",
                ConnectionStringSetting = "CosmosDBConnection")] DocumentClient client,
            [CosmosDB(
                databaseName: "openactive",
                collectionName: "Items",
                ConnectionStringSetting = "CosmosDBConnection")]
                IAsyncCollector<CachedRpdeItem> itemsOut,
             ILogger log)
        {
            var feedStateItem = FeedState.DecodeFromMessage(message);

            log.LogInformation($"PollFeed queue trigger function processed message: {feedStateItem?.nextUrl}");

            // Increment poll requests before anything else
            feedStateItem.totalPollRequests++;
            feedStateItem.dateModified = DateTime.Now;

            // Attempt to get next page
            RpdeFeed data = null;
            try
            {
                var result = await httpClient.GetStringAsync(feedStateItem.nextUrl);
                data = JsonConvert.DeserializeObject<RpdeFeed>(result);
            } catch (Exception ex)
            {
                log.LogError(ex, "Error retrieving page: " + feedStateItem.nextUrl);
            }

            int delaySeconds;

            // check for "license": "https://creativecommons.org/licenses/by/4.0/"
            if (data?.license == Utils.CC_BY_LICENSE && data?.next != null)
            {
                if (data?.items?.Count > 0)
                {
                    var cacheItems = data?.items.Select(item => new CachedRpdeItem
                    {
                        id = feedStateItem.idIsNumeric ? ((long)item.id).ToString("D20") : HttpUtility.UrlEncode(item.id),
                        modified = item.modified,
                        deleted = item.state == "deleted",
                        data = item.data,
                        kind = item.kind,
                        source = feedStateItem.name
                    }).ToList();

                    feedStateItem.totalPagesRead++;
                    feedStateItem.totalItemsRead += cacheItems.Count;
                    feedStateItem.nextUrl = data.next;

                    try
                    {
                        var sw = new Stopwatch();
                        sw.Start();

                        string importMethod;

                        // Only use bulkExecutor for large numbers of items, from rough testing 100 seems to be the tipping point to use BulkExecutor
                        if (cacheItems.Count < 100)
                        {
                            importMethod = "AddAsync";
                            foreach (var item in cacheItems)
                            {
                                await itemsOut.AddAsync(item);
                            }
                        }
                        else
                        {
                            importMethod = "BulkExecutor";
                            var bulkExecutor = await InitializeBulkExecutor(client);
                            BulkImportResponse importStats = await bulkExecutor.BulkImportAsync(
                              documents: cacheItems,
                              enableUpsert: true,
                              disableAutomaticIdGeneration: true,
                              maxConcurrencyPerPartitionKeyRange: 1,
                              maxInMemorySortingBatchSize: null);

                            log.LogInformation($"{importStats.NumberOfDocumentsImported} items imported of {cacheItems.Count()} retrieved, using {importStats.TotalRequestUnitsConsumed} RUs in {importStats.TotalTimeTaken}.");
                            if (importStats.BadInputDocuments?.Count > 0)
                            {
                                log.LogError($"{importStats.BadInputDocuments.Count} items failed to import from URL '{feedStateItem.nextUrl}' of '{feedStateItem.name}'.");
                            }
                        }

                        sw.Stop();
                        log.LogInformation($"TIMER for {importMethod}: {sw.ElapsedMilliseconds} ms to import {cacheItems.Count} items.");

                        // TODO: Write items to cosmos, only overwritting when modified is newer
                        // TODO: Count how many updates were actually made, if 0 check to see if there are duplicate messages in the queue and drop the one with the greater GUID (so that they don't both drop themselves) was a duplicate and do not add to the queue
                        // Or compare the max modified of the database with the max modified of the items array, to determine if any modification has happened previous to this?
                        feedStateItem.pollRetries = 0;
                        delaySeconds = 0;
                    } catch (Exception ex)
                    {
                        feedStateItem.pollRetries++;
                        feedStateItem.totalErrors++;
                        delaySeconds = (int)BigInteger.Pow(2, feedStateItem.pollRetries);
                        log.LogWarning($"Error writing page to CosmosDB: Retrying '{feedStateItem.name}' attempt {feedStateItem.pollRetries} in {delaySeconds} seconds. Error: " + ex.ToString());
                    }
                } else
                {
                    feedStateItem.pollRetries = 0;
                    delaySeconds = 8;
                }
            } else
            {
                if (feedStateItem.pollRetries > 20)
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
                    await queueCollector.AddAsync(newMessage);
                    await messageReceiver.CompleteAsync(lockToken);
                    
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