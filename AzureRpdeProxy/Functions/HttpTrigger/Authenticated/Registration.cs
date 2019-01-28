using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs.Extensions.Http;
using System.IO;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs.ServiceBus;
using System.Net.Http;
using System.Collections.Generic;
using Microsoft.Azure.ServiceBus.Core;
using System.Linq;

namespace AzureRpdeProxy
{
    public static class Registration
    {

        private static readonly HttpClient httpClient;

        static Registration()
        {
            httpClient = new HttpClient();
        }

        [FunctionName("Registration")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log,
            [ServiceBus(Utils.PURGE_QUEUE_NAME, Connection = "ServiceBusConnection", EntityType = EntityType.Queue)] ICollector<FeedState> queueCollector)
        {
            log.LogInformation("Registration request received");

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

            var registrationRequest = JsonConvert.DeserializeObject<ProxyRegistrationRequest>(requestBody);

            if (registrationRequest?.name == null || registrationRequest?.url == null)
            {
                return new BadRequestObjectResult("Please pass a JSON object containing name and url");
            }

            dynamic data = null;

            // Attempt to get first page
            try
            {
                var result = await httpClient.GetStringAsync(registrationRequest.url);
                data = JsonConvert.DeserializeObject(result);
            } catch (Exception ex)
            {
                return new UnprocessableEntityObjectResult($"Registration error while validating first page. Error retrieving '{registrationRequest.url}'. {ex.ToString()}");
            }

            // check for "license": "https://creativecommons.org/licenses/by/4.0/"
            if (data?.license != Utils.CC_BY_LICENSE)
            {
                return new UnprocessableEntityObjectResult("Registration error while validating first page. Invalid RPDE feed supplied: open license not found");
            }

            var matchingFeedStateList = await Utils.GetFeedStateFromQueues(registrationRequest.name);

            if (matchingFeedStateList.Count > 0)
            {
                if (matchingFeedStateList.First().url != registrationRequest.url)
                {
                    return new UnprocessableEntityObjectResult($"Conflicting feed already registered with same name '{registrationRequest.name}' using different url '{matchingFeedStateList.First().url}'.");
                } else
                {
                    // Do nothing, as feed is already registered
                }
            } else
            {
                // Purge all items that may be left in the database before registration starts
                queueCollector.Add(new FeedState()
                {
                    name = registrationRequest.name,
                    url = registrationRequest.url,
                    nextUrl = registrationRequest.url,
                    dateCreated = DateTime.UtcNow,
                    datasetUrl = registrationRequest.datasetUrl,
                    idIsNumeric = registrationRequest.idIsNumeric,
                    deletedItemDaysToLive = registrationRequest.deletedItemDaysToLive
                });
            }

            // Check that feed is not already registered
            /// TODO: Cosmos distinct query for feed name and initUrl
            /// If > 1 result returned fail due to "feed in an inconsistent state with multiple initUrls", add to dead-letter queue for cleanup?
            /// If initUrl in Cosmos query is not equal, fail due to "feed already registered with different key"
            /// If initUrl in Cosmos query is equal, succeed "feel already registered"
            /// 

            return new JsonResult(new ProxyRegistrationResponse
            {
                name = registrationRequest.name,
                url = Utils.GetFeedUrl(registrationRequest.name),
                dateCreated = matchingFeedStateList?.FirstOrDefault()?.dateCreated ?? DateTime.UtcNow,
                dateModified = matchingFeedStateList?.FirstOrDefault()?.dateModified ?? DateTime.UtcNow
            });
        }
    }

    public class ProxyRegistrationResponse
    {
        [JsonProperty("@context")]
        public string context { get; set; } = "https://schema.org/";
        [JsonProperty("@type")]
        public string type { get; set; } = "DataFeed";
        public string name { get; set; }
        public string url { get; set; }
        public DateTime dateCreated { get; set; }
        public DateTime dateModified { get; set; }
    }

    public class ProxyRegistrationRequest
    {
        [JsonProperty("@context")]
        public string context { get; set; } = "https://schema.org/";
        [JsonProperty("@type")]
        public string type { get; set; } = "DataFeed";
        public string name { get; set; }
        public string url { get; set; }
        public string datasetUrl { get; set; }
        public bool idIsNumeric { get; set; } = false;
        public int deletedItemDaysToLive { get; set; } = 7;  // 7 days is RPDE spec recommendation
    }
}
