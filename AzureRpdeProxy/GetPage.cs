using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Mvc.WebApiCompatShim;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System.Linq;
using System.Collections.Generic;
using Microsoft.Azure.Documents;
using System.Net.Http;
using System.Net;
using System.Globalization;
using System.Web.Http;
using System.Text;

namespace AzureRpdeProxy
{
    public static class GetPage
    {
        [FunctionName("GetPage")]
        public static async Task<HttpResponseMessage> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "feeds/{source}")] HttpRequest req, string source,
            [CosmosDB(
                databaseName: "openactive",
                collectionName: "Items",
                ConnectionStringSetting = "CosmosDBConnection")] DocumentClient client,
            ILogger log)
        {
            long afterTimestamp = 0;

            string afterTimestampString = req.Query["afterTimestamp"];
            if (afterTimestampString != null && !long.TryParse(afterTimestampString, out afterTimestamp))
            {
                return req.CreateErrorResponse(HttpStatusCode.BadRequest, "afterTimestamp must be an integer");
            }
            string afterId = req.Query["afterId"];

            Uri collectionUri = UriFactory.CreateDocumentCollectionUri("openactive", "Items");

            List<CachedRpdeItem> items;
            try
            {
                var itemQuery = client.CreateDocumentQuery<CachedRpdeItem>(collectionUri, new FeedOptions { MaxItemCount = 500, PartitionKey = new PartitionKey(source) }).OrderBy(p => p.modified); //.ThenBy(p => p.id);
                if (afterTimestamp > 0 && afterId != null)
                {
                    // The query above will return one additional record (from the previous page) to check that the provided "source" value is valid
                    var itemQueryWithWhere = itemQuery.Where(p =>
                        (p.modified == afterTimestamp && p.id == afterId) || // Note this is the addition that returns one additional record
                        (p.modified == afterTimestamp && p.id.CompareTo(afterId) > 0) ||
                        (p.modified > afterTimestamp)
                    );
                    items = await itemQueryWithWhere.Take(500).ToListAsync();
                }
                else
                {
                    items = await itemQuery.Take(500).ToListAsync();
                }
            } catch (DocumentClientException ex)
            {
                if (ex.StatusCode == (HttpStatusCode)StatusCodes.Status429TooManyRequests)
                {
                    // TODO: Include header
                    //response.Headers.Add("Retry-After", ResetSeconds.ToString(CultureInfo.InvariantCulture));

                    log.LogWarning($"Throttle on GetPage, retry after {ex.RetryAfter.TotalSeconds} seconds.");
                    return req.CreateTooManyRequestsResponse(ex.RetryAfter);// StatusCodeResult((HttpStatusCode)StatusCodes.Status429TooManyRequests, $"Rate Limit Reached. Retry in {ex.RetryAfter.TotalSeconds} seconds.");
                } else
                {
                    log.LogError("Error during GetPage: " + ex.ToString());
                    return req.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.Message);
                }
            }

            // Will always be > 0 is if there are any items in CosmosDB for the chosen source value, due to additional record included above, assuming the table is not deleted
            if (items.Count() > 0)
            {
                if (items[0].id == afterId && items[0].modified == afterTimestamp)
                {
                    items.RemoveAt(0);
                }

                // Check again as the list is now shorter
                if (items.Count() > 0)
                {
                    afterTimestamp = items.Last().modified;
                    afterId = items.Last().id;
                }

                var result = req.CreateCachableJSONResponse(new RpdeFeed
                {
                    next = $"{Utils.GetFeedUrl(source)}?afterTimestamp={afterTimestamp}&afterId={afterId}",
                    items = items.Select(i => new RpdeItem
                        {
                            id = i.id,
                            modified = i.modified,
                            kind = i.kind,
                            state = i.deleted ? "deleted" : "updated",
                            data = i.data
                        }).ToList(),
                        license = Utils.CC_BY_LICENSE
                    },
                    items.Count() > 0 ? 3600 : 10 // Recommendation from https://developer.openactive.io/publishing-data/data-feeds/scaling-feeds
                 );

                return result;
            }
            else
            {
                // Return 404 for invalid source, rather than just for last page
                return  req.CreateErrorResponse(HttpStatusCode.NotFound, $"'{source}' feed not found");
            }
        }

        public static HttpResponseMessage CreateTooManyRequestsResponse(this HttpRequest req, TimeSpan RetryAfter)
        {
            var response = req.CreateErrorResponse(HttpStatusCode.TooManyRequests, $"Status Code: {(int)HttpStatusCode.TooManyRequests}; {HttpStatusCode.TooManyRequests}; " + string.Format("Rate Limit Reached. Retry in {0} seconds.", RetryAfter.TotalSeconds));
            response.Headers.Add("Retry-After", RetryAfter.TotalSeconds.ToString("0"));
            return response;
        }

        public static HttpResponseMessage CreateErrorResponse(this HttpRequest req, HttpStatusCode statusCode, string errorMessage)
        {
            // Note this error forces the response to JSON to remove the need for content negotiation
            return req.CreateJSONResponse(statusCode, new
            {
                message = errorMessage
            });
        }

        public static HttpResponseMessage CreateCachableJSONResponse(this HttpRequest req, object o, int seconds)
        {
            var resp = req.CreateJSONResponse(HttpStatusCode.OK, o);
            // TODO: Add cache headers / Cache middleware
            return resp;
        }

        public static HttpResponseMessage CreateJSONResponse(this HttpRequest request, HttpStatusCode statusCode, object o)
        {
            // Note this uses the compatability shim in Microsoft.AspNetCore.Mvc.WebApiCompatShim to get an HttpRequestMessage out of an HttpRequest
            // It can be removed once this code is fully ported to ASP.NET Core.
            HttpRequestMessage req = request.HttpContext.GetHttpRequestMessage();
          
            var e = JsonConvert.SerializeObject(o,
                Newtonsoft.Json.Formatting.None,
                new JsonSerializerSettings
                {
                    NullValueHandling = NullValueHandling.Ignore
                });

            var resp = req.CreateResponse(statusCode);

            resp.Content = new StringContent(e, Encoding.UTF8, "application/json");
            return resp;
        }
    }
}
