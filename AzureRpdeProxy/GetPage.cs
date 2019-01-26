using System;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Mvc.WebApiCompatShim;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Linq;
using System.Collections.Generic;
using System.Net.Http;
using System.Net;
using System.Text;
using NPoco;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Data;
using System.Web;
using Microsoft.AspNetCore.Mvc;
using System.Net.Http.Headers;

namespace AzureRpdeProxy
{
    public static class GetPage
    {
        enum ResultColumns
        {
            data = 0,
            modified = 1,
            id = 2
        }

        [FunctionName("GetPage")]
        public static async Task<HttpResponseMessage> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "feeds/{source}")] HttpRequest req, string source,
            ILogger log)
        {
            long afterTimestamp = 0;

            string afterTimestampString = req.Query["afterTimestamp"];
            if (afterTimestampString != null && !long.TryParse(afterTimestampString, out afterTimestamp))
            {
                return req.CreateErrorResponse(HttpStatusCode.BadRequest, "afterTimestamp must be an integer");
            }
            string afterId = req.Query["afterId"];

            try
            {
                var sw = new Stopwatch();
                sw.Start();

                StringBuilder str = new StringBuilder();
                int itemCount = 0;
                using (SqlConnection connection = new SqlConnection(SqlUtils.SqlDatabaseConnectionString))
                {
                    SqlCommand cmd = new SqlCommand("READ_ITEM_PAGE", connection);
                    cmd.CommandType = CommandType.StoredProcedure;

                    cmd.Parameters.Add(
                        new SqlParameter()
                        {
                            ParameterName = "@source",
                            SqlDbType = SqlDbType.VarChar,
                            Value = source,
                        });
                    cmd.Parameters.Add(
                        new SqlParameter()
                        {
                            ParameterName = "@id",
                            SqlDbType = SqlDbType.NVarChar,
                            Value = afterId ?? "",
                        });
                    cmd.Parameters.Add(
                        new SqlParameter()
                        {
                            ParameterName = "@modified",
                            SqlDbType = SqlDbType.BigInt,
                            Value = afterTimestamp,
                        });

                    connection.Open();

                    SqlDataReader reader = await cmd.ExecuteReaderAsync();

                    // Construct using string concatenation instead of deserialisation for maximum efficiency

                    // Call Read before accessing data.
                    if (await reader.ReadAsync())
                    {
                        str.Append(",\"items\": [");
                        //Skip the first row if it's the same as the query parameters
                        if ((reader.GetString((int)ResultColumns.id) != afterId &&
                              reader.GetInt64((int)ResultColumns.modified) != afterTimestamp) || reader.Read())
                        {
                            str.Append(reader.GetString(0));
                            // Get the last item values for the next URL
                            afterTimestamp = reader.GetInt64((int)ResultColumns.modified);
                            afterId = reader.GetString((int)ResultColumns.id);
                            itemCount++;
                            while (await reader.ReadAsync())
                            {
                                str.Append(",");
                                str.Append(reader.GetString(0));
                                // Get the last item values for the next URL
                                afterTimestamp = reader.GetInt64((int)ResultColumns.modified);
                                afterId = reader.GetString((int)ResultColumns.id);
                                itemCount++;
                            }
                        }
                        str.Append("],\"license\":");
                        str.Append(JsonConvert.ToString(Utils.CC_BY_LICENSE));
                        str.Append("}");
                        // Add next URL to beginning of response
                        str.Insert(0, "{\"next\":" + JsonConvert.ToString($"{Utils.GetFeedUrl(source)}?afterTimestamp={afterTimestamp}&afterId={afterId}"));
                    } else
                    {
                        // Call Close when done reading.
                        reader.Close();

                        // Return 404 for invalid source, rather than just for last page
                        return req.CreateErrorResponse(HttpStatusCode.NotFound, $"'{source}' feed not found");
                    }

                    // Call Close when done reading.
                    reader.Close();
                }

                // Create response
                var resp = req.CreateCachableJSONResponseFromString(str.ToString(),
                    itemCount > 0 ? TimeSpan.FromHours(1) : TimeSpan.FromSeconds(10));

                sw.Stop();

                log.LogWarning($"GETPAGE TIMER {sw.ElapsedMilliseconds} ms.");

                // TODO: Add cache headers / Cache middleware
                return resp;
                /*
                using (var db = new Database(SqlUtils.SqlDatabaseConnectionString, DatabaseType.SqlServer2012, SqlClientFactory.Instance))
                {
                    var limit = 500;
                    // This query will return one additional record (from the previous page) to check that the provided "source" value is valid (note >= instead of >)
                    var whereClause = afterTimestamp > 0 && afterId != null ? " AND ((modified = @1 AND id >= @2) OR (modified > @1))" : "";
                    var results = await db.QueryAsync<CachedRpdeItem>($"SELECT TOP {limit} u.* from [dbo].[items] u (nolock) WHERE source = @0 {whereClause} ORDER BY modified, id", source, afterTimestamp, afterId);
                    sw.Stop();

                    // As results are async this will run the ORM deserialisation of the item JSON in parallel 
                    foreach (var i in results)
                    {
                        items.Add(new RpdeItem
                        {
                            id = i.id,
                            modified = i.modified,
                            kind = i.kind,
                            state = i.deleted ? "deleted" : "updated",
                            data = i.data
                        });
                    }

                    // Will always be > 0 is if there are any items in the database for the chosen source value, due to additional record included above, assuming the table is not deleted
                    if (items.Count > 0)
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

                        return req.CreateCachableJSONResponse(new RpdeFeed
                            {
                                next = $"{Utils.GetFeedUrl(source)}?afterTimestamp={afterTimestamp}&afterId={afterId}",
                                items = items,
                                license = Utils.CC_BY_LICENSE
                            },
                            items.Count() > 0 ? 3600 : 10 // Recommendation from https://developer.openactive.io/publishing-data/data-feeds/scaling-feeds
                        );
                    }
                    else
                    {
                        // Return 404 for invalid source, rather than just for last page
                        return  req.CreateErrorResponse(HttpStatusCode.NotFound, $"'{source}' feed not found");
                    }
                }
                */
            }
            catch (SqlException ex)
            {
                if (SqlUtils.SqlTransientErrorNumbers.Contains(ex.Number))
                {
                    log.LogWarning($"Throttle on GetPage, retry after {SqlUtils.SqlRetrySecondsRecommendation} seconds.");
                    return req.CreateTooManyRequestsResponse(TimeSpan.FromSeconds(SqlUtils.SqlRetrySecondsRecommendation));// StatusCodeResult((HttpStatusCode)StatusCodes.Status429TooManyRequests, $"Rate Limit Reached. Retry in {ex.RetryAfter.TotalSeconds} seconds.");
                } else
                {
                    log.LogError("Error during GetPage: " + ex.ToString());
                    return req.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.Message);
                }
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

        public static HttpResponseMessage CreateCachableJSONResponseFromString(this HttpRequest request, string jsonString, TimeSpan cacheMaxAge)
        {
            var resp = request.CreateJSONResponseFromString(HttpStatusCode.OK, jsonString);
  
            resp.Headers.CacheControl = new CacheControlHeaderValue()
            {
                Public = true,
                MaxAge = cacheMaxAge
            };

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

        public static HttpResponseMessage CreateJSONResponseFromString(this HttpRequest request, HttpStatusCode statusCode, string jsonString)
        {
            // Note this uses the compatability shim in Microsoft.AspNetCore.Mvc.WebApiCompatShim to get an HttpRequestMessage out of an HttpRequest
            // It can be removed once this code is fully ported to ASP.NET Core.
            HttpRequestMessage req = request.HttpContext.GetHttpRequestMessage();

            var resp = req.CreateResponse(statusCode);
            resp.Content = new StringContent(jsonString, Encoding.UTF8, "application/json");
            return resp;
        }
    }
}
