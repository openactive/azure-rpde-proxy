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

        [FunctionName("FeedPage")]
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
                LastItem lastItem = null;
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

                    // This query will return one additional record (from the previous page) to check that the provided "source" value is valid
                    // (So even the last page will return at least 1 record)
                    SqlDataReader reader = await cmd.ExecuteReaderAsync();

                    // Call Read before accessing data.
                    if (await reader.ReadAsync())
                    {
                        var itemStrings = new List<string>();

                        //Skip the first row if it's the same as the query parameters (see comment above)
                        if ((reader.GetString((int)ResultColumns.id) != afterId &&
                              reader.GetInt64((int)ResultColumns.modified) != afterTimestamp) || reader.Read())
                        {
                            do
                            {
                                var timestamp = reader.GetInt64((int)ResultColumns.modified);
                                if (timestamp != Utils.LAST_PAGE_ITEM_RESERVED_MODIFIED)
                                {
                                    itemStrings.Add(reader.GetString((int)ResultColumns.data));

                                    // Get the last item values for the next URL
                                    afterTimestamp = timestamp;
                                    afterId = reader.GetString((int)ResultColumns.id);
                                }
                                else
                                {
                                    lastItem = JsonConvert.DeserializeObject<LastItem>(reader.GetString((int)ResultColumns.data));
                                }
                            }
                            while (await reader.ReadAsync());
                        }

                        itemCount = itemStrings.Count;

                        // Construct response using string concatenation instead of deserialisation for maximum efficiency
                        str.Append("{\"next\":");
                        str.Append(JsonConvert.ToString($"{Utils.GetFeedUrl(source)}?afterTimestamp={afterTimestamp}&afterId={afterId}"));
                        str.Append(",\"items\": [");
                        str.AppendJoin(',', itemStrings);
                        str.Append("],\"license\":");
                        str.Append(JsonConvert.ToString(Utils.CC_BY_LICENSE));
                        str.Append("}");
                       
                        // Call Close when done reading.
                        reader.Close();
                    } else
                    {
                        // Call Close when done reading.
                        reader.Close();

                        // Return 404 for invalid source, rather than just for last page
                        return req.CreateErrorResponse(HttpStatusCode.NotFound, $"'{source}' feed not found");
                    }
                }

                // Create response
                var resp = req.CreateJSONResponseFromString(HttpStatusCode.OK, str.ToString());

                // Pages other than the last page have a constant expiry set
                if (itemCount > 0)
                {
                    resp = resp.AsCachable(TimeSpan.FromHours(1));
                }
                else
                {
                    if (lastItem?.Expires != null)
                    {
                        // Add 2 seconds to expiry to account for proxy lag
                        const int ESTIMATED_PROXY_LATENCY_SECONDS = 2;
                        var expiresFromProxy = lastItem?.Expires?.AddSeconds(ESTIMATED_PROXY_LATENCY_SECONDS);
                        if (expiresFromProxy < DateTimeOffset.UtcNow)
                        {
                            // If the expiry has passed, project it forward based on the poll interval if possible
                            if (lastItem.RecommendedPollInterval != null)
                            {
                                resp = resp.AsCachable(ProjectExpiryForward((DateTimeOffset)expiresFromProxy, (int)lastItem.RecommendedPollInterval));
                                resp.Headers.Add(Utils.RECOMMENDED_POLL_INTERVAL_HEADER, Convert.ToString(lastItem.RecommendedPollInterval));
                            } else
                            {
                                // Default cache expiry
                                resp = resp.AsCachable(TimeSpan.FromSeconds(10));
                            }
                        }
                        else
                        {
                            resp = resp.AsCachable(expiresFromProxy);
                        }
                    }
                    else if (lastItem?.MaxAge != null)
                    {
                        resp = resp.AsCachable((TimeSpan)lastItem.MaxAge);
                    }
                    else
                    {
                        // Default cache expiry
                        resp = resp.AsCachable(TimeSpan.FromSeconds(10));
                    }
                }

                sw.Stop();

                log.LogWarning($"GETPAGE TIMER {sw.ElapsedMilliseconds} ms.");

                return resp;
            }
            catch (SqlException ex)
            {
                if (SqlUtils.SqlTransientErrorNumbers.Contains(ex.Number) || ex.Message.Contains("timeout", StringComparison.InvariantCultureIgnoreCase))
                {
                    log.LogWarning($"Throttle on GetPage, retry after {SqlUtils.SqlRetrySecondsRecommendation} seconds.");
                    return req.CreateTooManyRequestsResponse(TimeSpan.FromSeconds(SqlUtils.SqlRetrySecondsRecommendation));
                } else
                {
                    log.LogError("Error during GetPage: " + ex.ToString());
                    return req.CreateErrorResponse(HttpStatusCode.InternalServerError, ex.Message);
                }
            }
        }

        private static DateTimeOffset ProjectExpiryForward(DateTimeOffset expires, int recommendedPollInterval)
        {
            var expiresFromNowInSeconds = DateTimeOffset.UtcNow.Subtract(expires).TotalSeconds;

            var intervalsCount = Math.Ceiling(expiresFromNowInSeconds / recommendedPollInterval);
            return expires.AddSeconds(intervalsCount * recommendedPollInterval);
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

        public static HttpResponseMessage AsCachable(this HttpResponseMessage request, TimeSpan cacheMaxAge)
        {
            request.Headers.CacheControl = new CacheControlHeaderValue()
            {
                Public = true,
                MaxAge = cacheMaxAge,
                SharedMaxAge = cacheMaxAge
            };

            return request;
        }

        public static HttpResponseMessage AsCachable(this HttpResponseMessage request, DateTimeOffset? expires)
        {
            request.Headers.CacheControl = new CacheControlHeaderValue()
            {
                Public = true
            };
            request.Content.Headers.Expires = expires;

            return request;
        }

        public static HttpResponseMessage CreateJSONResponse(this HttpRequest request, HttpStatusCode statusCode, object o)
        {
            var e = JsonConvert.SerializeObject(o,
                Newtonsoft.Json.Formatting.None,
                new JsonSerializerSettings
                {
                    NullValueHandling = NullValueHandling.Ignore
                });

            var resp = request.CreateJSONResponseFromString(statusCode, e);
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
