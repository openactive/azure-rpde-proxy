using Microsoft.Azure.Documents.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureRpdeProxy
{
    class Utils
    {
        public const string CC_BY_LICENSE = "https://creativecommons.org/licenses/by/4.0/";
        public const string QUEUE_NAME = "feedstate2";
        public const string PURGE_QUEUE_NAME = "purge";
        public const string REGISTRATION_QUEUE_NAME = "registration";

        public static string GetFeedUrl(string name)
        {
            return Environment.GetEnvironmentVariable("FeedBaseUrl") + "api/feeds/" + name;
        }
        
    }

    static class Extensions
    {
        public static Task<List<T>> ToListAsync<T>(this IQueryable<T> query) => query.AsDocumentQuery().ToListAsync();

        public static async Task<List<T>> ToListAsync<T>(this IDocumentQuery<T> queryable)
        {
            var list = new List<T>();

            while (queryable.HasMoreResults)
            {
                //Note that ExecuteNextAsync can return many records in each call
                var response = await queryable.ExecuteNextAsync<T>();
                Console.WriteLine($"GetPage Query: {response.RequestCharge} RUs for {response.Count} results");
                list.AddRange(response);
            }

            return list;
        }
    }
}
