using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.ServiceBus;
using System.Collections.Generic;
using Microsoft.Azure.ServiceBus.Core;
using System.Linq;
using Microsoft.Extensions.Configuration;

namespace AzureRpdeProxy
{
    public static class Status
    {
        [FunctionName("Status")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string name = req.Query["name"];

            string ServiceBusConnectionString = Environment.GetEnvironmentVariable("ServiceBusConnection");
            var queueClients = new List<IMessageReceiver> {
                new MessageReceiver(ServiceBusConnectionString, Utils.FEED_STATE_QUEUE_NAME),
                new MessageReceiver(ServiceBusConnectionString, Utils.FEED_STATE_QUEUE_NAME + "/$DeadLetterQueue"),
                new MessageReceiver(ServiceBusConnectionString, Utils.PURGE_QUEUE_NAME),
                new MessageReceiver(ServiceBusConnectionString, Utils.REGISTRATION_QUEUE_NAME)
            };
            List<MessageStatus> list = new List<MessageStatus>();
   
            foreach (var client in queueClients) {
                List<MessageStatus> lastRetrievedList = null;
                do
                {
                    lastRetrievedList = (await client.PeekAsync(100)).Select(m => new MessageStatus
                    {
                        queue = client.Path,
                        feedState = FeedState.DecodeFromMessage(m),
                        expiration = m.ExpiresAtUtc,
                        delayedUntil = m.ScheduledEnqueueTimeUtc,
                        locked = m.SystemProperties.IsLockTokenSet,
                        lockedUntil = m.SystemProperties.LockedUntilUtc,
                        retryAttempts = m.SystemProperties.DeliveryCount,
                        deadLetterSource = m.SystemProperties.DeadLetterSource,
                        isReceived = m.SystemProperties.IsReceived
                    }).ToList();
                    list.AddRange(lastRetrievedList);
                } while (lastRetrievedList.Count > 0);
            }
            
            return new JsonResult(list.OrderBy(x => x.feedState.name).ToList(), new JsonSerializerSettings
            {
                Formatting = Formatting.Indented
            });
        }
    }
}
