using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;
using NPoco;
using System;
using System.Collections.Generic;
using System.Text;

namespace AzureRpdeProxy
{
    public class FeedState
    {
        public static FeedState DecodeFromMessage(Message message)
        {
            return JsonConvert.DeserializeObject<FeedState>(Encoding.UTF8.GetString(message.Body));
        }

        public Message EncodeToMessage(int delaySeconds)
        {
            return new Message {
                Body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(this)),
                ContentType = "application/json",
                ScheduledEnqueueTimeUtc = DateTime.UtcNow.AddSeconds(delaySeconds)
            };
        }

        public string name { get; set; }
        public string url { get; set; }
        public string nextUrl { get; set; }
        public string datasetUrl { get; set; }
        public DateTime dateCreated { get; set; }
        public DateTime dateModified { get; set; } = DateTime.Now;

        public long totalPagesRead { get; set; } = 0;
        public long totalItemsRead { get; set; }
        public long totalPollRequests { get; set; } = 0;
        public long totalErrors { get; set; } = 0;
        public int pollRetries { get; set; } = 0;
        public int purgeRetries { get; set; } = 0;
        public long purgedItems { get; set; } = 0;
        public long totalPurgeCount { get; set; } = -1;

        public bool idIsNumeric { get; set; } = false;
        public Guid id { get; set; } = Guid.NewGuid();

        public void ResetCounters()
        {
            totalPagesRead = 0;
            totalItemsRead = 0;
            totalPollRequests = 0;
            totalErrors = 0;
            pollRetries = 0;
            purgeRetries = 0;
            purgedItems = 0;
        }
    }

    public class MessageStatus
    {
        public FeedState feedState { get; set; }
        public DateTime expiration { get; internal set; }
        public DateTime delayedUntil { get; internal set; }
        public bool locked { get; internal set; }
        public DateTime lockedUntil { get; internal set; }
        public int retryAttempts { get; internal set; }
        public string deadLetterSource { get; internal set; }
        public bool isReceived { get; internal set; }
        public string queue { get; internal set; }
    }

    public class RpdeFeed
    {
        public string next { get; set; }
        public List<RpdeItem> items { get; set; }
        public string license { get; set; }
    }

    public class RpdeItem
    {
        public virtual dynamic id { get; set; }
        public long modified { get; set; }
        public string kind { get; set; }
        public string state { get; set; }
        public dynamic data { get; set; }
    }

    [TableName("items")]
    [PrimaryKey(new string[2] {"source", "id" }, AutoIncrement = false)]
    public class CachedRpdeItem
    {
        [Column("source")]
        public string source { get; set; }
        [Column("id")]
        public string id { get; set; }
        [Column("modified")]
        public long modified { get; set; }
        [Column("kind")]
        public string kind { get; set; }
        [Column("deleted")]
        public bool deleted { get; set; }
        [SerializedColumn("data")]
        public dynamic data { get; set; }
    }
}
