using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;
using NPoco;
using System;
using System.Collections.Generic;
using System.Text;
using System.Web;

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
            return EncodeToMessage(DateTime.UtcNow.AddSeconds(delaySeconds));
        }

        public Message EncodeToMessage(DateTimeOffset? expires)
        {
            return new Message {
                Body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(this)),
                ContentType = "application/json",
                ScheduledEnqueueTimeUtc = expires?.UtcDateTime ?? DateTime.UtcNow 
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
        public long lastPageReads { get; set; } = 0;
        public int pollRetries { get; set; } = 0;
        public int purgeRetries { get; set; } = 0;
        public long purgedItems { get; set; } = 0;
        public long totalPurgeCount { get; set; } = -1;
        
        public Guid id { get; set; } = Guid.NewGuid();
        public int deletedItemDaysToLive { get; set; } = 7; // 7 days is RPDE spec recommendation

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

    public class LastItem
    {
        public int? RecommendedPollInterval { get; set; }
        public DateTimeOffset? Expires { get; set; }
        public TimeSpan? MaxAge { get; set; }
    }

    public class RpdeFeed
    {
        public string next { get; set; }
        public List<RpdeItem> items { get; set; }
        public string license { get; set; }
    }

    public class RpdeItem
    {
        public RpdeItem ConvertToStringId()
        {
            id = id is int || id is long ? ((long)id).ToString("D20") : HttpUtility.UrlEncode(id);
            return this;
        }
        public virtual dynamic id { get; set; }
        public long modified { get; set; }
        public string kind { get; set; }
        public string state { get; set; }
        public dynamic data { get; set; }
    }

    public class DataCatalog
    {
        [JsonProperty("@context")]
        public string context
        {
            get { return "https://schema.org"; }
        }
        [JsonProperty("@type")]
        public string type
        {
            get { return "DataCatalog"; }
        }
        public string id { get; set; }
        public List<string> dataset { get; set; }
        public DateTime datePublished { get; set; } = DateTime.Now;
        public Organization publisher { get; set; }
        public string license { get; set; } = Utils.CC_BY_LICENSE;
    }

    public class Organization
    {
        public string type
        {
            get { return "Organization"; }
        }
        public string name { get; set; }
        public string url { get; set; }
    }

    [TableName("feeds")]
    [PrimaryKey("source", AutoIncrement = false)]
    public class Feed
    {
        [Column("source")]
        public string source { get; set; }
        [Column("url")]
        public string url { get; set; }
        [Column("datasetUrl")]
        public string datasetUrl { get; set; }
        [SerializedColumn("initialFeedState")]
        public FeedState initialFeedState { get; set; }
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
        [Column("expiry")]
        public DateTime? expiry { get; set; }
    }
}
