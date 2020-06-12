using System;
//using System.ComponentModel.DataAnnotations.Schema;
//using Newtonsoft.Json;

namespace ConsumerApp.Dtos
{
    public class ChannelMessagesJson
    {
        public long Id { get; set; }

        public DateTime Created { get; set; }

        public string Content { get; set; }

        public DateTime ReceivedTimestamp { get; set; }

        public long MessageEventId { get; set; }
    }
}
