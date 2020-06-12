using System;

namespace ConsumerApp.Models
{
    public class TopicMessages
    {
        public long Id { get; set; }

        public DateTime Created { get; set; }

        public string Content { get; set; }

        public bool IsReceived { get; set; }

        public DateTime ReceivedTimestamp { get; set; }

    }
}
