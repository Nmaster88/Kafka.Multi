using System;

namespace ProducerApp.Models
{
    public class ChannelMessages
    {
        public int Id { get; set; }

        public DateTime Created { get; set; }

        public string Content { get; set; }

        public DateTime ReceivedTimestamp { get; set; }

        public int MessageEventId { get; set; }

        //public string Key { get; set; }

        //public string Value { get; set; }

    }
}
