using System;

namespace ProducerApp.Models
{
    public class ReceivedMessages
    {
        public int Id { get; set; }

        public DateTime ReceivedTimestamp { get; set; }

        public int MessageEventId { get; set; }

        //public string Key { get; set; }

        //public string Value { get; set; }

    }
}
