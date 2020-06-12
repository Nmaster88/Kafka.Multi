using System;
using System.ComponentModel.DataAnnotations.Schema;

namespace ConsumerApp.Models
{
    public class TopicMessagesJson
    {
        public long id { get; set; }

        [Column(TypeName = "jsonb")]
        public TopicMessages content { get; set; }
    }
}
