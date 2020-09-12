using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using ConsumerApp.DataAccess.TestDb;
using ConsumerApp.Interfaces;
using ConsumerApp.Models;

namespace ConsumerApp.Services
{
    public class ManualConsumer : Consumer
    {
        public override void Run(string brokerList, List<string> topics, CancellationToken cancellationToken){
            var config = new ConsumerConfig
            {
                // the group.id property must be specified when creating a consumer, even 
                // if you do not intend to use any consumer group functionality.
                GroupId = new Guid().ToString(),
                BootstrapServers = brokerList,
                // partition offsets can be committed to a group even by consumers not
                // subscribed to the group. in this example, auto commit is disabled
                // to prevent this from occurring.
                EnableAutoCommit = true
            };
            Stopwatch sw = new Stopwatch();
            sw.Start();
            using (var consumer =
                new ConsumerBuilder<string, string>(config)
                    .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                    .Build())
            {
                List<TopicMessages> messages = new List<TopicMessages>();
                consumer.Assign(topics.Select(topic => new TopicPartitionOffset(topic, 0, Offset.Beginning)).ToList());

                try
                {
                    while (true)
                    {

                        try
                        {
                            var consumeResult = consumer.Consume(cancellationToken);
                            // Note: End of partition notification has not been enabled, so
                            // it is guaranteed that the ConsumeResult instance corresponds
                            // to a Message, and not a PartitionEOF event.
                            Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: ${consumeResult.Message.Value}");

                            var channelMessage = Newtonsoft.Json.JsonConvert.DeserializeObject<ChannelMessages>(consumeResult.Message.Value);

                            messages.Add(new TopicMessages()
                            {
                                Content = channelMessage.Content
                            });
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Closing consumer.");
                    consumer.Close();
                }

                //Only save the results at the end
                //We can make that it is saved when we have a certain number of elements
                using (var db = new TestDbContext())
                {
                    db.Messages.AddRange(
                        messages
                    );
                    var count = db.SaveChanges();
                    Console.WriteLine("{0} records saved to database", count);
                }
            }

            sw.Stop();
            Console.WriteLine($"sw.ElapsedMilliseconds: {sw.ElapsedMilliseconds}");

        }
    }
}