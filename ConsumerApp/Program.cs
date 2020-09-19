using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;
using ConsumerApp.Types;
using ConsumerApp.Services;

namespace ConsumerApp
{
    class Program
    {
        // /// <summary>
        // ///     In this example
        // ///         - offsets are manually committed.
        // ///         - no extra thread is created for the Poll (Consume) loop.
        // /// </summary>
        // public static void Run_Consume(string brokerList, List<string> topics, CancellationToken cancellationToken)
        // {
        //     var config = new ConsumerConfig
        //     {
        //         BootstrapServers = brokerList,
        //         GroupId = "csharp-consumer",
        //         EnableAutoCommit = false,
        //         StatisticsIntervalMs = 5000,
        //         SessionTimeoutMs = 6000,
        //         AutoOffsetReset = AutoOffsetReset.Earliest,
        //         EnablePartitionEof = true
        //     };

        //     const int commitPeriod = 5;

        //     // Note: If a key or value deserializer is not set (as is the case below), the 
        //     // deserializer corresponding to the appropriate type from Confluent.Kafka.Deserializers
        //     // will be used automatically (where available). The default deserializer for string
        //     // is UTF8. The default deserializer for Ignore returns null for all input data
        //     // (including non-null data).
        //     using (var consumer = new ConsumerBuilder<Ignore, string>(config)
        //         // Note: All handlers are called on the main .Consume thread.
        //         .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
        //         .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
        //         .SetPartitionsAssignedHandler((c, partitions) =>
        //         {
        //             Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}]");
        //             // possibly manually specify start offsets or override the partition assignment provided by
        //             // the consumer group by returning a list of topic/partition/offsets to assign to, e.g.:
        //             // 
        //             // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
        //         })
        //         .SetPartitionsRevokedHandler((c, partitions) =>
        //         {
        //             Console.WriteLine($"Revoking assignment: [{string.Join(", ", partitions)}]");
        //         })
        //         .Build())
        //     {
        //         consumer.Subscribe(topics);

        //         try
        //         {
        //             while (true)
        //             {
        //                 try
        //                 {
        //                     var consumeResult = consumer.Consume(cancellationToken);

        //                     if (consumeResult.IsPartitionEOF)
        //                     {
        //                         Console.WriteLine(
        //                             $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

        //                         continue;
        //                     }

        //                     Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");

        //                     if (consumeResult.Offset % commitPeriod == 0)
        //                     {
        //                         // The Commit method sends a "commit offsets" request to the Kafka
        //                         // cluster and synchronously waits for the response. This is very
        //                         // slow compared to the rate at which the consumer is capable of
        //                         // consuming messages. A high performance application will typically
        //                         // commit offsets relatively infrequently and be designed handle
        //                         // duplicate messages in the event of failure.
        //                         try
        //                         {
        //                             consumer.Commit(consumeResult);
        //                         }
        //                         catch (KafkaException e)
        //                         {
        //                             Console.WriteLine($"Commit error: {e.Error.Reason}");
        //                         }
        //                     }
        //                 }
        //                 catch (ConsumeException e)
        //                 {
        //                     Console.WriteLine($"Consume error: {e.Error.Reason}");
        //                 }
        //             }
        //         }
        //         catch (OperationCanceledException)
        //         {
        //             Console.WriteLine("Closing consumer.");
        //             consumer.Close();
        //         }
        //     }
        // }

        // /// <summary>
        // ///     In this example
        // ///         - consumer group functionality (i.e. .Subscribe + offset commits) is not used.
        // ///         - the consumer is manually assigned to a partition and always starts consumption
        // ///           from a specific offset (0).
        // /// </summary>
        // public static void Run_ManualAssign(string brokerList, List<string> topics, CancellationToken cancellationToken)
        // {
        //     var config = new ConsumerConfig
        //     {
        //         // the group.id property must be specified when creating a consumer, even 
        //         // if you do not intend to use any consumer group functionality.
        //         GroupId = new Guid().ToString(),
        //         BootstrapServers = brokerList,
        //         // partition offsets can be committed to a group even by consumers not
        //         // subscribed to the group. in this example, auto commit is disabled
        //         // to prevent this from occurring.
        //         EnableAutoCommit = true
        //     };
        //     Stopwatch sw = new Stopwatch();
        //     sw.Start();
        //     using (var consumer =
        //         new ConsumerBuilder<string, string>(config)
        //             .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
        //             .Build())
        //     {
        //         List<TopicMessages> messages = new List<TopicMessages>();
        //         consumer.Assign(topics.Select(topic => new TopicPartitionOffset(topic, 0, Offset.Beginning)).ToList());

        //         try
        //         {
        //             while (true)
        //             {

        //                 try
        //                 {
        //                     var consumeResult = consumer.Consume(cancellationToken);
        //                     // Note: End of partition notification has not been enabled, so
        //                     // it is guaranteed that the ConsumeResult instance corresponds
        //                     // to a Message, and not a PartitionEOF event.
        //                     Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: ${consumeResult.Message.Value}");

        //                     var channelMessage = Newtonsoft.Json.JsonConvert.DeserializeObject<ChannelMessages>(consumeResult.Message.Value);

        //                     messages.Add(new TopicMessages()
        //                     {
        //                         Content = channelMessage.Content
        //                     });
        //                 }
        //                 catch (ConsumeException e)
        //                 {
        //                     Console.WriteLine($"Consume error: {e.Error.Reason}");
        //                 }
        //             }
        //         }
        //         catch (OperationCanceledException)
        //         {
        //             Console.WriteLine("Closing consumer.");
        //             consumer.Close();
        //         }

        //         //Only save the results at the end
        //         //We can make that it is saved when we have a certain number of elements
        //         using (var db = new TestDbContext())
        //         {
        //             db.Messages.AddRange(
        //                 messages
        //             );
        //             var count = db.SaveChanges();
        //             Console.WriteLine("{0} records saved to database", count);
        //         }
        //     }

        //     sw.Stop();
        //     Console.WriteLine($"sw.ElapsedMilliseconds: {sw.ElapsedMilliseconds}");

        // }

        // public static void Run_MultiAssign_Multi(string brokerList, List<string> topics, CancellationToken cancellationToken)
        // {

        //     var config = new ConsumerConfig
        //     {
        //         // the group.id property must be specified when creating a consumer, even 
        //         // if you do not intend to use any consumer group functionality.
        //         GroupId = new Guid().ToString(),
        //         BootstrapServers = brokerList,
        //         // partition offsets can be committed to a group even by consumers not
        //         // subscribed to the group. in this example, auto commit is disabled
        //         // to prevent this from occurring.
        //         EnableAutoCommit = true
        //     };
        //     Stopwatch sw = new Stopwatch();
        //     sw.Start();
        //     Console.WriteLine($"sw.ElapsedMilliseconds start");
        //     using (var consumer =
        //         new ConsumerBuilder<int, string>(config)
        //             .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
        //             .Build())
        //     {
        //         consumer.Assign(topics.Select(topic => new TopicPartitionOffset(topic, 0, Offset.Beginning)).ToList());
        //         try
        //         {
        //             while (true)
        //             {

        //                 try
        //                 {

        //                     var result = consumer.ConsumeBatch<int, string>(1000, 10);

        //                     if (result.Messages.Count > 0)
        //                     {
        //                         var connString = Utility.GetConnectionString("ConnectionStrings:DefaultConnectionEF");

        //                         using var conn = new NpgsqlConnection(connString);

        //                         conn.Open();
        //                         var valuesTableSql = string.Join(",", Enumerable.Range(0, result.Messages.Count).Select(i => $"(@p1{i}, @p2{i},@p3{i},@p4{i},@p5{i})"));
        //                         using (var tx = conn.BeginTransaction())
        //                         {
        //                             using (var cmd = new NpgsqlCommand($"INSERT INTO \"Messages\" (\"Id\", \"Created\", \"Content\", \"IsReceived\", \"ReceivedTimestamp\") VALUES {valuesTableSql};", conn))
        //                             {
        //                                 for (int i = 0; i < result.Messages.Count; ++i)
        //                                 {
        //                                     cmd.Parameters.AddWithValue($"p1{i}", result.Messages.ElementAt(i).Key);
        //                                     cmd.Parameters.AddWithValue($"p2{i}", DateTime.Now);
        //                                     cmd.Parameters.AddWithValue($"p3{i}", result.Messages.ElementAt(i).Value);
        //                                     cmd.Parameters.AddWithValue($"p4{i}", false);
        //                                     cmd.Parameters.AddWithValue($"p5{i}", DateTime.Now);
        //                                 }

        //                                 cmd.ExecuteNonQuery();
        //                             }

        //                             tx.Commit();
        //                         }
        //                         Console.WriteLine($"after database: {sw.ElapsedMilliseconds}, MC: {result.MessagesConsumed}, WM: {result.BatchWindowMilliseconds}");
        //                     }
        //                 }
        //                 catch (ConsumeException e)
        //                 {
        //                     Console.WriteLine($"Consume error: {e.Error.Reason}");
        //                 }
        //             }
        //         }
        //         catch (OperationCanceledException)
        //         {
        //             Console.WriteLine("Closing consumer.");
        //             consumer.Close();
        //         }


        //     }

        //     sw.Stop();
        //     Console.WriteLine($"sw.ElapsedMilliseconds: {sw.ElapsedMilliseconds}");


        // }

        // public static void Run_MultiAssign_Json(string brokerList, List<string> topics, CancellationToken cancellationToken)
        // {
        //     //TODO: create the table for messages_Json here
        //     PostgresDbController postgresDb = new PostgresDbController();
        //     DbController db = postgresDb;

        //     db.Initialize();

        //     db.OpenConn();

        //     db.CreateDbIfnotExists();

        //     if (db.CreateTableIfNotExists("TopicMessages"))
        //     {
        //         Console.WriteLine("table created");
        //     }

        //     var config = new ConsumerConfig
        //     {
        //         // the group.id property must be specified when creating a consumer, even 
        //         // if you do not intend to use any consumer group functionality.
        //         GroupId = new Guid().ToString(),
        //         BootstrapServers = brokerList,
        //         // partition offsets can be committed to a group even by consumers not
        //         // subscribed to the group. in this example, auto commit is disabled
        //         // to prevent this from occurring.
        //         EnableAutoCommit = true
        //     };
        //     Stopwatch sw = new Stopwatch();
        //     sw.Start();
        //     using (var consumer =
        //         new ConsumerBuilder<long, string>(config)
        //             .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
        //             .Build())
        //     {
        //         consumer.Assign(topics.Select(topic => new TopicPartitionOffset(topic, 0, Offset.Beginning)).ToList());
        //         try
        //         {
        //             while (true)
        //             {
        //                 try
        //                 {
        //                     var result = consumer.ConsumeBatchDesirialize<long, ChannelMessagesJson>(1000, 50);
        //                     if (result.Messages.Count > 0)
        //                     {
        //                         var swsql = new Stopwatch();
        //                         swsql.Start();
        //                         db.InsertJsonBatchDesirializedIntoTableOpt(result);
        //                         swsql.Stop();
        //                         Console.WriteLine($"swsql batch: {swsql.ElapsedMilliseconds}");
        //                     }
        //                 }
        //                 catch (ConsumeException e)
        //                 {
        //                     Console.WriteLine($"Consume error: {e.Error.Reason}");
        //                 }
        //             }
        //         }
        //         catch (OperationCanceledException)
        //         {
        //             Console.WriteLine("Closing consumer.");

        //         }
        //         finally
        //         {
        //             consumer.Close();
        //         }
        //     }

        //     sw.Stop();
        //     Console.WriteLine($"sw.ElapsedMilliseconds: {sw.ElapsedMilliseconds}");

        //     db.CloseConn();
        // }

        private static void PrintUsage()
            => Console.WriteLine("Usage: .. <subscribe|manual> <broker,broker,..> <topic> [topic..]");

        private static void Consumer(string brokerList, List<string> topics, string mode)
        {
            Console.WriteLine($"Started consumer, Ctrl-C to stop consuming");

            ConsumerApp.Interfaces.Consumer consumer;

            //Modes of the producer
            //single - each line is write in the console
            //multi - produces a lot of messages
            if(String.IsNullOrEmpty(mode))
            {
                mode = ConsumerTypes.Database.ToString();
            }
            else
            {
                ConsumerTypes producerType;
                if(Enum.TryParse(mode, out producerType))
                {
                    if(Enum.IsDefined(typeof(ConsumerTypes),mode))
                    {
                        mode = producerType.ToString();
                    }
                    else
                    {
                        Console.WriteLine("The producer type passed as argument doesn't exits. mode defaults to multi");
                        mode = ConsumerTypes.Database.ToString();
                    }
                }
                else
                {
                    mode = ConsumerTypes.Database.ToString();
                }
            }

            if(mode == ConsumerTypes.single.ToString())
            {
                consumer = new SimpleConsumer();
            }
            else if(mode == ConsumerTypes.manual.ToString())
            {
                consumer = new ManualConsumer();
            }  
            else if(mode == ConsumerTypes.Database.ToString())
            {
                consumer = new DatabaseConsumer();
            } 
            else if(mode == ConsumerTypes.Dapper.ToString())
            {
                consumer = new DatabaseDapperConsumer();
            } 
            else 
            {
                consumer = new DatabaseConsumer();
            }

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            consumer.Run(brokerList, topics, cts.Token);
            // switch (mode)
            // {
            //     case "subscribe":
            //         Run_Consume(brokerList, topics, cts.Token);
            //         break;
            //     case "manual":
            //         Run_ManualAssign(brokerList, topics, cts.Token);
            //         break;
            //     case "multi":
            //         Run_MultiAssign_Multi(brokerList, topics, cts.Token);
            //         break;
            //     case "multijson":
            //         Run_MultiAssign_Json(brokerList, topics, cts.Token);
            //         break;
            //     default:
            //         PrintUsage();
            //         break;
            // }

        }

        static void Main(string[] args)
        {
            #region " kafka configuration "

                //var mode = args[0];
                var mode = "multijson";

                // The Kafka endpoint address
                string brokerList = "127.0.0.1:9092";

                // The Kafka topic we'll be using
                List<string> topics = new List<string>() { "new-message-topic" };

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
                //TODO: continue the refactor
            #endregion 

            Consumer(brokerList, topics, mode);
        }
    }
}
