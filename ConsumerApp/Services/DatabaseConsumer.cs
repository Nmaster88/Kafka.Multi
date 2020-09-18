using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using ConsumerApp.DataAccess;
using ConsumerApp.Dtos;
using ConsumerApp.Interfaces;

namespace ConsumerApp.Services
{
    public class DatabaseConsumer : Consumer
    {
        public override void Run(string brokerList, List<string> topics, CancellationToken cancellationToken){
            //TODO: create the table for messages_Json here
            PostgresDbController postgresDb = new PostgresDbController();
            DbController db = postgresDb;

            db.Initialize();

            db.OpenConn();

            db.CreateDbIfnotExists();

            if (db.CreateTableIfNotExists("TopicMessages"))
            {
                Console.WriteLine("table created");
            }

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
                new ConsumerBuilder<long, string>(config)
                    .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                    .Build())
            {
                consumer.Assign(topics.Select(topic => new TopicPartitionOffset(topic, 0, Offset.Beginning)).ToList());
                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cancellationToken);

                            if (consumeResult.IsPartitionEOF)
                            {
                                Console.WriteLine(
                                    $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                                continue;
                            }

                            var result = consumer.ConsumeBatchDesirialize<long, ChannelMessagesJson>(1000, 50);
                            if (result.Messages.Count > 0)
                            {
                                var swsql = new Stopwatch();
                                swsql.Start();
                                db.InsertJsonBatchDesirializedIntoTableOpt(result);
                                swsql.Stop();
                                Console.WriteLine($"swsql batch: {swsql.ElapsedMilliseconds}");
                            }
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

                }
                finally
                {
                    consumer.Close();
                }
            }

            sw.Stop();
            Console.WriteLine($"sw.ElapsedMilliseconds: {sw.ElapsedMilliseconds}");

            db.CloseConn();
         }
    }
}