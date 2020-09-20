using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using ConsumerApp.Interfaces;
using Npgsql;

namespace ConsumerApp.Modules
{
    public class EntityFrameworkConsumer : Consumer
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
            Console.WriteLine($"sw.ElapsedMilliseconds start");
            using (var consumer =
                new ConsumerBuilder<int, string>(config)
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

                            var result = consumer.ConsumeBatch<int, string>(1000, 10);

                            if (result.Messages.Count > 0)
                            {
                                var connString = Utility.GetConnectionString("ConnectionStrings:DefaultConnectionEF");

                                using var conn = new NpgsqlConnection(connString);

                                conn.Open();
                                var valuesTableSql = string.Join(",", Enumerable.Range(0, result.Messages.Count).Select(i => $"(@p1{i}, @p2{i},@p3{i},@p4{i},@p5{i})"));
                                using (var tx = conn.BeginTransaction())
                                {
                                    using (var cmd = new NpgsqlCommand($"INSERT INTO \"Messages\" (\"Id\", \"Created\", \"Content\", \"IsReceived\", \"ReceivedTimestamp\") VALUES {valuesTableSql};", conn))
                                    {
                                        for (int i = 0; i < result.Messages.Count; ++i)
                                        {
                                            cmd.Parameters.AddWithValue($"p1{i}", result.Messages.ElementAt(i).Key);
                                            cmd.Parameters.AddWithValue($"p2{i}", DateTime.Now);
                                            cmd.Parameters.AddWithValue($"p3{i}", result.Messages.ElementAt(i).Value);
                                            cmd.Parameters.AddWithValue($"p4{i}", false);
                                            cmd.Parameters.AddWithValue($"p5{i}", DateTime.Now);
                                        }

                                        cmd.ExecuteNonQuery();
                                    }

                                    tx.Commit();
                                }
                                Console.WriteLine($"after database: {sw.ElapsedMilliseconds}, MC: {result.MessagesConsumed}, WM: {result.BatchWindowMilliseconds}");
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
                    consumer.Close();
                }


            }

            sw.Stop();
            Console.WriteLine($"sw.ElapsedMilliseconds: {sw.ElapsedMilliseconds}");

        }
    }
}