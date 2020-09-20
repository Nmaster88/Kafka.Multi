using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using ConsumerApp.DataAccess;
using ConsumerApp.Dtos;
using ConsumerApp.Interfaces;

namespace ConsumerApp.Modules
{
    public class PostgreDbConsumer : Consumer
    {
        public override void Run(string brokerList, List<string> topics, CancellationToken cancellationToken)
        {
            //TODO: create the table for messages_Json here
            //PostgresDbContext postgresDb = new PostgresDbContext();
            //IDbContext db = postgresDb;

            var DbContext = new DbContextStrategy();
            DbContext.SetStrategy(new PostgresDbContext());
            DbContext.CreateDatabaseIfNotExist();

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

            DbContext.OpenConnection();



            using (var consumer =
                new ConsumerBuilder<long, string>(config)
                    .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                    .Build())
            {
                consumer.Assign(topics.Select(topic => new TopicPartitionOffset(topic, 0, Offset.Beginning)).ToList());
                try
                {
                    //TODO improve this to use stategy pattern
                    ITableContext _tableContext = new TopicMessagesJsonContext(DbContext._dbContext);
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
                                _tableContext.InsertJsonBatchDesirializedIntoTable(result);
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

            DbContext.CloseConnection();
        }
    }
}