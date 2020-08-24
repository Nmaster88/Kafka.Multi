﻿using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Confluent.Kafka;
using ProducerApp.Dtos;
using System.Text.Json;
using System.Collections.Generic;

namespace ProducerApp
{
    class Program
    {

        public static async Task Run_Single(ProducerConfig config, string kafkaTopic)
        {
            // Create the producer
            using (var producer = new ProducerBuilder<string, string>(config).Build())
            {
                Console.WriteLine("\n-----------------------------------------------------------------------");
                Console.WriteLine($"Producer {producer.Name} producing on topic {kafkaTopic}.");
                Console.WriteLine("-----------------------------------------------------------------------");
                Console.WriteLine("To create a kafka message with UTF-8 encoded key and value:");
                Console.WriteLine("> key value<Enter>");
                Console.WriteLine("To create a kafka message with a null key and UTF-8 encoded value:");
                Console.WriteLine("> value<enter>");
                Console.WriteLine("Ctrl-C to quit.\n");

                var cancelled = false;
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cancelled = true;
                };

                while (!cancelled)
                {
                    Console.Write("> ");

                    string text;
                    try
                    {
                        text = Console.ReadLine();
                    }
                    catch (IOException)
                    {
                        // IO exception is thrown when ConsoleCancelEventArgs.Cancel == true.
                        break;
                    }
                    if (text == null)
                    {
                        // Console returned null before 
                        // the CancelKeyPress was treated
                        break;
                    }

                    string key = null;
                    string val = text;

                    // split line if both key and value specified.
                    int index = text.IndexOf(" ");
                    if (index != -1)
                    {
                        key = text.Substring(0, index);
                        val = text.Substring(index + 1);
                    }

                    try
                    {
                        // Note: Awaiting the asynchronous produce request below prevents flow of execution
                        // from proceeding until the acknowledgement from the broker is received (at the 
                        // expense of low throughput).
                        var deliveryReport = await producer.ProduceAsync(
                            kafkaTopic, new Message<string, string> { Key = key, Value = val });

                        Console.WriteLine($"Delivered message to: {deliveryReport.TopicPartitionOffset}");
                    }
                    catch (ProduceException<string, string> e)
                    {
                        Console.WriteLine($"Failed to deliver message: {e.Message} [{e.Error.Code}]");
                    }
                }

            }
        }

        public static async Task Run_Multi(ProducerConfig config, string kafkaTopic)
        {
            // Create the producer
            using (var producer = new ProducerBuilder<long, string>(config).Build())
            {


                Console.WriteLine("\n-----------------------------------------------------------------------");
                Console.WriteLine($"Producer {producer.Name} producing on topic {kafkaTopic}.");
                Console.WriteLine("-----------------------------------------------------------------------");
                Console.WriteLine("100.000 messages will be generated");

                var DateTimeNow = DateTime.UtcNow;

                var generator = new UniqueIdGenerator.Net.Generator(1, DateTimeNow.Date);
                Stopwatch sw = new Stopwatch();
                sw.Start();

                var start = 0;

                for (int i = start; i < start + 100000; i++)
                {
                    //Create message
                    ChannelMessagesJson newMessage = new ChannelMessagesJson()
                    {
                        Id = (long)generator.NextLong(),
                        Created = DateTimeNow,
                        Content = LoremNET.Lorem.Words(5, 20)
                    };
                    var serializedNewMessage = JsonSerializer.Serialize(newMessage);
                    await producer.ProduceAsync(
                        kafkaTopic, new Message<long, string> { Key = newMessage.Id, Value = serializedNewMessage });
                    if (i < start + 40000)
                    {
                        //update message
                        ChannelMessagesJson updateMessage = new ChannelMessagesJson()
                        {
                            Id = (long)generator.NextLong(),
                            ReceivedTimestamp = DateTimeNow,
                            MessageEventId = newMessage.Id
                        };
                        var serializedUpdateMessage = JsonSerializer.Serialize(updateMessage);
                        await producer.ProduceAsync(
                            kafkaTopic, new Message<long, string> { Key = updateMessage.Id, Value = serializedUpdateMessage });
                    }

                }
                producer.Flush();
                sw.Stop();
                Console.WriteLine($"sw.ElapsedMilliseconds: {sw.ElapsedMilliseconds}");
            }
        }

        public static async Task Run_Multi_Sep(ProducerConfig config, string kafkaTopic)
        {
            // Create the producer
            using (var producer = new ProducerBuilder<long, string>(config).Build())
            {


                Console.WriteLine("\n-----------------------------------------------------------------------");
                Console.WriteLine($"Producer {producer.Name} producing on topic {kafkaTopic}.");
                Console.WriteLine("-----------------------------------------------------------------------");
                Console.WriteLine("100.000 messages will be generated");

                var DateTimeNow = DateTime.UtcNow;

                var generator = new UniqueIdGenerator.Net.Generator(1, DateTimeNow.Date);
                Stopwatch sw = new Stopwatch();
                sw.Start();

                List<ChannelMessagesJson> newMessages = new List<ChannelMessagesJson>();
                List<ChannelMessagesJson> ackMessages = new List<ChannelMessagesJson>();

                var countMessages = 0;
                var start = 0;
                for (int i = start; i < start + 60000; i++)
                {
                    //Create message
                    ChannelMessagesJson newMessage = new ChannelMessagesJson()
                    {
                        Id = (long)generator.NextLong(),
                        Created = DateTimeNow,
                        Content = LoremNET.Lorem.Words(5, 20)
                    };
                    newMessages.Add(newMessage);
                    var serializedNewMessage = JsonSerializer.Serialize(newMessage);
                    await producer.ProduceAsync(
                        kafkaTopic, new Message<long, string> { Key = newMessage.Id, Value = serializedNewMessage });
                    countMessages++;
                    // if (i < start + 40000)
                    // {
                    //     //update message
                    //     ChannelMessagesJson updateMessage = new ChannelMessagesJson()
                    //     {
                    //         Id = (long)generator.NextLong(),
                    //         ReceivedTimestamp = DateTimeNow,
                    //         MessageEventId = newMessage.Id
                    //     };
                    //     ackMessages.Add(updateMessage);
                    // }

                }

                for (int i = start; i < start + 40000; i++)
                {
                    ChannelMessagesJson updateMessage = new ChannelMessagesJson()
                    {
                        Id = (long)generator.NextLong(),
                        ReceivedTimestamp = DateTimeNow,
                        MessageEventId = newMessages[i].Id
                    };
                    var serializedUpdateMessage = JsonSerializer.Serialize(updateMessage);
                    await producer.ProduceAsync(
                        kafkaTopic, new Message<long, string> { Key = updateMessage.Id, Value = serializedUpdateMessage });
                    countMessages++;
                }
                //To send the ack messages only after the created messages
                // foreach (var ackMessage in ackMessages)
                // {
                //     var serializedUpdateMessage = JsonSerializer.Serialize(ackMessage);
                //     await producer.ProduceAsync(
                //         kafkaTopic, new Message<long, string> { Key = ackMessage.Id, Value = serializedUpdateMessage });
                //     countMessages++;
                // }

                producer.Flush();
                sw.Stop();
                Console.WriteLine($"sw: {sw.ElapsedMilliseconds}");
                Console.WriteLine($"Messages count : {countMessages}");
            }
        }

        public static async Task Main(string[] args)
        {
            //Modes of the producer
            //single - each line is write in the console
            //multi - produces a lot of messages
            //var mode = args[0];
            var mode = "multi";

            // The Kafka endpoint address
            string kafkaEndpoint = "127.0.0.1:9092";

            // The Kafka topic we'll be using
            string kafkaTopic = "new-message-topic";

            // Create the producer configuration
            var config = new ProducerConfig { BootstrapServers = kafkaEndpoint, EnableDeliveryReports = false, QueueBufferingMaxMessages = 200000 };

            switch (mode)
            {
                case "single":
                    await Run_Single(config, kafkaTopic);
                    break;

                case "multi":
                    await Run_Multi_Sep(config, kafkaTopic);
                    break;
            }


            // Since we are producing synchronously, at this point there will be no messages
            // in-flight and no delivery reports waiting to be acknowledged, so there is no
            // need to call producer.Flush before disposing the producer.
            // }

        }

    }
}