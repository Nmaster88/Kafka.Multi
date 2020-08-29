using System;
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using ProducerApp.Dtos;
using ProducerApp.Interfaces;

namespace ProducerApp.Services
{
    public class MultiProducer : Producer
    {
        public override async Task Run(ProducerConfig config, string kafkaTopic){
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
    }
}