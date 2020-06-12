using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using ConsumerApp.Dtos;
using Microsoft.Extensions.Configuration;
namespace ConsumerApp
{
    public class BatchResult<TKey, TValue>
    {
        public BatchResult(ICollection<Message<TKey, TValue>> messages, int batchWindowMilliseconds, int messagesConsumed)
        {
            this.Messages = messages;
            this.MessagesConsumed = messagesConsumed;
            this.BatchWindowMilliseconds = batchWindowMilliseconds;
        }

        public ICollection<Message<TKey, TValue>> Messages { get; private set; }
        public int BatchWindowMilliseconds { get; private set; }

        public int MessagesConsumed { get; private set; }

    }

    public static class KafkaConsumerExtensions
    {
        // extends consumer 
        //maxBatchSize - How big is the batch size
        //windowMilliseconds - how much is the milliseconds we want to ellapse
        public static BatchResult<TKey, TValue> ConsumeBatch<TKey, TValue>(this IConsumer<TKey, TValue> consumer, int maxBatchSize, int windowMilliseconds)
        {
            var sw = new Stopwatch();
            sw.Start();
            List<Message<TKey, TValue>> messages = new List<Message<TKey, TValue>>();

            for (int i = 0; i < maxBatchSize; ++i)
            {
                var consumeResult = consumer.Consume(Math.Max(0, windowMilliseconds - (int)sw.ElapsedMilliseconds));

                if (consumeResult != null && !consumeResult.IsPartitionEOF)
                {
                    messages.Add(consumeResult.Message);
                }

                if (sw.ElapsedMilliseconds >= windowMilliseconds)
                {
                    Console.WriteLine($"EM>WM {i}");
                    break;
                }
            }

            sw.Stop();

            return new BatchResult<TKey, TValue>(messages, (int)sw.ElapsedMilliseconds, messages.Count);
        }

        public static BatchResult<TKey, Tobj> ConsumeBatchDesirialize<TKey, Tobj>(this IConsumer<TKey, string> consumer, int maxBatchSize, int windowMilliseconds)
        {
            var sw = new Stopwatch();
            sw.Start();
            List<Message<TKey, Tobj>> messages = new List<Message<TKey, Tobj>>();

            for (int i = 0; i < maxBatchSize; ++i)
            {
                var consumeResult = consumer.Consume(Math.Max(0, windowMilliseconds - (int)sw.ElapsedMilliseconds));

                if (consumeResult != null && !consumeResult.IsPartitionEOF)
                {
                    messages.Add(new Message<TKey, Tobj>() { Key = consumeResult.Message.Key, Value = JsonSerializer.Deserialize<Tobj>(consumeResult.Message.Value) });
                }

                if (sw.ElapsedMilliseconds >= windowMilliseconds)
                {
                    Console.WriteLine($"EM>WM {i}");
                    break;
                }
            }

            sw.Stop();

            return new BatchResult<TKey, Tobj>(messages, (int)sw.ElapsedMilliseconds, messages.Count);
        }
    }
}