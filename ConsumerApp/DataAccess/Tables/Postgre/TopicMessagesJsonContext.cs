using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Confluent.Kafka;
using ConsumerApp.Dtos;
using Npgsql;

namespace ConsumerApp.DataAccess
{
    public class TopicMessagesJsonContext : ITableContext
    {
        IDbContext _context;
        public TopicMessagesJsonContext(IDbContext context)
        {
            this._context = context;
        }
        public void CreateTableIfNotExists()
        {
            _context.CommandBuilder("CREATE TABLE IF NOT EXISTS topicmessagesjson (id bigint, content jsonb);");
            _context.CommandExecuteQuery();
            _context.CommandDispose();
        }

        public void InsertJsonBatchIntoTable(BatchResult<long, string> result)
        {
            var valuesTableSql = string.Join(",", Enumerable.Range(0, result.Messages.Count).Select(i => $"(@p1{i}, @p2{i} :: jsonb )"));
            var options = new JsonSerializerOptions
            {
                AllowTrailingCommas = true
            };

            _context.CommandBuilder($"INSERT INTO \"topicmessages\" (\"id\", \"content\") VALUES {valuesTableSql};");
            if (result.Messages.Count > 0)
            {
                var queryIds = result.Messages.Where(me => JsonSerializer.Deserialize<ChannelMessagesJson>(me.Value).MessageEventId == 0).Select(me => me.Key);
            }
            for (int i = 0; i < result.Messages.Count; ++i)
            {
                var deserializedMessage = JsonSerializer.Deserialize<ChannelMessagesJson>(result.Messages.ElementAt(i).Value);

                _context.CommandAddParameters($"p1{i}", result.Messages.ElementAt(i).Key);
                if (deserializedMessage.MessageEventId != 0)
                {
                    var Content = result.Messages.FirstOrDefault(m => m.Key == deserializedMessage.MessageEventId);
                    _context.CommandAddParameters($"p2{i}", $"{{ \"id\":{deserializedMessage.MessageEventId}, \"content\": \"{JsonSerializer.Deserialize<ChannelMessagesJson>(Content.Value).Content}\" ,\"status\":\"acknowledged\", \"isreceived\":true,\"receivedtimestamp\":\"{deserializedMessage.ReceivedTimestamp}\"}}");
                }
                else
                {
                    _context.CommandAddParameters($"p2{i}", $"{{ \"id\":{deserializedMessage.Id}, \"content\": \"{deserializedMessage.Content}\" ,\"status\":\"new\",\"created\":\"{deserializedMessage.Created}\"}}");
                }
            }
            _context.CommandExecuteQuery();
            _context.CommandDispose();
        }

        public void InsertJsonBatchDesirializedIntoTable(BatchResult<long, ChannelMessagesJson> result)
        {
            var valuesTableSql = string.Join(",", Enumerable.Range(0, result.Messages.Count).Select(i => $"(@p1{i}, @p2{i} :: jsonb )"));
            var options = new JsonSerializerOptions
            {
                AllowTrailingCommas = true
            };
            if (result.Messages.Count > 0)
            {
                //To get from the batch of ack events all new messages key, to query the database
                var queryIds = result.Messages.Where(me => me.Value.MessageEventId != 0).Select(me => me.Value.MessageEventId).ToList();
                List<Message<long, ChannelMessagesJson>> values = new List<Message<long, ChannelMessagesJson>>();
                if (queryIds.Count() != 0)
                {
                    values = SelectResultsFromTable(queryIds);
                }

                _context.CommandBuilder($"INSERT INTO \"topicmessages\" (\"id\", \"content\") VALUES {valuesTableSql};");
                bool executeQuery = true;
                for (int i = 0; i < result.Messages.Count; ++i)
                {
                    executeQuery = true;
                    var deserializedMessage = result.Messages.ElementAt(i).Value;

                    _context.CommandAddParameters($"p1{i}", result.Messages.ElementAt(i).Key);

                    if (deserializedMessage.MessageEventId != 0)
                    {
                        Message<long, ChannelMessagesJson> Content = values.FirstOrDefault(m => m.Key == deserializedMessage.MessageEventId);
                        if (Content == null)
                        {
                            //In this rare event, it means the acknowledge is probably on the same batch as the created event
                            var ContentBatch = result.Messages.FirstOrDefault(m => m.Key == deserializedMessage.MessageEventId);
                        }
                        _context.CommandAddParameters($"p2{i}", $"{{ \"id\":{deserializedMessage.MessageEventId}, \"content\": \"{Content.Value.Content}\" ,\"status\":\"acknowledged\", \"isreceived\":true,\"receivedtimestamp\":\"{deserializedMessage.ReceivedTimestamp}\"}}");
                    }
                    else
                    {
                        _context.CommandAddParameters($"p2{i}", $"{{ \"id\":{deserializedMessage.Id}, \"content\": \"{deserializedMessage.Content}\" ,\"status\":\"new\",\"created\":\"{deserializedMessage.Created}\"}}");
                    }
                }
                if (executeQuery == true)
                {
                    _context.CommandExecuteQuery();
                }
            }
            _context.CommandDispose();
        }

        public List<Message<long, ChannelMessagesJson>> SelectResultsFromTable(IEnumerable<long> ids)
        {
            var swSel = new Stopwatch();
            swSel.Start();
            List<Message<long, ChannelMessagesJson>> messages = new List<Message<long, ChannelMessagesJson>>();
            _context.CommandBuilder($"SELECT id,content FROM \"topicmessages\" WHERE id IN ({string.Join(",", ids)});");
            long key;
            ChannelMessagesJson val;
            NpgsqlDataReader reader = _context.CommandExecuteQuery(Types.ExecuteQueryTypes.Reader) as NpgsqlDataReader;
            while (reader.Read())
            {
                key = Int64.Parse(reader[0].ToString());
                var content = reader[1].ToString();
                val = JsonSerializer.Deserialize<ChannelMessagesJson>(content);
                messages.Add(new Message<long, ChannelMessagesJson>() { Key = key, Value = val });
            }
            reader.Close();
            swSel.Stop();
            Console.WriteLine($"swSel: {swSel.ElapsedMilliseconds}");
            return messages;
        }
    }
}