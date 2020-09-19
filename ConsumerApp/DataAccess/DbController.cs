using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;
using ConsumerApp.Dtos;
using ConsumerApp.Models;
using Npgsql;
using NpgsqlTypes;

namespace ConsumerApp.DataAccess
{
    public interface DbController
    {
        void Initialize();

        void OpenConn();

        void CloseConn();

        void CreateDbIfnotExists();

        bool CreateTableIfNotExists(string tableName);

        bool InsertJsonBatchIntoTable(BatchResult<long, string> result);

        bool InsertJsonBatchDesirializedIntoTable(BatchResult<long, ChannelMessagesJson> result);

        bool InsertJsonBatchDesirializedIntoTableOpt(BatchResult<long, ChannelMessagesJson> result);
    }
    public class PostgresDbController : DbController
    {
        string connString = Utility.GetConnectionString("ConnectionStrings:DefaultConnection");

        string dbName = Utility.GetConnectionString("ConnectionStrings:DbName");

        NpgsqlConnection conn = null;

        public void Initialize()
        {
            conn = new NpgsqlConnection(connString);
        }

        public void OpenConn()
        {
            conn.Open();
        }

        public void CloseConn()
        {
            conn.Close();
        }

        public void CreateDbIfnotExists()
        {
            var query = "SELECT 'CREATE DATABASE " + this.dbName + "' WHERE NOT EXISTS ( SELECT datname FROM pg_catalog.pg_database WHERE datname =  '" + Utility.GetConnectionString("ConnectionStrings:DbName") + "' );";

            using var cmd = new NpgsqlCommand(query, conn);

            var createDb = cmd.ExecuteScalar();

            if (createDb != null)
            {
                using var cmdCreateDb = new NpgsqlCommand(createDb.ToString(), conn);
                cmdCreateDb.ExecuteScalar();
            }
            cmd.Dispose();
            conn.ChangeDatabase(Utility.GetConnectionString("ConnectionStrings:DbName"));
        }

        public bool CreateTableIfNotExists(string tableName)
        {
            if (conn.Database != this.dbName)
            {
                return false;
            }

            using var cmd = new NpgsqlCommand();
            cmd.Connection = conn;
            //TODO: Change this to be generic
            cmd.CommandText = "CREATE TABLE IF NOT EXISTS topicmessages (id bigint, content jsonb);";
            cmd.ExecuteNonQuery();
            cmd.Dispose();
            return true;
        }

        public bool InsertJsonBatchIntoTable(BatchResult<long, string> result)
        {
            var valuesTableSql = string.Join(",", Enumerable.Range(0, result.Messages.Count).Select(i => $"(@p1{i}, @p2{i} :: jsonb )"));
            var options = new JsonSerializerOptions
            {
                AllowTrailingCommas = true
            };
            using (var cmd = new NpgsqlCommand($"INSERT INTO \"topicmessages\" (\"id\", \"content\") VALUES {valuesTableSql};", conn))
            {
                if (result.Messages.Count > 0)
                {
                    var queryIds = result.Messages.Where(me => JsonSerializer.Deserialize<ChannelMessagesJson>(me.Value).MessageEventId == 0).Select(me => me.Key);
                }
                for (int i = 0; i < result.Messages.Count; ++i)
                {
                    var deserializedMessage = JsonSerializer.Deserialize<ChannelMessagesJson>(result.Messages.ElementAt(i).Value);

                    cmd.Parameters.AddWithValue($"p1{i}", result.Messages.ElementAt(i).Key);

                    if (deserializedMessage.MessageEventId != 0)
                    {
                        var Content = result.Messages.FirstOrDefault(m => m.Key == deserializedMessage.MessageEventId);
                        cmd.Parameters.AddWithValue($"p2{i}", $"{{ \"id\":{deserializedMessage.MessageEventId}, \"content\": \"{JsonSerializer.Deserialize<ChannelMessagesJson>(Content.Value).Content}\" ,\"status\":\"acknowledged\", \"isreceived\":true,\"receivedtimestamp\":\"{deserializedMessage.ReceivedTimestamp}\"}}");
                    }
                    else
                    {

                        cmd.Parameters.AddWithValue($"p2{i}", $"{{ \"id\":{deserializedMessage.Id}, \"content\": \"{deserializedMessage.Content}\" ,\"status\":\"new\",\"created\":\"{deserializedMessage.Created}\"}}");
                    }
                }

                cmd.ExecuteNonQuery();
            }
            return true;
        }

        public bool InsertJsonBatchDesirializedIntoTable(BatchResult<long, ChannelMessagesJson> result)
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

                using (var cmd = new NpgsqlCommand($"INSERT INTO \"topicmessages\" (\"id\", \"content\") VALUES {valuesTableSql};", conn))
                {
                    bool executeQuery = true;
                    for (int i = 0; i < result.Messages.Count; ++i)
                    {
                        executeQuery = true;
                        var deserializedMessage = result.Messages.ElementAt(i).Value;

                        cmd.Parameters.AddWithValue($"p1{i}", result.Messages.ElementAt(i).Key);

                        if (deserializedMessage.MessageEventId != 0)
                        {
                            Message<long, ChannelMessagesJson> Content = values.FirstOrDefault(m => m.Key == deserializedMessage.MessageEventId);
                            if (Content == null)
                            {
                                //In this rare event, it means the acknowledge is probably on the same batch as the created event
                                var ContentBatch = result.Messages.FirstOrDefault(m => m.Key == deserializedMessage.MessageEventId);
                            }
                            cmd.Parameters.AddWithValue($"p2{i}", $"{{ \"id\":{deserializedMessage.MessageEventId}, \"content\": \"{Content.Value.Content}\" ,\"status\":\"acknowledged\", \"isreceived\":true,\"receivedtimestamp\":\"{deserializedMessage.ReceivedTimestamp}\"}}");
                        }
                        else
                        {
                            cmd.Parameters.AddWithValue($"p2{i}", $"{{ \"id\":{deserializedMessage.Id}, \"content\": \"{deserializedMessage.Content}\" ,\"status\":\"new\",\"created\":\"{deserializedMessage.Created}\"}}");
                        }
                    }
                    if (executeQuery == true)
                    {
                        cmd.ExecuteNonQuery();
                    }

                }
            }

            return true;
        }


        public bool InsertJsonBatchDesirializedIntoTableOpt(BatchResult<long, ChannelMessagesJson> result)
        {
            var valuesTableSql = string.Join(",", Enumerable.Range(0, result.Messages.Count).Select(i => $"(@p1{i}, @p2{i} :: jsonb )"));
            var options = new JsonSerializerOptions
            {
                AllowTrailingCommas = true
            };
            if (result.Messages.Count > 0)
            {
                //To get from the batch all new messages key, to query the database
                var queryIds = result.Messages.Where(me => me.Value.MessageEventId != 0).Select(me => me.Value.MessageEventId).ToList();
                List<Message<long, string>> values = new List<Message<long, string>>();
                if (queryIds.Count() != 0)
                {
                    values = SelectResultsFromTableString(queryIds);
                }
                using (var cmd = new NpgsqlCommand($"INSERT INTO \"topicmessages\" (\"id\", \"content\") VALUES {valuesTableSql};", conn))
                {
                    bool executeQuery = true;
                    for (int i = 0; i < result.Messages.Count; ++i)
                    {
                        executeQuery = true;
                        var deserializedMessage = result.Messages.ElementAt(i).Value;

                        cmd.Parameters.AddWithValue($"p1{i}", result.Messages.ElementAt(i).Key);

                        if (deserializedMessage.MessageEventId != 0)
                        {
                            Message<long, string> Content = values.FirstOrDefault(m => m.Key == deserializedMessage.MessageEventId);
                            if (Content == null)
                            {
                                //In this rare event, it means the acknowledge is probably on the same batch as the created event
                                var ContentBatch = result.Messages.FirstOrDefault(m => m.Key == deserializedMessage.MessageEventId);
                                cmd.Parameters.AddWithValue($"p2{i}", $"{{ \"id\":{deserializedMessage.MessageEventId}, \"content\": \"{ContentBatch.Value}\" ,\"status\":\"acknowledged\", \"isreceived\":true,\"receivedtimestamp\":\"{deserializedMessage.ReceivedTimestamp}\"}}");
                            }
                            else
                            {
                                cmd.Parameters.AddWithValue($"p2{i}", $"{{ \"id\":{deserializedMessage.MessageEventId}, \"content\": \"{Content.Value}\" ,\"status\":\"acknowledged\", \"isreceived\":true,\"receivedtimestamp\":\"{deserializedMessage.ReceivedTimestamp}\"}}");
                            }
                        }
                        else
                        {

                            cmd.Parameters.AddWithValue($"p2{i}", $"{{ \"id\":{deserializedMessage.Id}, \"content\": \"{deserializedMessage.Content}\" ,\"status\":\"new\",\"created\":\"{deserializedMessage.Created}\"}}");
                        }
                    }
                    if (executeQuery == true)
                    {
                        cmd.ExecuteNonQuery();
                    }

                }
            }

            return true;
        }

        public class NewMessageJson
        {
            public long id { get; set; }

            public string content { get; set; }

            public string status { get; set; }

            public DateTime Created { get; set; }
        }

        public List<Message<long, ChannelMessagesJson>> SelectResultsFromTable(IEnumerable<long> ids)
        {
            var swSel = new Stopwatch();
            swSel.Start();
            List<Message<long, ChannelMessagesJson>> messages = new List<Message<long, ChannelMessagesJson>>();
            using (var cmd = new NpgsqlCommand($"SELECT id,content FROM \"topicmessages\" WHERE id IN ({string.Join(",", ids)});", conn))
            {
                long key;
                ChannelMessagesJson val;
                NpgsqlDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    key = Int64.Parse(reader[0].ToString());
                    var content = reader[1].ToString();
                    val = JsonSerializer.Deserialize<ChannelMessagesJson>(content);
                    messages.Add(new Message<long, ChannelMessagesJson>() { Key = key, Value = val });
                }
                reader.Close();
            }
            swSel.Stop();
            Console.WriteLine($"swSel: {swSel.ElapsedMilliseconds}");
            return messages;
        }

        public List<Message<long, string>> SelectResultsFromTableString(List<long> ids)
        {
            var swSel = new Stopwatch();
            swSel.Start();
            List<Message<long, string>> messages = new List<Message<long, string>>();

            using (var cmd = new NpgsqlCommand($"SELECT id,content FROM \"topicmessages\" WHERE id IN ({string.Join(",", ids)});", conn))
            {
                long key;
                string val;
                NpgsqlDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    key = Int64.Parse(reader[0].ToString());
                    var content = reader[1].ToString();
                    val = JsonSerializer.Deserialize<ChannelMessagesJson>(content).Content;
                    messages.Add(new Message<long, string>() { Key = key, Value = val });
                }
                reader.Close();
            }
            swSel.Stop();
            Console.WriteLine($"swSel: {swSel.ElapsedMilliseconds}");
            return messages;
        }

        public List<Message<long, ChannelMessagesJson>> SelectResultsFromTableTemp(List<long> ids)
        {
            var swSel = new Stopwatch();
            swSel.Start();
            List<Message<long, ChannelMessagesJson>> messages = new List<Message<long, ChannelMessagesJson>>();

            // using (var cmd = new NpgsqlCommand($"CREATE TEMPORARY TABLE createdEvs(bigint id) ON COMMIT DELETE ROWS", conn))
            // {
            //     cmd.ExecuteNonQuery();
            //     var count = ids.Count();
            //     var valuesTableSql = string.Join(",", Enumerable.Range(0, count).Select(i => $"(@p1{i})"));
            //     cmd.CommandText = $"INSERT INTO \"topicmessages\" (\"id\", \"content\") VALUES {valuesTableSql};";
            //     for (int i = 0; i < count; ++i)
            //     {
            //         cmd.Parameters.AddWithValue($"p1{i}", ids[i]);
            //     }
            //     cmd.ExecuteNonQuery();
            // }

            using (var cmd = new NpgsqlCommand($"SELECT * FROM \"topicmessages\" WHERE id IN ({string.Join(",", ids)});", conn))
            {
                long key;
                ChannelMessagesJson val;
                NpgsqlDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    key = Int64.Parse(reader[0].ToString());
                    var content = reader[1].ToString();
                    val = JsonSerializer.Deserialize<ChannelMessagesJson>(content);
                    messages.Add(new Message<long, ChannelMessagesJson>() { Key = key, Value = val });
                }
                reader.Close();
            }
            swSel.Stop();
            Console.WriteLine($"swSel: {swSel.ElapsedMilliseconds}");
            return messages;
        }
    }
}