using System.Collections.Generic;
using ConsumerApp.Dtos;
using Npgsql;

namespace ConsumerApp.DataAccess
{
    public interface ITableContext
    {
        void CreateTableIfNotExists();
        void InsertJsonBatchIntoTable(BatchResult<long, string> result);
        void InsertJsonBatchDesirializedIntoTable(BatchResult<long, ChannelMessagesJson> result);
        List<Confluent.Kafka.Message<long, ChannelMessagesJson>> SelectResultsFromTable(IEnumerable<long> ids);
    }
}