using System.Collections.Generic;
using ConsumerApp.Dtos;
using ConsumerApp.Types;

namespace ConsumerApp.DataAccess
{
    public interface IDbContext
    {
        void Initialize();

        void OpenConn();

        void CloseConn();

        void CommandBuilder(string query);

        void CommandAddParameters(string parameterName, object value);

        object CommandExecuteQuery(ExecuteQueryTypes type = ExecuteQueryTypes.Scalar);

        void CommandDispose();

        void CreateDbIfNotExists();
    }
}