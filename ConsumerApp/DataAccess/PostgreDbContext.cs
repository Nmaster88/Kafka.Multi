using System.Collections.Generic;
using Confluent.Kafka;
using ConsumerApp.Dtos;
using ConsumerApp.Types;
using Npgsql;

namespace ConsumerApp.DataAccess
{
    public class PostgresDbContext : IDbContext
    {
        string connString = Utility.GetConnectionString("ConnectionStrings:PostgreConnection");

        string dbName = Utility.GetConnectionString("ConnectionStrings:PostgreDbName");

        NpgsqlConnection conn = null;

        public void Initialize()
        {
            conn = new NpgsqlConnection(connString);
        }

        public void OpenConn()
        {
            if (conn == null)
            {
                throw new System.Exception("conn does not exist");
            }
            conn.Open();
        }

        public void CloseConn()
        {
            if (conn == null)
            {
                throw new System.Exception("conn does not exist");
            }
            conn.Close();
        }

        public void CreateDbIfNotExists()
        {
            var query = "SELECT 'CREATE DATABASE " + this.dbName + "' WHERE NOT EXISTS ( SELECT datname FROM pg_catalog.pg_database WHERE datname =  '" + Utility.GetConnectionString("ConnectionStrings:DbName") + "' );";

            using var cmd = new NpgsqlCommand(query, conn);

            var createDb = cmd.ExecuteScalar();

            if (createDb != null)
            {
                using var cmdCreateDb = new NpgsqlCommand(createDb.ToString(), conn);
                cmdCreateDb.ExecuteScalar();
            }
            else
            {
                throw new System.Exception("db was not created");
            }

            cmd.Dispose();
            conn.ChangeDatabase(Utility.GetConnectionString("ConnectionStrings:DbName"));
        }

        NpgsqlCommand _command = null;

        public void CommandBuilder(string query)
        {
            if (string.IsNullOrEmpty(query))
            {
                throw new System.Exception("the query passed to the commandbuilder is not valid");
            }
            _command = new NpgsqlCommand();
            _command.Connection = conn;
            _command.CommandText = query;
        }

        public void CommandAddParameters(string parameterName, object value)
        {
            _command.Parameters.AddWithValue(parameterName, value);
        }

        public object CommandExecuteQuery(ExecuteQueryTypes type = ExecuteQueryTypes.Scalar)
        {
            object value = null;
            if (type == ExecuteQueryTypes.NonQuery)
            {
                value = _command.ExecuteNonQuery();
            }
            else if (type == ExecuteQueryTypes.Scalar)
            {
                value = _command.ExecuteScalar();
            }
            else if (type == ExecuteQueryTypes.Reader)
            {
                value = _command.ExecuteReader();
            }
            else
            {
                value = _command.ExecuteScalar();
            }
            if (value != null)
            {
                throw new System.Exception("The query didn't return any response");
            }
            return value;
        }

        public void CommandDispose()
        {
            _command.Dispose();
            _command = null;
        }
    }
}