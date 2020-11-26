using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace BlTools.MssqlFluentSqlWrapper
{
    public class FluentSqlCommand
    {
        #region [ Private Fields ]

        private readonly SqlCommand _command;
        private readonly SqlParameterCollection _parameters;
        private Action<SqlDataReader, int> _dataReaderAction;
        private Func<FluentSqlCommand, bool> _onFailAction;
        private readonly bool _leaveConnectionOpened;
        private SqlParameter _returnValueParameter;

        #endregion

        #region [ ctor ]

        private FluentSqlCommand()
        {
            _command = new SqlCommand();
            _parameters = _command.Parameters;
            Status = ExecStatus.NotExecuted;
        }

        public FluentSqlCommand(string connectionString)
            :this()
        {
            _command.Connection = new SqlConnection(connectionString);
        }

        public FluentSqlCommand(SqlConnection connection, bool leaveOpened = false)
            :this()
        {
            _command.Connection = connection;
            _leaveConnectionOpened = leaveOpened;
        }

        #endregion

        #region [ Command Configuration ]

        public FluentSqlCommand StoredProc(string procName)
        {
            _command.CommandText = procName;
            _command.CommandType = CommandType.StoredProcedure;
            return this;
        }

        public FluentSqlCommand Query(string queryText)
        {
            _command.CommandText = queryText;
            _command.CommandType = CommandType.Text;
            return this;
        }

        public FluentSqlCommand WithTimeout(int commandTimeoutSeconds)
        {
            _command.CommandTimeout = commandTimeoutSeconds;
            return this;
        }

        public FluentSqlCommand WithTransaction(SqlTransaction transaction)
        {
            _command.Transaction = transaction;
            return this;
        }

        public FluentSqlCommand WithReturnValue()
        {
            _returnValueParameter = new SqlParameter("__returnValue", SqlDbType.Int) { Direction = ParameterDirection.ReturnValue };
            _parameters.Add(_returnValueParameter);
            return this;
        }

        /// <summary>
        /// Handles exceptions. Function should return bool value indicating whether exception was handled or not (and should be re-thrown in this case).
        /// </summary>
        /// <param name="failAction"></param>
        /// <returns></returns>
        public FluentSqlCommand OnFailure(Func<FluentSqlCommand, bool> failAction)
        {
            _onFailAction = failAction;
            return this;
        }

        #region [ Parameters ]

        public FluentSqlCommand AddParam<T>(string name, T value)
        {
            _parameters.AddWithValue(name, EnsureValue(value));
            return this;
        }

        public FluentSqlCommand AddParam(string name, DateTime value)
        {
            AddParam(name, value, SqlDbType.DateTime2);
            return this;
        }

        public FluentSqlCommand AddParam(string name, DateTime? value)
        {
            AddParam(name, value, SqlDbType.DateTime2);
            return this;
        }

        public FluentSqlCommand AddParam<T>(string name, T value, SqlDbType dbType)
        {
            var parameter = new SqlParameter(name, dbType) { Value = EnsureValue(value) };
            _parameters.Add(parameter);
            return this;
        }

        public FluentSqlCommand AddParam<T>(string name, T value, SqlDbType dbType, int size)
        {
            var parameter = new SqlParameter(name, dbType, size) { Value = EnsureValue(value) };
            _parameters.Add(parameter);
            return this;
        }

        public FluentSqlCommand AddParam(SqlParameter parameter)
        {
            _parameters.Add(parameter);
            return this;
        }

        public FluentSqlCommand AddOutParam(string name, SqlDbType dbType)
        {
            AddOutParam(name, dbType,
                dbType == SqlDbType.NVarChar
                    ? 4000
                    : dbType == SqlDbType.VarChar || dbType == SqlDbType.VarBinary
                        ? 8000
                        : 0);
            return this;
        }

        public FluentSqlCommand AddOutParam(string name, SqlDbType dbType, int size)
        {
            var parameter = new SqlParameter(name, dbType) {Direction = ParameterDirection.Output, Size = size};
            _parameters.Add(parameter);
            return this;
        }

        public FluentSqlCommand AddInOutParam<T>(string name, T value)
        {
            var parameter = new SqlParameter(name, EnsureValue(value)) {Direction = ParameterDirection.InputOutput};
            _parameters.Add(parameter);
            return this;
        }

        #endregion

        #endregion

        #region [ Command Execution ]

        public int ExecNonQuery()
        {
            var result = Exec(ExecType.NonQuery);
            return result.NonQueryResult;
        }

        public async Task<int> ExecNonQueryAsync()
        {
            var result = await ExecAsync(ExecType.NonQuery);
            return result.NonQueryResult;
        }

        public T ExecScalar<T>()
        {
            var execResult = Exec(ExecType.Scalar);
            var result = execResult.ScalarResult;
            return DBNull.Value.Equals(result) ? default : (T) result;
        }

        public async Task<T> ExecScalarAsync<T>()
        {
            var execResult = await ExecAsync(ExecType.Scalar);
            var result = execResult.ScalarResult;
            return DBNull.Value.Equals(result) ? default : (T) result;
        }

        /// <summary>
        /// Execute reader and call action for every read. Provides reader result number as action parameter.
        /// </summary>
        /// <param name="action">Action to be called for every read.</param>
        public void ExecReader(Action<SqlDataReader, int> action)
        {
            _dataReaderAction = action;
            Exec(ExecType.Reader);
        }

        public async Task ExecReaderAsync(Action<SqlDataReader, int> action)
        {
            _dataReaderAction = action;
            await ExecAsync(ExecType.Reader);
        }

        public void ExecReader(Action<SqlDataReader> action)
        {
            void ActionWrap(SqlDataReader reader, int resultNumber) => action(reader);
            _dataReaderAction = ActionWrap;
            Exec(ExecType.Reader);
        }

        public async Task ExecReaderAsync(Action<SqlDataReader> action)
        {
            void ActionWrap(SqlDataReader reader, int resultNumber) => action(reader);
            _dataReaderAction = ActionWrap;
            await ExecAsync(ExecType.Reader);
        }

        public List<T> ExecReadItemList<T>(Func<SqlDataReader, T> itemBuilder)
        {
            var result = new List<T>();
            void ActionWrap(SqlDataReader reader, int resultNumber) => result.Add(itemBuilder(reader));
            _dataReaderAction = ActionWrap;
            Exec(ExecType.Reader);
            return result;
        }

        public async Task<List<T>> ExecReadItemListAsync<T>(Func<SqlDataReader, T> itemBuilder)
        {
            var result = new List<T>();
            void ActionWrap(SqlDataReader reader, int resultNumber) => result.Add(itemBuilder(reader));
            _dataReaderAction = ActionWrap;
            await ExecAsync(ExecType.Reader);
            return result;
        }

        public T ExecReadItem<T>(Func<SqlDataReader, T> itemBuilder)
        {
            var result = default(T);
            void ActionWrap(SqlDataReader reader, int resultNumber) => result = itemBuilder(reader);
            _dataReaderAction = ActionWrap;
            Exec(ExecType.Reader);
            return result;
        }

        public async Task<T> ExecReadItemAsync<T>(Func<SqlDataReader, T> itemBuilder)
        {
            var result = default(T);
            void ActionWrap(SqlDataReader reader, int resultNumber) => result = itemBuilder(reader);
            _dataReaderAction = ActionWrap;
            await ExecAsync(ExecType.Reader);
            return result;
        }

        public string ExecReadXml()
        {
            var result = Exec(ExecType.XmlReader);
            return result.XmlResult;
        }

        public async Task<string> ExecReadXmlAsync()
        {
            var result = await ExecAsync(ExecType.XmlReader);
            return result.XmlResult;
        }

        public DataSet ExecFillDataSet(DataSet dataSet)
        {
            var result = Exec(ExecType.DataSet, new ExecResult { FillDataSetResult = dataSet });
            return result.FillDataSetResult;
        }

        public async Task<DataSet> ExecFillDataSetAsync(DataSet dataSet)
        {
            var result = await ExecAsync(ExecType.DataSet, new ExecResult { FillDataSetResult = dataSet });
            return result.FillDataSetResult;
        }

        public DataSet ExecFillDataSet()
        {
            return ExecFillDataSet(new DataSet());
        }

        public async Task<DataSet> ExecFillDataSetAsync()
        {
            return await ExecFillDataSetAsync(new DataSet());
        }

        #endregion

        #region [ Command Post Execution Actions ]

        public ExecStatus Status { get; private set; }

        public Exception Error { get; private set; }

        public T GetParamValue<T>(string name)
        {
            if (!_parameters.Contains(name))
            {
                throw new InvalidOperationException($"No parameter with name [\"{name}\"] found");
            }
            var result = _parameters[name].Value;
            return (T)(result == DBNull.Value ? default : result);
        }

        public T GetResult<T>()
        {
            if (_returnValueParameter == null)
            {
                throw new InvalidOperationException($"Can not get return value. Return value was not configured by {nameof(WithReturnValue)}");
            }
            var result = _returnValueParameter.Value;
            return (T)(result == DBNull.Value ? default : result);
        }

        #endregion

        #region [ Private methods ]

        private static object EnsureValue<T>(T value)
        {
            return value == null ? (object)DBNull.Value : value;
        }

        private ExecResult Exec(ExecType execType, ExecResult result = null)
        {
            result ??= new ExecResult();
            using (_leaveConnectionOpened ? null : _command.Connection) // using null is safe.
            {
                try
                {
                    if (_command.Connection.State != ConnectionState.Open)
                    {
                        _command.Connection.Open();
                    }

                    using (_command)
                    {
                        switch (execType)
                        {
                            case ExecType.NonQuery:
                            {
                                result.NonQueryResult = _command.ExecuteNonQuery();
                                break;
                            }
                            case ExecType.Scalar:
                            {
                                result.ScalarResult = _command.ExecuteScalar();
                                break;
                            }
                            case ExecType.Reader:
                            {
                                using var reader = _command.ExecuteReader();
                                var resultNumber = 0;
                                do
                                {
                                    while (reader.Read())
                                    {
                                        _dataReaderAction?.Invoke(reader, resultNumber);
                                    }
                                    resultNumber++;
                                } while (reader.NextResult());

                                break;
                            }
                            case ExecType.DataSet:
                            {
                                using var adapter = new SqlDataAdapter(_command);
                                adapter.Fill(result.FillDataSetResult);
                                break;
                            }
                            case ExecType.XmlReader:
                            {
                                using var reader = _command.ExecuteXmlReader();
                                if (reader.Read())
                                {
                                    result.XmlResult = reader.ReadOuterXml();
                                }

                                break;
                            }
                        }
                        
                        Status = ExecStatus.Success;
                    }
                }
                catch (Exception ex)
                {
                    HandleException(ex);
                }
            }

            return result;
        }

        private async Task<ExecResult> ExecAsync(ExecType execType, ExecResult result = null)
        {
            result ??= new ExecResult();
            using (_leaveConnectionOpened ? null : _command.Connection) // using null is safe.
            {
                try
                {
                    if (_command.Connection.State != ConnectionState.Open)
                    {
                        await _command.Connection.OpenAsync();
                    }

                    using (_command)
                    {
                        switch (execType)
                        {
                            case ExecType.NonQuery:
                            {
                                result.NonQueryResult = await _command.ExecuteNonQueryAsync();
                                break;
                            }
                            case ExecType.Scalar:
                            {
                                result.ScalarResult = await _command.ExecuteScalarAsync();
                                break;
                            }
                            case ExecType.Reader:
                            {
                                using var reader = await _command.ExecuteReaderAsync();
                                var resultNumber = 0;
                                do
                                {
                                    while (await reader.ReadAsync())
                                    {
                                        _dataReaderAction?.Invoke(reader, resultNumber);
                                    }

                                    resultNumber++;
                                } while (await reader.NextResultAsync());

                                break;
                            }
                            case ExecType.DataSet:
                            {
                                using var adapter = new SqlDataAdapter(_command);
                                adapter.Fill(result.FillDataSetResult);

                                break;
                            }
                            case ExecType.XmlReader:
                            {
                                using var reader = await _command.ExecuteXmlReaderAsync();
                                if (await reader.ReadAsync())
                                {
                                    result.XmlResult = await reader.ReadOuterXmlAsync();
                                }

                                break;
                            }
                        }

                        Status = ExecStatus.Success;
                    }
                }
                catch (Exception ex)
                {
                    HandleException(ex);
                }
            }

            return result;
        }

        private void HandleException(Exception ex)
        {
            Error = ex;
            Status = ExecStatus.Failed;
            var wasHandled = _onFailAction?.Invoke(this) ?? false;
            if (!wasHandled)
            {
                throw ex;
            }
        }

        #endregion

        #region [ Nested Types ]

        private class ExecResult
        {
            public object ScalarResult { get; set; }
            public DataSet FillDataSetResult { get; set; }
            public int NonQueryResult { get; set; }
            public string XmlResult { get; set; }
        }

        private enum ExecType
        {
            NonQuery,
            Reader,
            Scalar,
            DataSet,
            XmlReader
        }

        #endregion
    }
}