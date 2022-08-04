mssql-fluent-sql-wrapper
===

Library provides the ability to call Stored Procedures, Functions, Raw SQL in MsSql server. It is a wrapper over System.Data.SqlClient data provider. Library created for the ability to use data provider in fluent notation mode.

```csharp
//call stored procedure with parameters
            var command = new FluentSqlCommand(ConnectionString)
                .StoredProc("stored_procedure_name")
                .AddParam("param1", "value1")
                .AddParam("param2", "value2")
                .AddParam("param3", "value3");

            var result = command.ExecReadItemList(record =>
            {
                var result = new OutgoingDataModel
                {
                    field1 = record.GetString("field1"),
                    field2 = record.GetString("field2")
                };
                return result;
            });
```