using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;

namespace BlTools.MssqlFluentSqlWrapper
{
    public static class DataRecordExtensions
    {
        public static byte[] GetBytes(this SqlDataReader reader, string name)
        {
            return reader.GetSqlBinary(reader.GetOrdinal(name)).Value;
        }

        public static byte[] GetBytesNull(this SqlDataReader reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? null : reader.GetSqlBinary(ordinal).Value;
        }

        public static long GetInt64(this IDataRecord reader, string name)
        {
            return reader.GetInt64(reader.GetOrdinal(name));
        }

        public static long? GetInt64Null(this IDataRecord reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (long?)null : reader.GetInt64(ordinal);
        }

        public static int GetInt32(this IDataRecord reader, string name)
        {
            return reader.GetInt32(reader.GetOrdinal(name));
        }

        public static int? GetInt32Null(this IDataRecord reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (int?) null : reader.GetInt32(ordinal);
        }

        public static short GetInt16(this IDataRecord reader, string name)
        {
            return reader.GetInt16(reader.GetOrdinal(name));
        }

        public static short? GetInt16Null(this IDataRecord reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (short?)null : reader.GetInt16(ordinal);
        }

        public static byte GetByte(this IDataRecord reader, string name)
        {
            return reader.GetByte(reader.GetOrdinal(name));
        }

        public static byte? GetByteNull(this IDataRecord reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (byte?)null : reader.GetByte(ordinal);
        }

        public static double GetDouble(this IDataRecord reader, string name)
        {
            return reader.GetDouble(reader.GetOrdinal(name));
        }

        public static double? GetDoubleNull(this IDataRecord reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (double?) null : reader.GetDouble(ordinal);
        }

        public static bool GetBoolean(this IDataRecord reader, string name)
        {
            return reader.GetBoolean(reader.GetOrdinal(name));
        }

        public static bool? GetBooleanNull(this IDataRecord reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (bool?)null : reader.GetBoolean(ordinal);
        }

        public static Guid GetGuid(this IDataRecord reader, string name)
        {
            return reader.GetGuid(reader.GetOrdinal(name));
        }

        public static Guid? GetGuidNull(this IDataRecord reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (Guid?)null : reader.GetGuid(ordinal);
        }

        public static string GetString(this IDataRecord reader, string name)
        {
            int ordinal;
            
            try
            {
                ordinal = reader.GetOrdinal(name);
            }
            catch (IndexOutOfRangeException ex)
            {
                throw new IndexOutOfRangeException(string.Format("The \"{0}\" is not a valid column name.", name), ex);
            }
            
            return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
        }
        
        public static char? GetChar(this IDataRecord reader, string name)
        {
            // IDataRecord does not support GetChar
            var ordinal = reader.GetOrdinal(name);
            if (reader.IsDBNull(ordinal))return  null ;
            var data = reader.GetString(ordinal);
            if (data.Length == 0)
                return null;
            return data[0];
        }

        /// <summary>
        /// Gets Value of TSQL TIME data type.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public static DateTime GetTime(this IDataRecord reader, string name)
        {
            return new DateTime() + (TimeSpan)reader.GetValue(reader.GetOrdinal(name));
        }

        public static TimeSpan GetTimeSpan(this IDataRecord reader, string name)
        {
            return (TimeSpan)reader.GetValue(reader.GetOrdinal(name));
        }

        public static DateTime GetDateTime(this IDataRecord reader, string name)
        {
            return reader.GetDateTime(reader.GetOrdinal(name));
        }

        public static DateTime GetDateTimeUtc(this IDataRecord reader, string name)
        {
            return DateTime.SpecifyKind(reader.GetDateTime(name), DateTimeKind.Utc);
        }

        public static DateTime? GetDateTimeNull(this IDataRecord reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (DateTime?) null : reader.GetDateTime(ordinal);
        }

        public static DateTime? GetDateTimeUtcNull(this IDataRecord reader, string name)
        {
            var result = reader.GetDateTimeNull(name);
            return !result.HasValue ? (DateTime?)null : DateTime.SpecifyKind(result.Value, DateTimeKind.Utc);
        }

        public static decimal? GetDecimalNull(this IDataRecord reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (decimal?)null : reader.GetDecimal(ordinal);
        }

        public static decimal GetDecimal(this IDataRecord reader, string name)
        {
            return reader.GetDecimal(reader.GetOrdinal(name));
        }

        public static bool IsDBNull(this IDataRecord reader, string name)
        {
            return reader.IsDBNull(reader.GetOrdinal(name));
        }

        public static object GetValue(this IDataRecord reader, string name)
        {
            return reader.GetValue(reader.GetOrdinal(name));
        }

        public static float? GetFloatNull(this IDataRecord reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (float?)null : reader.GetFloat(ordinal);
        }

        public static float GetFloat(this IDataRecord reader, string name)
        {
            return reader.GetFloat(reader.GetOrdinal(name));
        }

        public static SqlXml GetSqlXml(this SqlDataReader reader, string name)
        {
            return reader.GetSqlXml(reader.GetOrdinal(name));
        }

        public static T GetFieldSafe<T>(this IDataRecord reader, string fieldName, T defaultValue = default(T))
        {
            var ordinal = reader.GetOrdinal(fieldName);
            return !reader.IsDBNull(ordinal) ? (T) reader.GetValue(ordinal) : defaultValue;
        }

        public static T GetNullableField<T> (this DataRow row, string fieldName, T defaultValue = default (T))
        {
            if (row.IsNull(fieldName))
                return defaultValue;
            return (T) row[fieldName];
        }

        public static IEnumerable<object> GetRow(this IDataRecord reader)
        {
            var row = new List<object>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                row.Add(reader[i]);
            }
            return row;
        }

        public static IEnumerable<string> GetAllColumns(this IDataRecord reader)
        {
            var res = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                res.Add(reader.GetName(i));
            }
            return res;
        }
    }
}