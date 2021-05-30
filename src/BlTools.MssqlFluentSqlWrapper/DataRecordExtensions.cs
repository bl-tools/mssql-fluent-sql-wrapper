using System;
using System.Data;


namespace BlTools.MssqlFluentSqlWrapper
{
    public static class DataRecordExtensions
    {
        public static object GetObject(this IDataRecord reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? null : reader.GetValue(ordinal);
        }

        public static string GetString(this IDataRecord reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
        }

        public static byte[] GetBytes(this IDataRecord record, string name)
        {
            var ordinal = record.GetOrdinal(name);

            byte[] result = null;

            if (!record.IsDBNull(ordinal))
            {
                var arraySize = record.GetBytes(ordinal, 0, null, 0, 0);
                result = new byte[arraySize];
                record.GetBytes(ordinal, 0, result, 0, result.Length);
            }

            return result;
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

        public static TimeSpan GetTime(this IDataRecord reader, string name)
        {
            return (TimeSpan)reader.GetValue(reader.GetOrdinal(name));
        }

        public static TimeSpan? GetTimeNull(this IDataRecord reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (TimeSpan?)null : (TimeSpan)reader.GetValue(ordinal);
        }

        public static DateTime GetDateTime(this IDataRecord reader, string name)
        {
            return reader.GetDateTime(reader.GetOrdinal(name));
        }

        public static DateTime? GetDateTimeNull(this IDataRecord reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (DateTime?) null : reader.GetDateTime(ordinal);
        }

        public static DateTime GetDateTimeUtc(this IDataRecord reader, string name)
        {
            return DateTime.SpecifyKind(reader.GetDateTime(name), DateTimeKind.Utc);
        }

        public static DateTime? GetDateTimeUtcNull(this IDataRecord reader, string name)
        {
            var result = reader.GetDateTimeNull(name);
            return !result.HasValue ? (DateTime?)null : DateTime.SpecifyKind(result.Value, DateTimeKind.Utc);
        }

        public static decimal GetDecimal(this IDataRecord reader, string name)
        {
            return reader.GetDecimal(reader.GetOrdinal(name));
        }

        public static decimal? GetDecimalNull(this IDataRecord reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (decimal?)null : reader.GetDecimal(ordinal);
        }

        public static float GetFloat(this IDataRecord reader, string name)
        {
            return reader.GetFloat(reader.GetOrdinal(name));
        }

        public static float? GetFloatNull(this IDataRecord reader, string name)
        {
            var ordinal = reader.GetOrdinal(name);
            return reader.IsDBNull(ordinal) ? (float?)null : reader.GetFloat(ordinal);
        }

        // ReSharper disable once InconsistentNaming
        public static bool IsDBNull(this IDataRecord reader, string name)
        {
            return reader.IsDBNull(reader.GetOrdinal(name));
        }
    }
}