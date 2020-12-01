using NUnit.Framework;

namespace BlTools.MssqlFluentSqlWrapper.Tests
{
    [TestFixture]
    public class DebugEntryPointTests
    {
        private const string _connectionString = "";//TODO: place connection string here.

        [Test]
        public void RunTestCommandWithDataSetResult()
        {
            const string value = "123";
            var res = new FluentSqlCommand(_connectionString)
                .Query($"select '{value}' as my")
                .ExecFillDataSet();

            Assert.AreEqual(value, res.Tables[0].Rows[0][0]);
        }
    }
}
