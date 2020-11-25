using System;
using NUnit.Framework;

namespace BlTools.MssqlFluentSqlWrapper.Tests
{
    [TestFixture]
    public class DebugEntryPointTests
    {
        private const string _cs = "";//TODO: place connection string here.

        [Test]
        public void Do()
        {
            const string value = "123";
            var res = new FluentSqlCommand(_cs)
                .Query($"select '{value}' as my")
                .ExecFillDataSet();

            Assert.AreEqual(value, res.Tables[0].Rows[0][0]);
        }
    }
}
