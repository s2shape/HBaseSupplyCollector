using System;
using S2.BlackSwan.SupplyCollector.Models;
using Xunit;

namespace HBaseSupplyCollectorTests
{
    public class HBaseSupplyCollectorTests : IClassFixture<LaunchSettingsFixture>
    {
        private readonly LaunchSettingsFixture _fixture;
        private readonly HBaseSupplyCollector.HBaseSupplyCollector _instance;
        private readonly DataContainer _container;
        private readonly DataEntity _emailToAddress;

        public HBaseSupplyCollectorTests(LaunchSettingsFixture fixture) {
            _fixture = fixture;
            _instance = new HBaseSupplyCollector.HBaseSupplyCollector();
            _container = new DataContainer()
            {
                ConnectionString = _instance.BuildConnectionString(
                    Environment.GetEnvironmentVariable("HBASE_HOST"),
                    Int32.Parse(Environment.GetEnvironmentVariable("HBASE_PORT"))
                )
            };
            var emailCollection = new DataCollection(_container, "emails");
            _emailToAddress = new DataEntity("TO_ADDRS_EMAILS", DataType.String, "string", _container, emailCollection);
        }

        [Fact]
        public void DataStoreTypesTest()
        {
            var result = _instance.DataStoreTypes();
            Assert.Contains("HBase", result);
        }

        [Fact]
        public void ConnectionTest()
        {
            Assert.True(_instance.TestConnection(_container));
        }

        [Fact]
        public void GetSchemaTest()
        {
            var (tables, elements) = _instance.GetSchema(_container);

            Assert.Equal(1, tables.Count);
            Assert.Equal(39, elements.Count);
            foreach (DataEntity element in elements)
            {
                Assert.NotEqual(string.Empty, element.DbDataType);
            }
        }

        [Fact]
        public void CollectSampleTest()
        {
            var samples = _instance.CollectSample(_emailToAddress, 4);
            Assert.Equal(4, samples.Count);
            samples = _instance.CollectSample(_emailToAddress, 200);
            Assert.Equal(200, samples.Count);
            Assert.Contains("qa25@example.com", samples);
        }

    }
}
