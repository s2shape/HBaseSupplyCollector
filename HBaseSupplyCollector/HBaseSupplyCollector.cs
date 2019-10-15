using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using Microsoft.HBase.Client;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using S2.BlackSwan.SupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;

namespace HBaseSupplyCollector
{
    public class HBaseSupplyCollector : SupplyCollectorBase {
        public override List<string> DataStoreTypes() {
            return (new[] {"HBase"}).ToList();
        }

        private const string PREFIX = "hbase://";

        public string BuildConnectionString(string host, int port)
        {
            return $"{PREFIX}{host}:{port}";
        }

        private HBaseClient Connect(string connectString)
        {
            if (!connectString.StartsWith(PREFIX))
                throw new ArgumentException("Invalid connection string!");

            var parts = connectString.Substring(PREFIX.Length).Split(":");

            var host = parts[0];
            var port = Int32.Parse(parts[1]);

            var credentials = new ClusterCredentials(new Uri($"http://{host}:{port}"), "anonymous", "");

            return new HBaseClient(credentials);
        }

        public override List<string> CollectSample(DataEntity dataEntity, int sampleSize) {
            var results = new List<string>();

            using (var conn = Connect(dataEntity.Container.ConnectionString)) {
                var scan = conn.CreateScannerAsync(dataEntity.Collection.Name,
                    new Scanner() {filter = dataEntity.Name}, new RequestOptions()).Result;

                var rows = conn.ScannerGetNextAsync(scan, new RequestOptions()).Result;

                foreach (var row in rows.rows) {
                    foreach (var rowColumn in row.values) {
                        results.Add(rowColumn.data.ToUtf8String());
                    }
                }
            }

            return results;
        }

        private long getRowCount(HBaseClient conn, string tableName) {
            return 0;
        }

        public override List<DataCollectionMetrics> GetDataCollectionMetrics(DataContainer container) {
            var metrics = new List<DataCollectionMetrics>();

            using (var conn = Connect(container.ConnectionString))
            {
                var tables = conn.ListTablesAsync().Result;

                foreach (var table in tables.name) {
                    metrics.Add(new DataCollectionMetrics() {
                        Name = table,
                        RowCount = getRowCount(conn, table)
                    });
                }
            }

            return metrics;
        }

        public override (List<DataCollection>, List<DataEntity>) GetSchema(DataContainer container) {
            var collections = new List<DataCollection>();
            var entities = new List<DataEntity>();

            using (var conn = Connect(container.ConnectionString)) {
                var tables = conn.ListTablesAsync().Result;

                foreach (var table in tables.name) {
                    var collection = new DataCollection(container, table);
                    collections.Add(collection);

                    var schema = conn.GetTableSchemaAsync(table).Result;
                    foreach (var column in schema.columns) {
                        entities.Add(new DataEntity(
                            column.name,
                            DataType.String, "string", container, collection
                            ));
                    }
                }
            }

            return (collections, entities);
        }

        public override bool TestConnection(DataContainer container) {
            try {
                using (var conn = Connect(container.ConnectionString)) {
                    conn.ListTablesAsync().Wait();
                }

                return true;
            }
            catch (Exception) {
                return false;
            }
        }
    }
}
