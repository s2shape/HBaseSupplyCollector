using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using Apache.Hadoop.Hbase.Thrift;
using S2.BlackSwan.SupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;
using Thrift.Protocol;
using Thrift.Transport;
using Thrift.Transport.Client;

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

        private Hbase.Client Connect(string connectString)
        {
            if (!connectString.StartsWith(PREFIX))
                throw new ArgumentException("Invalid connection string!");

            var parts = connectString.Substring(PREFIX.Length).Split(":");

            var host = parts[0];
            var port = Int32.Parse(parts[1]);

            var client = new TcpClient(host, port);
            var socket = new TSocketTransport(client);
            var transport = new TBufferedTransport(socket);
            var protocol = new TBinaryProtocol(transport);
            return new Hbase.Client(protocol);
        }

        public override List<string> CollectSample(DataEntity dataEntity, int sampleSize) {
            var results = new List<string>();

            using (var conn = Connect(dataEntity.Container.ConnectionString)) {
                var scanId = conn.scannerOpenAsync(dataEntity.Collection.Name.ToUtf8Bytes(), new byte[] { },
                    new List<byte[]>() {dataEntity.Name.ToUtf8Bytes()}, new Dictionary<byte[], byte[]>()).Result;

                var rows = conn.scannerGetListAsync(scanId, sampleSize).Result;

                foreach (var row in rows) {
                    foreach (var rowColumn in row.Columns) {
                        results.Add(rowColumn.Value.Value.ToUtf8String());
                    }
                }
            }

            return results;
        }

        private long getRowCount(Hbase.Client conn, byte[] tableName) {
            return 0;
        }

        public override List<DataCollectionMetrics> GetDataCollectionMetrics(DataContainer container) {
            var metrics = new List<DataCollectionMetrics>();

            using (var conn = Connect(container.ConnectionString))
            {
                var tables = conn.getTableNamesAsync().Result;

                foreach (var table in tables) {
                    metrics.Add(new DataCollectionMetrics() {
                        Name = table.ToUtf8String(),
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
                var tables = conn.getTableNamesAsync().Result;

                foreach (var table in tables) {
                    var collection = new DataCollection(container, table.ToUtf8String());
                    collections.Add(collection);

                    var columns = conn.getColumnDescriptorsAsync(table).Result;
                    foreach (var column in columns) {
                        entities.Add(new DataEntity(
                            column.Value.Name.ToUtf8String(),
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
                    conn.getTableNamesAsync().Wait();
                }

                return true;
            }
            catch (Exception) {
                return false;
            }
        }
    }
}
