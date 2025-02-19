﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using Microsoft.HBase.Client;
using Microsoft.HBase.Client.LoadBalancing;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
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
        private RequestOptions _globalRequestOptions;

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
            _globalRequestOptions = new RequestOptions()
            {
                Port = port,
                RetryPolicy = RetryPolicy.NoRetry,
                KeepAlive = true,
                TimeoutMillis = 30000,
                ReceiveBufferSize = 1024 * 1024 * 1,
                SerializationBufferSize = 1024 * 1024 * 1,
                UseNagle = false,
                AlternativeEndpoint = "/",
                AlternativeHost = null
            };

            return new HBaseClient(credentials);
        }

        public override List<string> CollectSample(DataEntity dataEntity, int sampleSize) {
            return AsyncHelpers.RunSync(() => CollectSampleAsync(dataEntity, sampleSize));
        }

        public async Task<List<string>> CollectSampleAsync(DataEntity dataEntity, int sampleSize) {
            var results = new List<string>();

            using (var conn = Connect(dataEntity.Container.ConnectionString)) {
                var scan = await conn.CreateScannerAsync(dataEntity.Collection.Name,
                    new Scanner() {
                        batch = 10,
                        columns = { dataEntity.Name.ToUtf8Bytes() }
                        //filter = "{\"type\": \"ColumnPrefixFilter\", \"value\": \""  + Convert.ToBase64String((dataEntity.Name).ToUtf8Bytes()) + "\" }" 
                    }, _globalRequestOptions).ConfigureAwait(false);

                try {
                    while (true) {
                        var rows = await conn.ScannerGetNextAsync(scan, _globalRequestOptions).ConfigureAwait(false);

                        if (rows == null || rows.rows.Count == 0) {
                            break;
                        }

                        foreach (var row in rows.rows) {
                            foreach (var rowColumn in row.values) {
                                results.Add(rowColumn.data.ToUtf8String());

                                if (results.Count >= sampleSize)
                                    break;
                            }

                            if (results.Count >= sampleSize)
                                break;
                        }

                        if (results.Count >= sampleSize)
                            break;
                    }
                }
                finally {
                    await conn.DeleteScannerAsync(dataEntity.Collection.Name, scan, _globalRequestOptions).ConfigureAwait(false);
                }
            }

            return results;
        }

        private long getRowCount(HBaseClient conn, string tableName) {
            return 0;
        }

        public override List<DataCollectionMetrics> GetDataCollectionMetrics(DataContainer container)
        {
            return AsyncHelpers.RunSync(() => GetDataCollectionMetricsAsync(container));
        }

        public async Task<List<DataCollectionMetrics>> GetDataCollectionMetricsAsync(DataContainer container) {
            var metrics = new List<DataCollectionMetrics>();

            using (var conn = Connect(container.ConnectionString))
            {
                var tables = await conn.ListTablesAsync(_globalRequestOptions).ConfigureAwait(false);

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
            return AsyncHelpers.RunSync(() => GetSchemaAsync(container));
        }

        public async Task<(List<DataCollection>, List<DataEntity>)> GetSchemaAsync(DataContainer container) {
            var collections = new List<DataCollection>();
            var entities = new List<DataEntity>();

            using (var conn = Connect(container.ConnectionString)) {
                var tables = await conn.ListTablesAsync(_globalRequestOptions).ConfigureAwait(false);

                foreach (var table in tables.name) {
                    var collection = new DataCollection(container, table);
                    collections.Add(collection);

                    var schema = await conn.GetTableSchemaAsync(table, _globalRequestOptions).ConfigureAwait(false);
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
            return AsyncHelpers.RunSync(() => TestConnectionAsync(container));
        }

        public async Task<bool> TestConnectionAsync(DataContainer container) {
            try {
                using (var conn = Connect(container.ConnectionString)) {
                    await conn.ListTablesAsync(_globalRequestOptions).ConfigureAwait(false);
                }

                return true;
            }
            catch (Exception) {
                return false;
            }
        }
    }
}
