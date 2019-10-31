using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using HBaseSupplyCollector;
using Microsoft.HBase.Client;
using Microsoft.HBase.Client.LoadBalancing;
using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using S2.BlackSwan.SupplyCollector.Models;
using SupplyCollectorDataLoader;

namespace HBaseSupplyCollectorLoader
{
    public class HBaseSupplyCollectorLoader : SupplyCollectorDataLoaderBase
    {
        private const string PREFIX = "hbase://";
        private RequestOptions _globalRequestOptions;
        private RequestOptions _globalXmlRequestOptions;

        private HBaseClient Connect(string connectString)
        {
            if (!connectString.StartsWith(PREFIX))
                throw new ArgumentException("Invalid connection string!");

            var parts = connectString.Substring(PREFIX.Length).Split(":");

            var host = parts[0];
            var port = Int32.Parse(parts[1]);

            Console.WriteLine($"Connecting to http://{host}:{port}");

            var credentials = new ClusterCredentials(new Uri($"http://{host}:{port}"), "anonymous", "");
            _globalRequestOptions = new RequestOptions() {
                Port = port,
                RetryPolicy = RetryPolicy.NoRetry,
                KeepAlive = true,
                TimeoutMillis = 30000,
                ReceiveBufferSize = 1024 * 1024 * 1,
                SerializationBufferSize = 1024 * 1024 * 1,
                UseNagle = false,
                AlternativeEndpoint = "/", //Constants.RestEndpointBase,
                AlternativeHost = null
            };

            return new HBaseClient(credentials);
        }

        public override void InitializeDatabase(DataContainer dataContainer) {
            // nothing to do
        }

        public override void LoadSamples(DataEntity[] dataEntities, long count) {
            using (var connect = Connect(dataEntities[0].Container.ConnectionString)) {
                var tableName = dataEntities[0].Collection.Name;

                var schema = new TableSchema() {
                    name = tableName,
                    inMemory = false
                };

                schema.columns.AddRange(
                    dataEntities.Select(x => new ColumnSchema() {
                        name = x.Name,
                        maxVersions = 1
                    }));

                connect.CreateTableAsync(schema, _globalRequestOptions).Wait();

                var columnNames = dataEntities.Select(x => x.Name.ToUtf8Bytes()).ToList();

                var r = new Random();
                long rows = 0;
                while (rows < count) {
                    if (rows % 1000 == 0) {
                        Console.Write(".");
                    }

                    var cellSet = new CellSet();
                    for (int i = 0; i < 100; i++) {
                        var row = new CellSet.Row { key = Guid.NewGuid().ToString().ToUtf8Bytes() };

                        foreach (var dataEntity in dataEntities) {
                            switch (dataEntity.DataType) {
                                case DataType.String:
                                    row.values.Add(new Cell() {
                                        column = dataEntity.Name.ToUtf8Bytes(),
                                        data = new Guid().ToString().ToUtf8Bytes()
                                    });
                                    break;
                                case DataType.Int:
                                    row.values.Add(new Cell()
                                    {
                                        column = dataEntity.Name.ToUtf8Bytes(),
                                        data = r.Next().ToString().ToUtf8Bytes()
                                    });
                                    break;
                                case DataType.Double:
                                    row.values.Add(new Cell()
                                    {
                                        column = dataEntity.Name.ToUtf8Bytes(),
                                        data = r.NextDouble().ToString().Replace(",", ".").ToUtf8Bytes()
                                    });
                                    break;
                                case DataType.Boolean:
                                    row.values.Add(new Cell()
                                    {
                                        column = dataEntity.Name.ToUtf8Bytes(),
                                        data = r.Next(100) > 50 ? "true".ToUtf8Bytes() : "false".ToUtf8Bytes()
                                    });
                                    break;
                                case DataType.DateTime:
                                    var val = DateTimeOffset
                                        .FromUnixTimeMilliseconds(
                                            DateTimeOffset.Now.ToUnixTimeMilliseconds() + r.Next()).DateTime;

                                    row.values.Add(new Cell()
                                    {
                                        column = dataEntity.Name.ToUtf8Bytes(),
                                        data = val.ToString("s").ToUtf8Bytes()
                                    });

                                    break;
                                default:
                                    row.values.Add(new Cell()
                                    {
                                        column = dataEntity.Name.ToUtf8Bytes(),
                                        data = r.Next().ToString().ToUtf8Bytes()
                                    });
                                    break;
                            }
                        }

                        cellSet.rows.Add(row);
                    }

                    connect.StoreCellsAsync(tableName, cellSet, _globalXmlRequestOptions).Wait();
                    rows += 100;
                }

                Console.WriteLine();
            }
        }

        private void LoadTable(HBaseClient connect, string tableName, string filePath) {
            using (var reader = new StreamReader(filePath)) {
                var header = reader.ReadLine();
                var columnsNames = header.Split(",");

                var schema = new TableSchema()
                {
                    name = tableName,
                    inMemory = false
                };

                schema.columns.AddRange(
                    columnsNames.Select(x => new ColumnSchema()
                    {
                        name = x,
                        maxVersions = 1
                    }));

                connect.CreateTableAsync(schema, _globalRequestOptions).Wait();

                while (!reader.EndOfStream) {
                    var line = reader.ReadLine();
                    if(String.IsNullOrEmpty(line))
                        continue;

                    var cellSet = new CellSet();
                    var cells = line.Split(",");

                    var row = new CellSet.Row { key = Guid.NewGuid().ToString().ToUtf8Bytes() };
                    for (int i = 0; i < cells.Length && i < columnsNames.Length; i++) {
                        var cell = new Cell()
                        {
                            column = (columnsNames[i] + ":columnname").ToUtf8Bytes(),
                            data = cells[i].ToUtf8Bytes()
                        };

                        row.values.Add(cell);
                    }
                    cellSet.rows.Add(row);

                    try
                    {
                        connect.StoreCellsAsync(tableName, cellSet, _globalRequestOptions).Wait();
                    }
                    catch (Exception ex)
                    {
                        if (ex.InnerException != null && ex.InnerException is WebException)
                        {
                            var resp = (ex.InnerException as WebException).Response as HttpWebResponse;
                            string msg;
                            using (var sr = new StreamReader(resp.GetResponseStream()))
                            {
                                msg = sr.ReadToEnd();
                            }
                            throw new Exception(msg, ex);
                        }
                    }
                }
            }
        }

        public override void LoadUnitTestData(DataContainer dataContainer) {
            using (var connect = Connect(dataContainer.ConnectionString)) {
                LoadTable(connect, "emails", "./tests/EMAILS.CSV");
            }
        }
    }
}
