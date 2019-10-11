using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using Apache.Hadoop.Hbase.Thrift;
using HBaseSupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;
using SupplyCollectorDataLoader;
using Thrift.Protocol;
using Thrift.Transport;
using Thrift.Transport.Client;

namespace HBaseSupplyCollectorLoader
{
    public class HBaseSupplyCollectorLoader : SupplyCollectorDataLoaderBase
    {
        private const string PREFIX = "hbase://";

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

        public override void InitializeDatabase(DataContainer dataContainer) {
            // nothing to do
        }

        public override void LoadSamples(DataEntity[] dataEntities, long count) {
            using (var connect = Connect(dataEntities[0].Container.ConnectionString)) {
                var tableName = dataEntities[0].Collection.Name.ToUtf8Bytes();

                var columns = dataEntities.Select(x => new ColumnDescriptor() {
                    Name = x.Name.ToUtf8Bytes(),
                    InMemory = false,
                    MaxVersions = 1
                }).ToList();

                connect.createTableAsync(tableName, columns).Wait();

                var columnNames = dataEntities.Select(x => x.Name.ToUtf8Bytes()).ToList();

                var r = new Random();
                long rows = 0;
                while (rows < count) {
                    if (rows % 1000 == 0) {
                        Console.Write(".");
                    }

                    var values = new List<byte[]>();
                    foreach (var dataEntity in dataEntities) {
                        switch (dataEntity.DataType)
                        {
                            case DataType.String:
                                values.Add(new Guid().ToString().ToUtf8Bytes());
                                break;
                            case DataType.Int:
                                values.Add(r.Next().ToString().ToUtf8Bytes());
                                break;
                            case DataType.Double:
                                values.Add(r.NextDouble().ToString().Replace(",", ".").ToUtf8Bytes());
                                break;
                            case DataType.Boolean:
                                values.Add(r.Next(100) > 50 ? "true".ToUtf8Bytes() : "false".ToUtf8Bytes());
                                break;
                            case DataType.DateTime:
                                var val = DateTimeOffset
                                    .FromUnixTimeMilliseconds(
                                        DateTimeOffset.Now.ToUnixTimeMilliseconds() + r.Next()).DateTime;
                                
                                values.Add(val.ToString("s").ToUtf8Bytes());
                                
                                break;
                            default:
                                values.Add(r.Next().ToString().ToUtf8Bytes());
                                break;
                        }
                    }

                    connect.appendAsync(new TAppend() {
                        Row = Guid.NewGuid().ToString().ToUtf8Bytes(),
                        Columns = columnNames,
                        Table = tableName,
                        Values = values
                    }).Wait();

                    rows++;
                }

                Console.WriteLine();
            }
        }

        private void LoadTable(Hbase.Client connect, string tableName, string filePath) {
            using (var reader = new StreamReader(filePath)) {
                var header = reader.ReadLine();
                var columnsNames = header.Split(",");

                var columns = columnsNames.Select(x => new ColumnDescriptor()
                {
                    Name = x.ToUtf8Bytes(),
                    InMemory = false,
                    MaxVersions = 1
                }).ToList();

                connect.createTableAsync(tableName.ToUtf8Bytes(), columns).Wait();

                while (!reader.EndOfStream) {
                    var line = reader.ReadLine();
                    if(String.IsNullOrEmpty(line))
                        continue;

                    var cells = line.Split(",");

                    connect.appendAsync(new TAppend()
                    {
                        Row = Guid.NewGuid().ToString().ToUtf8Bytes(),
                        Columns = columnsNames.Select(x => x.ToUtf8Bytes()).ToList(),
                        Table = tableName.ToUtf8Bytes(),
                        Values = cells.Select(x => x.ToUtf8Bytes()).ToList()
                    }).Wait();
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
