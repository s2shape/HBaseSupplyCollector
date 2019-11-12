# HBaseSupplyCollector
A supply collector designed to connect to HBase

## Building
Run `dotnet build`

## Testing
Run `./run-tests.sh`

## Known issues
- no support for metrics
- random sampling doesn't work - think about RandomRowFilter()
- modified/debug version of rest library is used. See https://github.com/s2shape/hbase-sdk-for-net
- load test doesn't work - takes too much time to load 1M samples using rest api
