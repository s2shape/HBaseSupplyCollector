#!/bin/bash
docker run --name hbase -d dajobe/hbase
sleep 20

export HBASE_HOST=localhost
export HBASE_PORT=8080

dotnet restore -s https://www.myget.org/F/s2/ -s https://api.nuget.org/v3/index.json
dotnet build
dotnet publish

pushd HBaseSupplyCollectorLoader/bin/Debug/netcoreapp2.2/publish
dotnet SupplyCollectorDataLoader.dll -xunit HBaseSupplyCollector hbase://$HBASE_HOST:$HBASE_PORT
popd

dotnet test

docker stop hbase
docker rm hbase
