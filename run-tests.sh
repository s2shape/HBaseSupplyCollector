#!/bin/bash
docker run --name hbase -d dajobe/hbase
sleep 20

export HBASE_HOST=localhost
export HBASE_PORT=9090

dotnet build
dotnet test

docker stop hbase
docker rm hbase
