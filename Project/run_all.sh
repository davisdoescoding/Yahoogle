#!/bin/bash

set -e  # exit on error
set -u  # treat unset vars as an err

# func to check if a port is open and accept connections
wait_for_port() {
  local HOST=$1
  local PORT=$2
  local TIMEOUT=30
  echo "Waiting for $HOST:$PORT to be ready..."
  for i in $(seq 1 $TIMEOUT); do
    nc -z $HOST $PORT && echo "$HOST:$PORT is ready." && return 0
    sleep 1
  done
  echo "Error: $HOST:$PORT did not become ready in time."
  exit 1
}

echo "Compiling Java source files..."
javac -sourcepath src -d bin $(find src -name '*.java')
echo "Compilation complete."
sleep 2

# start KVS coordinators
echo "Starting KVS Coordinator on port 8000..."
nohup java -cp bin cis5550.kvs.Coordinator 8000 > /dev/null 2>&1 &
wait_for_port localhost 8000

# start KVS workers
for i in $(seq 1 10); do
  PORT=$((8000 + i))
  WORKER_NAME="worker$i"
  echo "Starting KVS Worker $i on port $PORT..."
  nohup java -cp bin cis5550.kvs.Worker $PORT $WORKER_NAME localhost:8000 > /dev/null 2>&1 &
  wait_for_port localhost $PORT
done

# start flame coordinator
echo "Starting Flame Coordinator on port 9000..."
nohup java -cp bin cis5550.flame.Coordinator 9000 localhost:8000 > /dev/null 2>&1 &
wait_for_port localhost 9000

# start flame workers
for i in $(seq 1 10); do
  PORT=$((9000 + i))
  echo "Starting Flame Worker $i on port $PORT..."
  nohup java -cp bin cis5550.flame.Worker $PORT localhost:9000 > /dev/null 2>&1 &
  wait_for_port localhost $PORT
done

echo "All processes started successfully!"
