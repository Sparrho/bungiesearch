#!/usr/bin/env bash
# pass in --cluster as an argument to start a cluster instead of a single node
set -e
trap 'jobs -p | xargs kill -9' EXIT

CLUSTER_URL=http://127.0.0.1:9200
ES_PATH=elasticsearch

if [ ${TRAVIS} ]; then
  echo 'TRAVIS IS SET'
  ES_PATH=./elasticsearch-1.7.3/bin/elasticsearch
fi

function has_command() {
  type $1 &> /dev/null
}

function is_responding() {
  curl --output /dev/null --fail --silent $1
}

function wait_for_cluster() {
  echo 'Waiting on elasticsearch to be ready on port 9200'
  until is_responding "$CLUSTER_URL/_cluster/health?wait_for_nodes=$1&wait_for_status=green"; do
    printf '.'
    sleep 1
  done
  echo
}

if ! is_responding $CLUSTER_URL; then
  if ! has_command elasticsearch; then
    echo 'No elasticsearch command found and no server running'
    echo 'Elasticsearch cluster must be running on port 9200'
    exit 1
  else
    if [ "$1" != "--cluster" ]; then
      echo 'Starting single elasticsearch node'
      $ES_PATH &> /dev/null &
      wait_for_cluster 1
    else
      echo 'Starting elasticsearch cluster with 2 nodes'
      $ES_PATH \
        -D es.cluster.name="mycluster" \
        -D es.node.name="mycluster-node2" \
        -D es.node.master=true \
        -D es.node.data=false \
        -D es.network.host=127.0.0.1 \
        -D es.foreground=yes \
        -D es.discovery.zen.ping.multicast.enabled=false \
        -D es.discovery.zen.ping.unicast.hosts=127.0.0.1:9300,127.0.0.1:9301,127.0.0.1:9302 &> /dev/null &

      $ES_PATH \
        -D es.cluster.name="mycluster" \
        -D es.node.name="mycluster-node2" \
        -D es.node.master=false \
        -D es.node.data=true \
        -D es.network.host=127.0.0.1 \
        -D es.foreground=yes \
        -D es.discovery.zen.ping.multicast.enabled=false \
        -D es.discovery.zen.ping.unicast.hosts=127.0.0.1:9300,127.0.0.1:9301,127.0.0.1:9302 &> /dev/null &

      wait_for_cluster 2
    fi
  fi
fi

python -B tests/manage.py test

# only collect coverage in travis ci
if [ ${TRAVIS} ]; then
  echo 'Starting to collect coverage'
  coverage run --source=tests tests/manage.py test
fi

