#!/usr/bin/env bash

USER_COMMAND=$1
echo "Running command $USER_COMMAND"
shift

source "$(dirname "$0")"/kafka-common.sh

case $USER_COMMAND in
    "setup_kafka_dist") setup_kafka_dist;;
    "setup_confluent_dist") setup_confluent_dist;;
    "start_kafka_cluster") start_kafka_cluster;;
    "stop_kafka_cluster") stop_kafka_cluster;;
    "create_kafka_topic") create_kafka_topic;;
    "start_confluent_schema_registry") start_confluent_schema_registry;;
    "stop_confluent_schema_registry") stop_confluent_schema_registry;;
    *) echo "$USER_COMMAND not found";;
esac
