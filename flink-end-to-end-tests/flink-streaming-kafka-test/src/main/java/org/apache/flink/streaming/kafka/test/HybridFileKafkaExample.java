/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.kafka.test;

import java.io.File;
import java.util.Arrays;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka;
import org.apache.flink.streaming.kafka.test.base.CustomWatermarkExtractor;
import org.apache.flink.streaming.kafka.test.base.KafkaEvent;
import org.apache.flink.streaming.kafka.test.base.KafkaEventSchema;
import org.apache.flink.streaming.kafka.test.base.KafkaExampleUtil;
import org.apache.flink.streaming.kafka.test.base.RollingAdditionMapper;

import org.apache.kafka.clients.producer.ProducerConfig;


/**
 * A simple example that shows how to read from and write to modern Kafka. This will read String
 * messages from the input topic, parse them into a POJO type {@link KafkaEvent}, group by some key,
 * and finally perform a rolling addition on each key for which the results are written back to
 * another topic.
 *
 * <p>This example also demonstrates using a watermark assigner to generate per-partition watermarks
 * directly in the Flink Kafka consumer. For demonstration purposes, it is assumed that the String
 * messages are of formatted as a (word,frequency,timestamp) tuple.
 *
 * <p>Example usage: --input-topic test-input --output-topic test-output --bootstrap.servers
 * localhost:9092 --group.id myconsumer
 */
public class HybridFileKafkaExample extends KafkaExampleUtil {

    static final CustomWatermarkExtractor watermarkExtractor = new CustomWatermarkExtractor();

    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = KafkaExampleUtil.prepareExecutionEnv(parameterTool);

        FileSource<KafkaEvent> fileSource =
                FileSource.forRecordStreamFormat(new KafkaEventRecordFormat(),
                        Path.fromLocalFile(new File(parameterTool.getRequired("input-file")))).build();
        KafkaSource<KafkaEvent> kafkaSource = KafkaSource.<KafkaEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setGroupId("MyGroup")
                .setTopics(Arrays.asList(parameterTool.getRequired("input-topic")))
                .setDeserializer(
                    KafkaRecordDeserializationSchema.valueOnly(new KafkaEventSchema()))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();
        HybridSource<KafkaEvent> hybridSource =
                HybridSource.builder(fileSource)
                  .addSource(kafkaSource)
                  .build();

        DataStream<KafkaEvent> input =
                env.fromSource(
                        hybridSource,
                        WatermarkStrategy.<KafkaEvent>forMonotonousTimestamps().withTimestampAssigner(
                                (event, timestamp) -> watermarkExtractor.extractTimestamp(
                                        event,
                                        timestamp)),
                        "hybrid-source")
//                        .keyBy(r -> r.getWord(), TypeInformation.of(String.class))
                        .map(new RollingAdditionMapper(), TypeInformation.of(KafkaEvent.class));

        input.sinkTo(
                KafkaSink.<KafkaEvent>builder()
                        .setBootstrapServers(
                                parameterTool
                                        .getProperties()
                                        .getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(parameterTool.getRequired("output-topic"))
                                        .setValueSerializationSchema(new KafkaEventSchema())
                                        .build())
                        .build());

        env.execute("Hybrid (File + Kafka) Example");
    }
}
