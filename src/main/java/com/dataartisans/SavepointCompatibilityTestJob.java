/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;
import java.util.UUID;

public class SavepointCompatibilityTestJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.disableOperatorChaining();
		env.setStateBackend(new RocksDBStateBackend("hdfs:///user/tzulitai/flink-savepoint-1.2"));

		ParameterTool param = ParameterTool.fromArgs(args);

		Properties kafkaProps = new Properties();
		kafkaProps.setProperty("group.id", UUID.randomUUID().toString());
		kafkaProps.setProperty("bootstrap.servers", param.getRequired("broker"));
		kafkaProps.setProperty("security.protocol", "SASL_PLAINTEXT");
		kafkaProps.setProperty("sasl.kerberos.service.name", "kafka"); // use kerberos

		env.addSource(new SourceFunction<String>() {
			long value = 0;
			volatile boolean running = true;

			public void run(SourceContext<String> sourceContext) throws Exception {
				while(running) {
					sourceContext.collect(String.valueOf(value++));
					Thread.sleep(10);
				}
			}

			public void cancel() {
				running = false;
			}
		}).addSink(new FlinkKafkaProducer09<String>("kafka09-kerberos-test", new SimpleStringSchema(), kafkaProps));

		env.addSource(new FlinkKafkaConsumer09<String>("kafka09-kerberos-test", new SimpleStringSchema(), kafkaProps))
				.map(new EmptyStateMapper())
				.map(new LargeStateMapper())
				.addSink(new DiscardingSink<String>());

		env.execute("Kafka 09 with Kerberos Test");
	}

	public static class EmptyStateMapper extends RichMapFunction<String, String> {

		@Override
		public String map(String s) throws Exception {
			return s;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			// register some empty state
			getRuntimeContext().getState(new ValueStateDescriptor<Long>("emptyState", Long.class));
		}
	}

	public static class LargeStateMapper extends RichMapFunction<String, String> {

		private ValueState<Boolean> wroteState;
		private ListState<String> stringsState;

		@Override
		public String map(String s) throws Exception {
			if (!wroteState.value()) {
				wroteState.update(true);
			}
			stringsState.add(s);
			return s;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			wroteState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("wroteState", Boolean.class));
			stringsState = getRuntimeContext().getListState(new ListStateDescriptor<String>("stringsState", String.class));
		}
	}

}
