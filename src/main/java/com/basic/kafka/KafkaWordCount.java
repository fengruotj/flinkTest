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

package com.basic.kafka;

import com.basic.model.WordWithCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import java.util.Properties;


/**
 * Read Strings from Kafka and print them to standard out.
 * Note: On a cluster, DataStream.print() will print to the TaskManager's .out file!
 *
 * Please pass the following arguments to run the example:
 * 	--topic test --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myconsumer
 *
 * flink run -c com.basic.kafka.KafkaWordCount flinkTest-1.0-SNAPSHOT.jar --topic tweetswordtopic5 --envParallelism 1 --output hdfs://root2:9000/user/root/flinkwordcount/output/wordcountresult.txt
 */
public class KafkaWordCount {

	public static void main(String[] args) throws Exception {
		// parse input arguments
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);
		final int envParallelism;
		if(parameterTool.getNumberOfParameters() < 2) {
			System.out.println("Missing parameters!\nUsage: Kafka --topic <topic> ");
			return;
		}
		envParallelism=parameterTool.getInt("envParallelism");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// set default parallelism for all operators (recommended value: number of available worker CPU cores in the cluster (hosts * cores))
		env.setParallelism(envParallelism);

		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
		env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface

		//设置kafka必要参数
		Properties properties=new Properties();
		properties.put("zookeeper.connect","root2:2181,root4:2181,root5:2181");
		properties.put("group.id","flink-kafka");
		properties.put("bootstrap.servers","root8:9092,root9:9092,root10:9092");
		properties.put("auto.offset.reset","smallest");
		properties.put("serializer.class","kafka.serializer.StringEncoder");

		DataStream<String> messageStream = env
				.addSource(new FlinkKafkaConsumer08<>(
						parameterTool.getRequired("topic"),
						new SimpleStringSchema(),
						properties)).setParallelism(18);//KafKa 默认为18个分区

		// parse the data, group it, window it, and aggregate the counts
		DataStream<WordWithCount> windowCounts = messageStream
				.flatMap(new FlatMapFunction<String, WordWithCount>() {
					@Override
					public void flatMap(String value, Collector<WordWithCount> out) {
						for (String word : value.split(" ")) {
							out.collect(new WordWithCount(word, 1L));
						}
					}
				})
				.keyBy("word")
				.reduce(new ReduceFunction<WordWithCount>() {
					@Override
					public WordWithCount reduce(WordWithCount a, WordWithCount b) {
						return new WordWithCount(a.word, a.count + b.count);
					}
				});

		if (parameterTool.has("output")) {
			windowCounts.writeAsText(parameterTool.get("output")).setParallelism(envParallelism);
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			//windowCounts.print();
		}

		env.execute("Read from Kafka WordCount");
	}
}
