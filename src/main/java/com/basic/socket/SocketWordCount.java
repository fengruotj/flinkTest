package com.basic.socket;

import com.basic.model.WordWithCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * locate com.basic.socket
 * Created by 79875 on 2017/4/9.
 * 单词统计数据源为 Socket数据源
 * flink run -c com.basic.socket.SocketWordCount flinkTest-1.0-SNAPSHOT.jar --hostname root2 --port 9999 --envParallelism 8 --outputfile hdfs://root2:9000/user/root/flinkwordcount/output/wordcounresult.txt

 */
public class SocketWordCount {
    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // the port to connect to
        final int port;
        final String hostname;
        final int envParallelism;
        final String outputfile;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            port = params.getInt("port");
            hostname=params.get("hostname");
            envParallelism=params.getInt("envParallelism");
            outputfile=params.get("outputfile");
            System.out.println("hostname: "+hostname+" port: "+port+" envParallelism: "+envParallelism+" outputfile: "+outputfile);
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>', " +
                    "where port is the address of the text server");
            System.err.println("To start a simple text server, run 'netcat -l <port>' and type the input text " +
                    "into the command line");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");

        // parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts = text
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) {
                        for (String word : value.split(" ")) {
                            out.collect(new WordWithCount(word, 1L));
                        }
                    }
                }).setParallelism(envParallelism)
                .keyBy("word")
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                }).setParallelism(envParallelism);

        // print the results with a single thread, rather than in parallel
        windowCounts.writeAsText(outputfile).setParallelism(envParallelism);
        //windowCounts.print().setParallelism(1);
        env.execute("Socket WordCount");
    }

    // ------------------------------------------------------------------------

}
