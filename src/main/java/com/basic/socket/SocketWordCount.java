package com.basic.socket;

import com.basic.model.WordWithCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * locate com.basic.socket
 * Created by 79875 on 2017/4/9.
 * 单词统计数据源为 Socket数据源
 * nc -lk 9999
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
        final ParameterTool params = ParameterTool.fromArgs(args);
        try {
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

        // start a checkpoint every 1000 ms
        env.enableCheckpointing(1000);

// advanced options:

// set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

// checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60000);

// allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

// enable externalized checkpoints which are retained after job cancellation
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // set default parallelism for all operators (recommended value: number of available worker CPU cores in the cluster (hosts * cores))
        env.setParallelism(envParallelism);

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
                })
                .keyBy("word")
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });

        if (params.has("outputfile")) {
            windowCounts.writeAsText(params.get("outputfile")).setParallelism(envParallelism);
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            //windowCounts.print();
        }
        env.execute("Socket WordCount");
    }

    // ------------------------------------------------------------------------

}
