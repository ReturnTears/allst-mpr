package com.allst.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWindowWCJava {

	public static void main(String[] args) throws Exception {
		int port = 9000;
//		try {
//			ParameterTool parameterTool = ParameterTool.fromArgs(args);
//			port = parameterTool.getInt("port");
//		} catch (Exception e) {
//			System.out.println("No port set");
//			port = 9000;
//		}
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		String hostname = "master";
		String delimiter = "\n";
		DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);
		
		DataStream<WordWithCount> windowCounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String value, Collector<WordWithCount> collector) throws Exception {
                String[] splits = value.split("\\s");
                for (String word : splits) {
                    collector.collect(new WordWithCount(word, 1L));
                }
            }
        }).keyBy("word")
                .timeWindow(Time.seconds(2), Time.seconds(1))
                .sum("count");
		windowCounts.print().setParallelism(1);
        env.execute("Socket window count");
	}
	
	private static class WordWithCount {
        String word;
        long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

}
