package com.allst.mpr;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Mapper+Reducer+Driver
 * @author root
 *
 */
public class MaprWordCount {

	public MaprWordCount() {
		super();
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		// String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();// need add parameters in program arguments
		String[] otherArgs = new String[]{"hdfs://master:9000/root/data/hdpInput/inp.txt", "hdfs://master:9000/output/hdpOutput/1"}; 
		if(otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
		Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(MaprWordCount.class);
        job.setMapperClass(MaprWordCount.TokenizerMapper.class);
        job.setCombinerClass(MaprWordCount.TokenizerReducer.class);
        job.setReducerClass(MaprWordCount.TokenizerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        for(int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
 
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class TokenizerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		public TokenizerReducer() {
			super();
		}

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			IntWritable val;
			
			for(Iterator<IntWritable> i$ = values.iterator(); i$.hasNext(); sum += val.get()) {
                val = (IntWritable)i$.next();
            }
 
            this.result.set(sum);
            context.write(key, this.result);
		}
	}
	
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		
		public TokenizerMapper() {
			super();
		}
		
		private static final IntWritable one = new IntWritable(1);
		
		private Text word = new Text();
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				this.word.set(itr.toString());
				context.write(this.word, one);
			}
		}
		
	}
}
