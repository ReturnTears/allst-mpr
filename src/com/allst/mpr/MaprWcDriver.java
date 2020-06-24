package com.allst.mpr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * job stage
 * @author root
 *
 */
public class MaprWcDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("Hello Mpr");
		Configuration conf = new Configuration();
		conf.set("fs:defaultFS", "hdfs://master:9000");
		Job job = Job.getInstance(conf, "countDriver");
		job.setJarByClass(MaprWcDriver.class);
        job.setMapperClass(MaprWcMapper.class);
        job.setReducerClass(MaprWcReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path("hdfs://master:9000/root/data/hdpInput/inp.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/output/hdpOutput/1"));
        if (!job.waitForCompletion(true)) {
        	return;
        }
	}

}
