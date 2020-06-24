package com.allst.mpr;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * map stage
 * @author root
 *
 */
public class MaprWcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] words = StringUtils.split(value.toString(), " ");
		for (String word : words) {
			context.write(new Text(word), new IntWritable(1));
		}
	}
}
