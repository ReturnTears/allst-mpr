package com.allst.mpr;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * 
 * @author root
 *
 */
public class HadoopDataType {
	/**
	 * Hadoop Text type
	 */
	private static void testText() {
		System.out.println("test Text");
		Text text = new Text("hello Hadoop!");
		System.out.println(text.getLength());
		System.out.println(text.find("a"));
		System.out.println(text.toString());
	}
	
	/**
	 * ArrayWritable
	 */
	private static void testArrayWritable() {
		System.out.println(String.format("%s", "testArrayWritable"));
		ArrayWritable arr = new ArrayWritable(IntWritable.class);
		IntWritable year = new IntWritable(2020);
		IntWritable month = new IntWritable(07);
		IntWritable date = new IntWritable(01);
		arr.set(new IntWritable[] {year, month, date});
		System.out.println(String.format("year=%d-month=%d-date=%d",
				((IntWritable)arr.get()[0]).get(),
				((IntWritable)arr.get()[1]).get(),
				((IntWritable)arr.get()[2]).get()));
	}
	
	/**
	 * MapWritable
	 */
	private static void testMapWritable() {
		System.out.println(String.format("%s", "testMapWritable"));
		MapWritable map = new MapWritable();
		Text k1 = new Text("name");
		Text v1 = new Text("KangKang");
		Text k2 = new Text("pass");
		map.put(k1, v1);
		map.put(k2, NullWritable.get());
		System.out.println(map.get(k1).toString());
		System.out.println(map.get(k2).toString());
	}
	
	public static void main(String[] args) {
		testText();
		testArrayWritable();
		testMapWritable();
	}

}
