package com.acadgild.assigment;

import java.io.IOException;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import static java.util.stream.Collectors.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvalidTVMapper extends Mapper <LongWritable, Text, LongWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		Predicate<String> predicate=p-> p.equals("NA");
		List<String> NAList=Arrays.stream(value.toString().split("\\|")).limit(2).filter(predicate).collect(toList());
		if(NAList.size()>0){
			context.write(key, value);
		}
	}
}
