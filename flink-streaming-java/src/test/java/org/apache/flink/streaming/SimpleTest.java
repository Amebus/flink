package org.apache.flink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SimpleTest
{
	@Rule
	public ExpectedException expectedException = ExpectedException.none();
	
	@Test
	public void simpleMapTest() throws  Exception
	{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<Integer> dataStream = env
			.readTextFile("file:///home/federico/GitKraken/ConfigReader" )
			.map(new AAA())
			.timeWindowAll(Time.milliseconds(1))
			.reduce((a, b) -> a + b);
		
		dataStream.print();
		
		
		env.execute("Window WordCount");
	}
	
	public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>>
	{
		@Override
		public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
			for (String word: sentence.split(" ")) {
				out.collect(new Tuple2<String, Integer>(word, 1));
			}
		}
	}
	
	public static class AAA implements MapFunction<String, Integer>
	{
		
		@Override
		public Integer map(String value) throws Exception
		{
			return Integer.valueOf(value);
		}
	}
}
