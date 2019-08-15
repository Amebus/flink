package org.apache.flink.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
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
//			.writeAsText("/home/federico/aaa")
			.countWindowAll(4)
			.process(new ProcessAllWindowFunction<Integer, Integer, GlobalWindow>()
			{
				@Override
				public void process(Context context, Iterable<Integer> elements, Collector<Integer> out) throws Exception
				{
					int vr = 0;
					for (Integer vElement : elements)
					{
						out.collect(vElement);
						vr += vElement;
					}
				}
			});
//			.timeWindowAll(Time.milliseconds(1))
//			.reduce(new ReduceAAA());
//			.reduce((a, b) -> a + b);
		
		dataStream.print();
		
		env.execute("Window WordCount");
	}
	
	public static class AAA implements MapFunction<String, Integer>
	{
		
		@Override
		public Integer map(String value) throws Exception
		{
			return Integer.valueOf(value);
		}
	}
	
	public static class ReduceAAA implements ReduceFunction<Integer>
	{
		
		/**
		 * The core method of ReduceFunction, combining two values into one value of the same type.
		 * The reduce function is consecutively applied to all values of a group until only a single value remains.
		 *
		 * @param value1 The first value to combine.
		 * @param value2 The second value to combine.
		 * @return The combined value of both input values.
		 *
		 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
		 *                   to fail and may trigger recovery.
		 */
		@Override
		public Integer reduce(Integer value1, Integer value2) throws Exception
		{
			return value1 + value2;
		}
	}
}
