package org.apache.flink.streaming;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.bridge.OclContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.configuration.JsonSettingsRepository;
import org.apache.flink.api.configuration.JsonTupleDefinitionsRepository;
import org.apache.flink.api.engine.IUserFunctionsRepository;
import org.apache.flink.api.engine.JsonUserFunctionRepository;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.api.tuple.Tuple1Ocl;
import org.apache.flink.api.typeutils.OclTupleTypeInfo;
import org.apache.flink.configuration.ISettingsRepository;
import org.apache.flink.configuration.ITupleDefinitionsRepository;
import org.apache.flink.streaming.SimpleTest;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.helpers.Constants;
import org.apache.flink.util.Collector;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import scala.Int;

import java.util.ArrayList;
import java.util.List;

public class OclContextTest
{
	@Rule
	public ExpectedException expectedException = ExpectedException.none();
	
	@Test
	@SuppressWarnings("unchecked")
	public void A()
	{
		//new OclBridge().listDevices();
		
		OclContext vContext = new OclContext(new JsonSettingsRepository(Constants.RESOURCES_DIR),
											 new JsonTupleDefinitionsRepository(Constants.RESOURCES_DIR),
											 new JsonUserFunctionRepository(Constants.FUNCTIONS_DIR,
																			"functions.json"));
		vContext.open();
		
		
		List<IOclTuple> vTuples = new ArrayList<>();
		
		vTuples.add(new Tuple1Ocl<>(0));
		vTuples.add(new Tuple1Ocl<>(1));
		vTuples.add(new Tuple1Ocl<>(-78));
		vTuples.add(new Tuple1Ocl<>(3));
		
//		Iterable<? extends IOclTuple> vResult;
		IOclTuple vResult;
		
//		vResult = vContext.filter("filterFunction", vTuples);
//		vResult.forEach(x ->
//						{
//							Tuple1Ocl<Integer> vT = (Tuple1Ocl<Integer>)x;
//							System.out.println(vT.<Integer>getField(0));
//						});
		
		vResult = vContext.reduce("mapFunction", vTuples, vTuples.size());
//		vResult.forEach(x ->
//						{
////							Tuple1Ocl<String> vT = (Tuple1Ocl<String>)x;
////							System.out.println(vT.<String>getField(0));
//							System.out.println(x.<String>getField(0));
//						});

		
		vContext.close();
	}
	
	@Test
	public void simpleMapTest() throws  Exception
	{
		
		ISettingsRepository a = new JsonSettingsRepository(Constants.RESOURCES_DIR);
		ITupleDefinitionsRepository b = new JsonTupleDefinitionsRepository(Constants.RESOURCES_DIR);
		IUserFunctionsRepository c = new JsonUserFunctionRepository(Constants.FUNCTIONS_DIR,
																	"filterFunction2.json");
		OclContext vContext = new OclContext(a, b, c);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(vContext);
		vContext.open();
		
		int vWindowDim = 4;
		
		final int[] vA = {0};
		
		DataStream<Tuple1Ocl<Integer>> dataStream = env
			.readTextFile("file:///home/federico/GitKraken/ConfigReader" )
			.map(new AAA())
//			.filter( a -> a > 2)
//			.writeAsText("/home/federico/aaa")
			.countWindowAll(vWindowDim)
			.<Tuple1Ocl<Integer>>oclFilter("filterFunction",
										   OclTupleTypeInfo.<Tuple1Ocl>getBasicTupleTypeInfo(Integer.class))
			.countWindowAll(2)
//			.timeWindowAll(Time.milliseconds(1))
			.reduce(new ReduceAAA())
//			.reduce((a, b) -> a + b);
		;
		
		dataStream.print();
		
		env.execute("Window WordCount");
		
		vContext.close();
	}
	
	public static class AAA implements MapFunction<String, Tuple1Ocl<Integer>>
	{
		
		@Override
		public Tuple1Ocl<Integer> map(String value) throws Exception
		{
			return new Tuple1Ocl<>(Integer.valueOf(value));
		}
	}
	
	public static class ReduceAAA implements ReduceFunction<Tuple1Ocl<Integer>>
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
//		@Override
//		public Integer reduce(Integer value1, Integer value2) throws Exception
//		{
//			return value1 + value2;
//		}
		
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
		public Tuple1Ocl<Integer> reduce(Tuple1Ocl<Integer> value1, Tuple1Ocl<Integer> value2) throws Exception
		{
			return new Tuple1Ocl<>(value1.<Integer>getField(0) + value2.<Integer>getField(0));
		}
	}
}
