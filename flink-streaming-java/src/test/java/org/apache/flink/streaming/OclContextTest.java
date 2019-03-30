package org.apache.flink.streaming;

import org.apache.flink.api.bridge.OclContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.configuration.JsonSettingsRepository;
import org.apache.flink.api.engine.CppLibraryInfo;
import org.apache.flink.api.engine.IUserFunctionsRepository;
import org.apache.flink.api.engine.JsonUserFunctionRepository;
import org.apache.flink.api.engine.KernelCodeBuilderEngine;
import org.apache.flink.api.engine.builder.options.DefaultsValues;
import org.apache.flink.api.configuration.JsonTupleRepository;
import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.api.tuple.Tuple1Ocl;
import org.apache.flink.api.typeutils.OclTupleTypeInfo;
import org.apache.flink.configuration.ISettingsRepository;
import org.apache.flink.configuration.ITupleDefinitionRepository;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.helpers.Constants;
import org.apache.flink.streaming.helpers.OclContextHelpers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.helpers.OclContextHelpers.*;
import static org.junit.Assert.assertEquals;

public class OclContextTest
{
	
	@Rule
	public ExpectedException expectedException = ExpectedException.none();
	
	
//	@Test
	public void OclReduceSimple()
	{
		OclContext vContext = null;//getOclContext("OclReduceTest.json");
		
		vContext.open();
		
		List<IOclTuple> vTuples = new ArrayList<>(2);
		
		vTuples.add(new Tuple1Ocl<>(1));
		vTuples.add(new Tuple1Ocl<>(5));
		vTuples.add(new Tuple1Ocl<>(73));
		vTuples.add(new Tuple1Ocl<>(10));
		
//		vTuples = GetIntegerTestTuples();
		
		IOclTuple vResult = vContext.reduce("reduceTest", vTuples, vTuples.size());
		
		vContext.close();
		
		assertEquals(
			vTuples.stream().mapToInt(pT -> pT.getField(0)).sum(),
			(int)vResult.getField(0));
		
	}
	
//	@Test
	public void simpleMapTest() throws  Exception
	{
		ISettingsRepository a = new JsonSettingsRepository(Constants.RESOURCES_DIR);
		ITupleDefinitionRepository b = new JsonTupleRepository.Builder(Constants.RESOURCES_DIR).build();
		IUserFunctionsRepository c = new JsonUserFunctionRepository
			.Builder(Constants.FUNCTIONS_DIR)
			.setFileName("filterFunction2.json").build();
		
		OclContext vContext = new OclContext(a, b, c, new DefaultsValues.DefaultOclContextMappings());
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