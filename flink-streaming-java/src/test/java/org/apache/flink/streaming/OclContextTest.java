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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class OclContextTest
{
	
	private OclContext getOclContext(String pFileName)
	{
		return new OclContext(new JsonSettingsRepository(Constants.RESOURCES_DIR),
					   new JsonTupleRepository.Builder(Constants.RESOURCES_DIR).build(),
					   new JsonUserFunctionRepository
						   .Builder(Constants.FUNCTIONS_DIR)
						   .setFileName(pFileName).build(),
					   new DefaultsValues.DefaultOclContextMappings());
	}
	
	@Rule
	public ExpectedException expectedException = ExpectedException.none();
	
	
	@Test
	public void OclReduceSimple()
	{
		OclContext vContext = getOclContext("reduce.json");
		
		vContext.open();
		
		vContext.close();
	}
	
//	@Test
	public void OclMapSimple_IntToString()
	{
		OclContext vContext = getOclContext("functions.json");
		
		vContext.open();
		
		List<IOclTuple> vTuples = GetTestTuples();
		
		Iterable<? extends IOclTuple> vResult
			= vContext.map("mapIntToString", vTuples, vTuples.size());
		
		vContext.close();
		Iterator<IOclTuple> vIterator = vTuples.iterator();
		vResult.forEach(pOclTuple ->
						{
							IOclTuple vTuple = vIterator.next();
							assertEquals(vTuple.getField(0).toString(), pOclTuple.<String>getField(0));
						});
	}
	
//	@Test
	public void AAA()
	{
		KernelCodeBuilderEngine vEngine = new KernelCodeBuilderEngine(
			new JsonSettingsRepository(Constants.RESOURCES_DIR),
			new JsonTupleRepository.Builder(Constants.RESOURCES_DIR).build(),
			new JsonUserFunctionRepository
				.Builder(Constants.FUNCTIONS_DIR)
				.build()
				.getUserFunctions(),
			new DefaultsValues.DefaultOclContextMappings().getFunctionKernelBuilderMapper(),
			new DefaultsValues.DefaultOclContextMappings().getFunctionKernelBuilderOptionMapper());
		
		CppLibraryInfo vInfo = vEngine.generateKernels();
		
		System.out.println(vInfo.getKernelsFolder());
	}
	
//	@Test
	public void OclMapSimple_StringToInt()
	{
		OclContext vContext = getOclContext("functions.json");
		
		vContext.open();
		
		List<IOclTuple> vTuples = GetTestTuples()
			.stream()
			.map(pIOclTuple -> new Tuple1Ocl<>(pIOclTuple.<Integer>getField(0).toString()))
			.collect(Collectors.toList());
		
		Iterable<? extends IOclTuple> vResult
			= vContext.map("mapStringToInt", vTuples, vTuples.size());
		vContext.close();
		
		Iterator<IOclTuple> vIterator = vTuples.iterator();
		vResult.forEach(pOclTuple ->
						{
							IOclTuple vTuple = vIterator.next();
							assertEquals(Integer.valueOf(vTuple.getField(0)), pOclTuple.<Integer>getField(0));
						});
		
	}
	
//	@Test
	public void OclFilterSimple()
	{
		OclContext vContext = getOclContext("filterFunction2.json");
		vContext.open();
		
		List<IOclTuple> vTuples = GetTestTuples();
		TupleListInfo vListInfo = new TupleListInfo(vTuples);
		
		Iterable<? extends IOclTuple> vResult;
		
		vResult = vContext.filter("filterFunction", vTuples, vTuples.size());
		vContext.close();
		
		AtomicInteger vResultCount = new AtomicInteger();
		vResult.forEach(pO -> vResultCount.getAndIncrement());
		
		int vExpectedCount = vListInfo.countGreaterThan(2);
		System.out.println("OclFilterSimple - Expected: " + vExpectedCount + " - Actual: " + vResultCount);
		assertEquals(vExpectedCount, vResultCount.get());
		assertEquals(vListInfo.countLessOrEqualThan(2), vTuples.size() - vResultCount.get());
		
		List<IOclTuple> vGreaterThanTwo =
			vTuples
				.stream()
				.filter(pIOclTuple -> pIOclTuple.<Integer>getField(0) > 2)
				.collect(Collectors.toList());
		
		Iterator<IOclTuple> vIterator = vGreaterThanTwo.iterator();
		
		vResult.forEach(pO -> assertEquals(vIterator.next(), pO));
		
	}
	
	//@Test
	@SuppressWarnings("unchecked")
	public void A()
	{
		//new OclBridge().listDevices();
		OclContext vContext = getOclContext("functions.json");
		vContext.open();
		
		
		List<IOclTuple> vTuples = new ArrayList<>();
		
		vTuples.add(new Tuple1Ocl<>(0));
		vTuples.add(new Tuple1Ocl<>(1));
		vTuples.add(new Tuple1Ocl<>(-78));
		vTuples.add(new Tuple1Ocl<>(3));
		
		Iterable<? extends IOclTuple> vResult;
		IOclTuple vResult2;
//		IOclTuple vResult;
		
//		vResult = vContext.filter("filterFunction", vTuples);
//		vResult.forEach(x ->
//						{
//							Tuple1Ocl<Integer> vT = (Tuple1Ocl<Integer>)x;
//							System.out.println(vT.<Integer>getField(0));
//						});
		
//		vResult = vContext.map("mapFunction", vTuples, vTuples.size());
//		vResult.forEach(x ->
//						{
////							Tuple1Ocl<String> vT = (Tuple1Ocl<String>)x;
////							System.out.println(vT.<String>getField(0));
//							System.out.println(x.<String>getField(0));
//						});
		vResult2 = vContext.reduce("reduceFunction", vTuples, vTuples.size());
		System.out.println(vResult2.<Integer>getField(0));
		
		vContext.close();
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
	
	private List<IOclTuple> GetTestTuples()
	{
		List<IOclTuple> vConstTuples = new ArrayList<>();
		vConstTuples.add(new Tuple1Ocl<>(-1679099059));
		vConstTuples.add(new Tuple1Ocl<>(528136394));
		vConstTuples.add(new Tuple1Ocl<>(-1528862540));
		vConstTuples.add(new Tuple1Ocl<>(-1348335996));
		
		Random vRnd = new Random();
		int vMaxConstDiff = vConstTuples.size();
		int vMax = 50000;
//		int vMax = 1;
		List<IOclTuple> vTuples = new ArrayList<>(vMax);
		vTuples.addAll(vConstTuples);
		
		for (int vI = 0; vI < vMax - vMaxConstDiff; vI++)
		{
			vTuples.add(new Tuple1Ocl<>(vRnd.nextInt()));
		}
		return vTuples;
	}
	
	class TupleListInfo
	{
		private List<IOclTuple> mOclTuples;
		
		public TupleListInfo(List<IOclTuple> pOclTuples)
		{
			mOclTuples = pOclTuples;
		}
		
		public List<IOclTuple> getOclTuples()
		{
			return mOclTuples;
		}
		
		public int size()
		{
			return mOclTuples.size();
		}
		
		public int count(java.util.function.Predicate< ? super IOclTuple> filter)
		{
			return (int) mOclTuples.stream().filter(filter::test).count();
		}
		
		public int countEvenNumbers()
		{
			return count(pIOclTuple -> pIOclTuple.<Integer>getField(0) % 2 == 0);
		}
		
		public int countOddNumbers()
		{
			return size() - countEvenNumbers();
		}
		
		public int countGreaterThan(int pLimit)
		{
			return count( pIOclTuple -> pIOclTuple.<Integer>getField(0) > pLimit);
		}
		
		public int countLessOrEqualThan(int pLimit)
		{
			return size() - countGreaterThan(pLimit);
		}
	}
}
