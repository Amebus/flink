import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.configuration.JsonSettingsRepository;
import org.apache.flink.api.configuration.JsonTupleDefinitionsRepository;
import org.apache.flink.api.engine.JsonUserFunctionRepository;
import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.api.tuple.Tuple1Ocl;
import org.apache.flink.helpers.Constants;
import org.apache.flink.api.bridge.OclContext;
import org.apache.flink.streaming.SimpleTest;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
		vTuples.add(new Tuple1Ocl<>(2));
		vTuples.add(new Tuple1Ocl<>(3));
		
		Iterable<? extends IOclTuple> vResult;
		
		
//		vResult = vContext.filter("filterFunction", vTuples);
//		vResult.forEach(x ->
//						{
//							Tuple1Ocl<Integer> vT = (Tuple1Ocl<Integer>)x;
//							System.out.println(vT.<Integer>getField(0));
//						});
		
//		vResult = vContext.map("mapFunction", vTuples);
//		vResult.forEach(x ->
//						{
//							Tuple1Ocl<Integer> vT = (Tuple1Ocl<Integer>)x;
//							System.out.println(vT.<Integer>getField(0));
//						});
		
		vContext.close();
	}
	
	@Test
	public void simpleMapTest() throws  Exception
	{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		OclContext vContext = new OclContext(new JsonSettingsRepository(Constants.RESOURCES_DIR),
											 new JsonTupleDefinitionsRepository(Constants.RESOURCES_DIR),
											 new JsonUserFunctionRepository(Constants.FUNCTIONS_DIR,
																			"filterFunction2.json"));
		vContext.open();
		
		
		DataStream<Integer> dataStream = env
			.readTextFile("file:///home/federico/GitKraken/ConfigReader" )
			.map(new SimpleTest.AAA())
//			.filter( a -> a > 2)
//			.writeAsText("/home/federico/aaa")
			.countWindowAll(4)
			.process(new ProcessAllWindowFunction<Integer, Integer, GlobalWindow>()
			{
				@Override
				public void process(Context context, Iterable<Integer> elements, Collector<Integer> out) throws Exception
				{
					List<IOclTuple> vTuples = new ArrayList<>();
					for (Integer vElement : elements)
					{
						vTuples.add(new Tuple1Ocl<>(vElement));
					}

					Iterable<? extends IOclTuple> vResult = vContext.filter("filterFunction", vTuples);

					for (IOclTuple vOclTuple : vResult)
					{
						out.collect(vOclTuple.getField(0));
					}
				}
			})
			;
//			.timeWindowAll(Time.milliseconds(1))
//			.reduce(new ReduceAAA());
//			.reduce((a, b) -> a + b);
		
		dataStream.print();
		
		env.execute("Window WordCount");
		
		vContext.close();
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
