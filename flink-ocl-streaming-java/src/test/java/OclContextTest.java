import org.apache.flink.api.configuration.JsonSettingsRepository;
import org.apache.flink.api.configuration.JsonTupleDefinitionsRepository;
import org.apache.flink.api.engine.JsonUserFunctionRepository;
import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.api.tuple.Tuple1Ocl;
import org.apache.flink.helpers.Constants;
import org.apache.flink.streaming.api.environment.OclContext;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class OclContextTest
{
	@Test
	@SuppressWarnings("unchecked")
	public void A()
	{
		//new OclBridge().listDevices();
		
		OclContext vContext = new OclContext(new JsonSettingsRepository(Constants.RESOURCES_DIR),
											 new JsonTupleDefinitionsRepository(Constants.RESOURCES_DIR),
											 new JsonUserFunctionRepository(Constants.FUNCTIONS_DIR,
																			"filterFunction2.json"));
		vContext.open();
		
		
		List<IOclTuple> vTuples = new ArrayList<>();
		
		vTuples.add(new Tuple1Ocl<>(0));
		vTuples.add(new Tuple1Ocl<>(1));
		vTuples.add(new Tuple1Ocl<>(2));
		vTuples.add(new Tuple1Ocl<>(3));
		
		List<? extends IOclTuple> vResult = vContext.filter("filterFunction", vTuples);
		
		System.out.println(vResult.size());
		
		vResult.forEach(x ->
						{
							Tuple1Ocl<Integer> vT = (Tuple1Ocl<Integer>)x;
							System.out.println(vT.<Integer>getField(0));
						});
		
		int i = 0;
		
		vContext.close();
	}
}
