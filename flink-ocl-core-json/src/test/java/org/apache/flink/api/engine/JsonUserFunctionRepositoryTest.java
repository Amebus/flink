package org.apache.flink.api.engine;

import org.apache.flink.helpers.Constants;
import org.junit.Test;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JsonUserFunctionRepositoryTest
{
	private JsonUserFunction getMap1()
	{
		return new JsonUserFunction()
		{
			@Override
			public String getType()
			{
				return "map";
			}
			
			@Override
			public String getName()
			{
				return "map1";
			}
			
			@Override
			public String getFunction()
			{
				return "map1_line1;\nmap1_line2;\nmap1_line3;\n";
			}
			
			@Override
			public String getInputTupleName()
			{
				return "tupleOne";
			}
			
			@Override
			public String getOutputTupleName()
			{
				return "tupleTwo";
			}
		};
	}
	
	private JsonUserFunction getMap2()
	{
		return new JsonUserFunction()
		{
			@Override
			public String getType()
			{
				return "map";
			}
			
			@Override
			public String getName()
			{
				return "map2";
			}
			
			@Override
			public String getFunction()
			{
				return "map2_line1;\nmap2_line2;\nmap2_line3;\n";
			}
			
			@Override
			public String getInputTupleName()
			{
				return "tupleTwo";
			}
			
			@Override
			public String getOutputTupleName()
			{
				return "tupleOne";
			}
		};
	}
	
	private JsonUserFunctionRepository getRepository(String pFileName)
	{
		return new JsonUserFunctionRepository(Constants.FUNCTIONS_DIR, pFileName);
	}
	
	private Collection<IUserFunction> getUserFunctions(String pFileName)
	{
		return Objects.requireNonNull(getRepository(pFileName)).getUserFunctions();
	}
	
	@Test
	public void LoadFunctions_Ok()
	{
		Collection<IUserFunction> userFunctions = getUserFunctions("functions.json");
		
		Optional<IUserFunction> vOptional;
		IUserFunction vTempUF;
		
		assertTrue(userFunctions.stream().allMatch(IUserFunction::isMap));
		assertEquals(1L, userFunctions.stream().filter(x -> x.getName().equals("map1")).count());
		
		vOptional = userFunctions.stream().filter(x -> x.getName().equals("map1")).findFirst();
		assertTrue(vOptional.isPresent());
		vTempUF = vOptional.get();
		assertEquals(getMap1(), vTempUF);
		
		assertEquals(1L, userFunctions.stream().filter(x -> x.getName().equals("map2")).count());
		vOptional = userFunctions.stream().filter(x -> x.getName().equals("map2")).findFirst();
		assertTrue(vOptional.isPresent());
		vTempUF = vOptional.get();
		assertEquals(getMap2(), vTempUF);
	}
	
	@Test
	public void OneFunctionPerType_Ok()
	{
		Collection<? extends IUserFunction> userFunctions = getUserFunctions("oneFunctionPerType.json");
		
		assertEquals(1, userFunctions.stream().filter(IUserFunction::isMap).count());
		assertEquals(1, userFunctions.stream().filter(IUserFunction::isFilter).count());
		assertEquals(1, userFunctions.stream().filter(IUserFunction::isFlatMap).count());
		assertEquals(1, userFunctions.stream().filter(IUserFunction::isReduce).count());
	}
	
	@Test
	public void GetUserFunctionByName_Ok()
	{
		JsonUserFunctionRepository vRepo = getRepository("functions.json");
		
		JsonUserFunction vUserFunction = getMap1();
		
		assertEquals(vUserFunction, vRepo.getUserFunctionByName(vUserFunction.getName()));
		
		vUserFunction = getMap2();
		
		assertEquals(vUserFunction, vRepo.getUserFunctionByName(vUserFunction.getName()));
	}
	
	@Test
	public void EmptyBodyFunction_ThrowsError_Ok()
	{
		boolean rightExceptionWasThrow = false;
		try
		{
			getUserFunctions("emptyBodyFunctions.json");
		}
		catch (IllegalArgumentException ex)
		{
			rightExceptionWasThrow = ex.getMessage().endsWith("has no body.");
		}
		assertTrue(rightExceptionWasThrow);
	}
	
	@Test
	public void NoOutputMapFunction_ThrowsError_Ok()
	{
		boolean rightExceptionWasThrow = false;
		try
		{
			getUserFunctions("noOutputMapFunction.json");
		}
		catch (IllegalArgumentException ex)
		{
			rightExceptionWasThrow = ex.getMessage().endsWith("specify an output tuple.");
		}
		assertTrue(rightExceptionWasThrow);
	}
	
	@Test
	public void NoOutputFlatMapFunction_ThrowsError_Ok()
	{
		boolean rightExceptionWasThrow = false;
		try
		{
			getUserFunctions("noOutputFlatMapFunction.json");
		}
		catch (IllegalArgumentException ex)
		{
			rightExceptionWasThrow = ex.getMessage().endsWith("specify an output tuple.");
		}
		assertTrue(rightExceptionWasThrow);
	}
	
	@Test
	public void UnknownFunction_ThrowsError_Ok()
	{
		boolean rightExceptionWasThrow = false;
		try
		{
			getUserFunctions("unknownFunction.json");
		}
		catch (IllegalArgumentException ex)
		{
			rightExceptionWasThrow = ex.getMessage().endsWith(" which is unknown.");
		}
		assertTrue(rightExceptionWasThrow);
	}
}
