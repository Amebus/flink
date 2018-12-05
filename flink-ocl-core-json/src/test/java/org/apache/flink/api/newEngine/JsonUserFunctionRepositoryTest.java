package org.apache.flink.api.newEngine;

import org.apache.flink.api.configuration.JsonSettingsRepository;
import org.apache.flink.api.engine.IUserFunction;
import org.apache.flink.api.engine.JsonUserFunction;
import org.apache.flink.api.engine.JsonUserFunctionRepository;
import org.apache.flink.api.newEngine.kernel.KernelCodeBuilderEngine;
import org.apache.flink.streaming.helpers.Constants;
import org.junit.Test;

import java.util.Collection;
import java.util.Objects;

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
		return new JsonUserFunctionRepository
			.Builder(Constants.FUNCTIONS_DIR)
			.setFileName(pFileName)
			.build();
	}
	
	private Collection<IUserFunction> getUserFunctions(String pFileName)
	{
		return Objects.requireNonNull(getRepository(pFileName)).getUserFunctions();
	}
	
	@Test
	public void B()
	{
		Collection<IUserFunction> userFunctions = getUserFunctions("functions.json");
		
		
	}
}
