package org.apache.flink.streaming.api.ocl.engine.builder;

import org.apache.flink.streaming.api.ocl.engine.builder.mappers.TemplatePluginMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.*;

public class MapKernelBuilder extends KernelBuilder
{
	public MapKernelBuilder()
	{
		super();
	}
	
	@Override
	protected String getKernelType()
	{
		return "map";
	}
	
	@Override
	protected KernelBuilder setUpTemplatePluginMapper()
	{
		return super.setUpTemplatePluginMapper()
					.registerPlugin("<[utility-vars]>", getUtilityVarsPlugin())
					.registerPlugin("<[output-utility-vars]>", new OutputUtilityVarsPlugin())
					.registerPlugin("<[deserialization]>", new DeserializationPlugin())
					.registerPlugin("<[serialization]>", new SerializationPlugin())
					.registerPlugin("<[input-vars]>", new InputVarPlugin())
					.registerPlugin("<[output-vars]>", new OutputVarPlugin())
					.registerPlugin("<[user-function]>", PDAKernelBuilderPlugin.USER_FUNCTION);
		
	}
	
	protected IKernelBuilderPlugin getUtilityVarsPlugin()
	{
		return (pBuilder, pCodeBuilder) ->
			pCodeBuilder
				.append("\tunsigned char _arity = ")
				.append(pBuilder.getKernelBuilderOptions().getInputTuple().getArity())
				.append(";\n");
			
	}
}


