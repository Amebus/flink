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
				.append("\n")
				.append("// utility variables\n")
				.append("\tuint _gId = get_global_id(0);\n")
				.append("\tunsigned char _arity = ")
				.append(pBuilder.getKernelBuilderOptions().getInputTuple().getArity())
				.append(";\n")
				.append("\tint _i = _dataIndexes[_gId];\n")
				.append("\tint _userIndex = _i;\n")
				.append("\tunsigned char* _serializationTemp;\n");
			
	}
}


