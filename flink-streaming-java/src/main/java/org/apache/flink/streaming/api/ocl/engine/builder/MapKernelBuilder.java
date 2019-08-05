package org.apache.flink.streaming.api.ocl.engine.builder;

import org.apache.flink.streaming.api.ocl.engine.builder.mappers.TemplatePluginMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.*;

public class MapKernelBuilder extends KernelBuilder
{
	public MapKernelBuilder()
	{
		super();
	}
	public MapKernelBuilder(String pRootTemplate)
	{
		super(pRootTemplate);
	}
	public MapKernelBuilder(String pRootTemplate, TemplatePluginMapper pTemplatePluginMapper)
	{
		super(pRootTemplate, pTemplatePluginMapper);
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
	
	@Override
	protected IKernelBuilderPlugin getKernelCodePlugin()
	{
		return (pBuilder, pCodeBuilder) ->
			pCodeBuilder
				.append("\t\n")
				.append("\t<[utility-vars]>\n")
				.append("\t<[output-utility-vars]>\n")
				.append("\t<[input-vars]>\n")
				.append("\t<[output-vars]>\n")
				.append("\t<[deserialization]>\n")
				.append("\t<[user-function]>\n")
				.append("\t<[serialization]>")
				.append("\t\n");
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


