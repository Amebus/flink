package org.apache.flink.streaming.api.ocl.engine.builder;

import org.apache.flink.streaming.api.ocl.engine.builder.mappers.TemplatePluginMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.*;

public class FilterKernelBuilder extends KernelBuilder
{
	
	public FilterKernelBuilder()
	{
		super();
	}
	public FilterKernelBuilder(String pRootTemplate)
	{
		super(pRootTemplate);
	}
	public FilterKernelBuilder(String pRootTemplate, TemplatePluginMapper pTemplatePluginMapper)
	{
		super(pRootTemplate, pTemplatePluginMapper);
	}
	
	@Override
	protected KernelBuilder setUpExtras()
	{
		return this.setExtra("input-var-int", PDAKernelBuilderPlugin.Defaults.VarTypes.INT)
				   .setExtra("input-var-double", PDAKernelBuilderPlugin.Defaults.VarTypes.DOUBLE)
				   .setExtra("input-var-string", "__global " +
												 PDAKernelBuilderPlugin.Defaults.VarTypes.STRING +
												 "*");
	}
	
	@Override
	protected KernelBuilder setUpTemplatePluginMapper()
	{
		return super.setUpTemplatePluginMapper()
			 .registerPlugin("<[utility-vars]>", getUtilityVarsPlugin())
			 .registerPlugin("<[deserialization]>", new DeserializationPlugin())
			 .registerPlugin("<[serialization]>", getSerializationPlugin())
			 .registerPlugin("<[input-vars]>", new InputVarPlugin())
			 .registerPlugin("<[output-vars]>", getOutputVarsPlugin())
			 .registerPlugin("<[user-function]>", PDAKernelBuilderPlugin.USER_FUNCTION);
	}
	
	@Override
	protected IKernelBuilderPlugin getKernelCodePlugin()
	{
		return (pBuilder, pCodeBuilder) ->
			pCodeBuilder
				.append("\t\n")
				.append("\t<[utility-vars]>\n")
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
				.append("\tint _userIndex = _i;\n");
		
	}
	
	private IKernelBuilderPlugin getOutputVarsPlugin()
	{
		return (pKernelBuilder, pCodeBuilder) ->
			pCodeBuilder
				.append("\n// output-tuple\n")
				.append("unsigned char _r;")
				.append("\n");
	}
	
	private IKernelBuilderPlugin getSerializationPlugin()
	{
		return ((pKernelBuilder, pCodeBuilder) -> pCodeBuilder.append("\n_result[_gId] = _r;\n"));
	}
}