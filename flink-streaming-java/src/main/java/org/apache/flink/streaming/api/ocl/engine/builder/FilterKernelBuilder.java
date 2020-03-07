package org.apache.flink.streaming.api.ocl.engine.builder;

import org.apache.flink.streaming.api.ocl.engine.builder.plugins.DeserializationPlugin;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.InputVarPlugin;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.PDAKernelBuilderPlugin;

public class FilterKernelBuilder extends KernelBuilder
{
	
	public FilterKernelBuilder()
	{
		super();
	}
	
	@Override
	protected String getKernelType()
	{
		return "filter";
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
			 .registerPlugin("<[input-vars]>", new InputVarPlugin())
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
