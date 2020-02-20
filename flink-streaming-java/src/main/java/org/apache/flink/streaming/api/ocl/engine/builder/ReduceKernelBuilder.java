package org.apache.flink.streaming.api.ocl.engine.builder;

import org.apache.flink.streaming.api.ocl.engine.ITupleBytesDimensionGetter;
import org.apache.flink.streaming.api.ocl.engine.IUserFunction;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.TemplatePluginMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.*;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.reduce.*;
import org.apache.flink.streaming.configuration.ITupleDefinition;

import java.io.IOException;
import java.nio.file.Files;
//import java.nio.file.Path;
import java.nio.file.Paths;

public class ReduceKernelBuilder extends KernelBuilder
{
	@Override
	protected String getKernelType()
	{
		return "reduce";
	}
	
	@Override
	protected KernelBuilder setUpTemplatePluginMapper()
	{
		return super.setUpTemplatePluginMapper()
					.registerPlugin("<[utility-vars]>", getUtilityVarsPlugin())
					.registerPlugin("<[local-a]>", getLocalAPlugin())
					.registerPlugin("<[local-b]>", getLocalBPlugin())
					.registerPlugin("<[deser-a]>", getDeserilizeLocalAPlugin())
					.registerPlugin("<[deser-b]>", getDeserilizeLocalBPlugin())
					.registerPlugin("<[serialize-to-local]>", getSerializeToLocalPlugin())
					.registerPlugin("<[types-copy]>", getTypesCopyPlugin())
					.registerPlugin("<[local-cache-dim]>", getLocalCacheDimPlugin())
					.registerPlugin("<[user-function]>", PDAKernelBuilderPlugin.USER_FUNCTION);
		
	}
	
	protected IKernelBuilderPlugin getUtilityVarsPlugin()
	{
		return (pKernelBuilder, pCodeBuilder) ->
		{
			ITupleDefinition vTuple = pKernelBuilder.getKernelBuilderOptions().getInputTuple();
			ITupleBytesDimensionGetter vDimensionGetter =
				pKernelBuilder.getKernelBuilderOptions().getTupleBytesDimensionGetter();
			
			pCodeBuilder
				.append(" utility variables\n")
				.append("\tunsigned char _arity = ").append(vTuple.getArity())
				.append(";\n")
				.append("\tuint _roff = ").append(vTuple.getArity() + 1).append(";\n")
				.append("\tuint _otd = ").append(vDimensionGetter.getTupleDimension(vTuple)).append(";\n");
		};
		
	}
	
	protected IKernelBuilderPlugin getLocalAPlugin()
	{
		return new LocalAPlugin();
	}
	
	protected IKernelBuilderPlugin getLocalBPlugin()
	{
		return  new LocalBPlugin();
	}
	
	protected IKernelBuilderPlugin getDeserilizeLocalAPlugin()
	{
		return new DeserializationAPlugin();
	}
	
	protected IKernelBuilderPlugin getDeserilizeLocalBPlugin()
	{
		return new DeserializationBPlugin();
	}
	
	protected IKernelBuilderPlugin getSerializeToLocalPlugin()
	{
		return new SerializeToLocalPlugin();
	}
	
	protected IKernelBuilderPlugin getLocalCacheDimPlugin()
	{
		return (pKernelBuilder, pCodeBuilder) ->
		{
			IUserFunction vFunction = pKernelBuilder.getKernelBuilderOptions().getUserFunction();
			ITupleDefinition vTuple = pKernelBuilder.getKernelBuilderOptions().getInputTuple();
			ITupleBytesDimensionGetter vDimensionGetter =
				pKernelBuilder.getKernelBuilderOptions().getTupleBytesDimensionGetter();
			
			pCodeBuilder.append(vFunction.getWorkGroupSize() * vDimensionGetter.getTupleDimension(vTuple));
		};
	}
	
	protected IKernelBuilderPlugin getTypesCopyPlugin()
	{
		return (pKernelBuilder, pCodeBuilder) ->
		{
			pCodeBuilder.append(" types copy\n");
			
			ITupleDefinition vTuple = pKernelBuilder.getKernelBuilderOptions().getInputTuple();
			
			pCodeBuilder
				.append("\t\tunsigned char _types[")
				.append(vTuple.getArity())
				.append("];\n");
			
			vTuple.forEach(pVarDef ->
							   pCodeBuilder
								   .append("\t\t_types[").append(pVarDef.getIndex()).append("] = ")
								   .append("_data[").append(pVarDef.getIndex() + 1).append("];\n")
						  );
		};
	}
}
