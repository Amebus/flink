package org.apache.flink.streaming.api.ocl.engine.builder;

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
	public static final String ROOT_TEMPLATE = "<[reduce]>";
	
	public ReduceKernelBuilder()
	{
		super(ROOT_TEMPLATE);
	}
	
	public ReduceKernelBuilder(String pRootTemplate)
	{
		super(pRootTemplate);
	}
	
	public ReduceKernelBuilder(String pRootTemplate, TemplatePluginMapper pTemplatePluginMapper)
	{
		super(pRootTemplate, pTemplatePluginMapper);
	}
	
	@Override
	protected KernelBuilder setUpTemplatePluginMapper()
	{
		return super.setUpTemplatePluginMapper()
					.registerPlugin("<[reduce]>", getKernelCodePlugin())
					.registerPlugin("<[utility-vars]>", getUtilityVarsPlugin())
					.registerPlugin("<[local-a]>", getLocalAPlugin())
					.registerPlugin("<[local-b]>", getLocalBPlugin())
					.registerPlugin("<[deser-a]>", getDeserilizeLocalAPlugin())
					.registerPlugin("<[deser-b]>", getDeserilizeLocalBPlugin())
					.registerPlugin("<[serialize-to-local]>", getSerializeToLocalPlugin())
					.registerPlugin("<[types-copy]>", getTypesCopyPlugin())
					.registerPlugin("<[user-function]>", PDAKernelBuilderPlugin.USER_FUNCTION);
		
	}
	
	@Override
	protected IKernelBuilderPlugin getKernelCodePlugin()
	{
		return (pBuilder, pCodeBuilder) ->
		{
			// ~
			String vFile = "../Cpp/Code/Sources/reduce.template";
			// retrieve code from file
			try
			{
//				Path currentRelativePath = Paths.get("");
//				String s = currentRelativePath.toAbsolutePath().toString();
//				System.out.println("Current relative path is: " + s);
				
				Files.lines(Paths.get(vFile)).forEach(str -> pCodeBuilder.append(str).append("\n"));
			}
			catch (IOException pE)
			{
				throw new IllegalArgumentException("Unable to use the file \"" + vFile +"\"", pE);
			}
		};
	}
	
	protected IKernelBuilderPlugin getUtilityVarsPlugin()
	{
		return (pBuilder, pCodeBuilder) ->
			pCodeBuilder
				.append(" utility variables\n")
				.append("\tunsigned char _arity = ").append(pBuilder.getKernelBuilderOptions().getInputTuple().getArity())
				.append(";\n")
				.append("\tuint _roff = 2;\n" + // values to be computed
						"\tuint _otd = 4;\n");
		
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
