package org.apache.flink.streaming.api.ocl.engine.builder;

import org.apache.flink.streaming.api.ocl.engine.builder.mappers.TemplatePluginMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.*;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.reduce.*;
import org.apache.flink.streaming.configuration.ITupleDefinition;

import java.io.IOException;
import java.nio.file.Files;
//import java.nio.file.Path;
import java.nio.file.Paths;

public class ReducePDAKernelBuilder extends PDAKernelBuilder
{
	public static final String ROOT_TEMPLATE = "<[reduce]>";
	
	public ReducePDAKernelBuilder()
	{
		super(ROOT_TEMPLATE);
	}
	
	public ReducePDAKernelBuilder(String pRootTemplate)
	{
		super(pRootTemplate);
	}
	
	public ReducePDAKernelBuilder(String pRootTemplate, TemplatePluginMapper pTemplatePluginMapper)
	{
		super(pRootTemplate, pTemplatePluginMapper);
	}
	
	@Override
	protected PDAKernelBuilder setUpTemplatePluginMapper()
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
	protected IPDAKernelBuilderPlugin getKernelCodePlugin()
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
	
	protected IPDAKernelBuilderPlugin getUtilityVarsPlugin()
	{
		return (pBuilder, pCodeBuilder) ->
			pCodeBuilder
				.append(" utility variables\n")
				.append("\tunsigned char _arity = ").append(pBuilder.getKernelBuilderOptions().getInputTuple().getArity())
				.append(";\n")
				.append("\tuint _roff = 2;\n" + // values to be computed
						"\tuint _otd = 4;\n");
		
	}
	
	protected IPDAKernelBuilderPlugin getLocalAPlugin()
	{
		return new LocalAPlugin();
	}
	
	protected IPDAKernelBuilderPlugin getLocalBPlugin()
	{
		return  new LocalBPlugin();
	}
	
	protected IPDAKernelBuilderPlugin getDeserilizeLocalAPlugin()
	{
		return new DeserializationAPlugin();
	}
	
	protected IPDAKernelBuilderPlugin getDeserilizeLocalBPlugin()
	{
		return new DeserializationBPlugin();
	}
	
	protected IPDAKernelBuilderPlugin getSerializeToLocalPlugin()
	{
		return new SerializeToLocalPlugin();
	}
	
	protected IPDAKernelBuilderPlugin getTypesCopyPlugin()
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
