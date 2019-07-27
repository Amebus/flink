package org.apache.flink.streaming.api.ocl.engine.builder;

import org.apache.flink.streaming.api.ocl.engine.builder.mappers.TemplatePluginMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.*;

import java.io.IOException;
import java.nio.file.Files;
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
					.registerPlugin("<[local-a]>", )
					.registerPlugin("<[local-b]>", )
					.registerPlugin("<[deser-a]>", )
					.registerPlugin("<[deser-b]>", )
					.registerPlugin("<[serialize-to-local]>", )
					.registerPlugin("<[user-function]>", PDAKernelBuilderPlugin.USER_FUNCTION);
		
	}
	
	@Override
	protected IPDAKernelBuilderPlugin getKernelCodePlugin()
	{
		return (pBuilder, pCodeBuilder) ->
		{
			String vFile = "$/Documents/reduce.template";
			// retrieve code from file
			try
			{
				Files.lines(Paths.get(vFile)).forEach(pCodeBuilder::append);
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
				.append("\n")
				.append("// utility variables\n")
				.append("uint _roff = 2;\n" + // values to be computed
						"    uint _otd = 4;");
		
	}
}
