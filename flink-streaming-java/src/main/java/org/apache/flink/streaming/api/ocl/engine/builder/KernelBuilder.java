package org.apache.flink.streaming.api.ocl.engine.builder;

import org.apache.flink.streaming.api.ocl.common.mappers.StringKeyMapper;
import org.apache.flink.streaming.api.ocl.engine.OclKernel;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.TemplatePluginMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.PDAKernelBuilderPlugin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class KernelBuilder implements IKernelBuilder
{
	
	public String getKernelNameTemplate()
	{
		return "<[kernel-name]>";
	}
	
	private Pattern mTemplatePattern;
	public String getTemplateStringPattern()
	{
		return "<\\[.+?]>";
	}
	public Pattern getTemplatePattern()
	{
		if(mTemplatePattern == null)
			mTemplatePattern = Pattern.compile(getTemplateStringPattern());
		return mTemplatePattern;
	}
	
	private String mRootTemplate;
	private final TemplatePluginMapper mTemplatePluginMapper;
	private KernelBuilderOptions mKernelBuilderOptions;
	private StringKeyMapper<Object> mExtras;
	
	protected abstract String getKernelType();
	public KernelBuilder()
	{
		mTemplatePluginMapper = new TemplatePluginMapper();
		mExtras = new StringKeyMapper<>();
		
		this.setUpTemplatePluginMapper();
	}
	
	protected KernelBuilder setUpExtras()
	{
		return this;
	}
	
	protected KernelBuilder setUpTemplatePluginMapper()
	{
		return this.registerPlugin(getKernelNameTemplate(), getKernelNamePlugin());
	}
	
	protected IKernelBuilderPlugin getKernelNamePlugin()
	{
		return (pBuilder, pCodeBuilder)
			-> pCodeBuilder.append(pBuilder.getKernelName());
	}
	
	protected StringKeyMapper<Object> getExtrasContainer()
	{
		return mExtras;
	}
	
	@SuppressWarnings("unchecked")
	public <T> T getExtra(String pKey)
	{
		return (T) getExtrasContainer().resolve(pKey);
	}
	
	@SuppressWarnings("unchecked")
	public <T> T removeExtra(String pKey)
	{
		return (T) getExtrasContainer().unregister(pKey);
	}
	
	public KernelBuilder setExtra(String pKey, Object pExtra)
	{
		getExtrasContainer().register(pKey, pExtra);
		return this;
	}
	
	public KernelBuilder clearExtras()
	{
		getExtrasContainer().clear();
		return this;
	}
	
	public String getKernelName()
	{
		return getKernelBuilderOptions().getUserFunction().getName();
	}
	
	public String getRootTemplateCode()
	{
		return mRootTemplate;
	}
	
	public String getTemplateCode(String pTemplate)
	{
		StringBuilder vBuilder = new StringBuilder();
		getTemplatePluginMapper()
			.resolve(pTemplate)
			.parseTemplateCode(
				this,
				vBuilder);
		return vBuilder.toString();
	}
	
	public KernelBuilderOptions getKernelBuilderOptions()
	{
		return mKernelBuilderOptions;
	}
	public IKernelBuilder setPDAKernelBuilderOptions(KernelBuilderOptions pKernelBuilderOptions)
	{
		mKernelBuilderOptions = pKernelBuilderOptions;
		
		String vFile = getKernelBuilderOptions()
			.getContextOptions()
			.getKernelSourcePath(getKernelType())
			.replace("reduce", "reduce_step_0");
		// retrieve code from file
		mRootTemplate = getTemplate(vFile);
		
		return clearExtras()
			.setUpExtras();
	}
	
	protected String getTemplate(String pSourcePath)
	{
		StringBuilder vKernelCode = new StringBuilder();
		try
		{
			Files.lines(Paths.get(pSourcePath)).forEach(str -> vKernelCode.append(str).append("\n"));
		}
		catch (IOException pE)
		{
			throw new IllegalArgumentException("Unable to use the file \"" + pSourcePath +"\"", pE);
		}
		String vResult = vKernelCode.toString();
		if (vResult.trim().equals(""))
		{
			throw new IllegalArgumentException("The template can't be empty");
		}
		return vResult;
	}
	
	public TemplatePluginMapper getTemplatePluginMapper()
	{
		return mTemplatePluginMapper;
	}
	public KernelBuilder registerPlugin(String pTemplate, IKernelBuilderPlugin pPlugin)
	{
		getTemplatePluginMapper().register(pTemplate, pPlugin);
		return this;
	}
	
	@Override
	public Iterable<OclKernel> build()
	{
		checkKernelBuilderOptions();
		List<OclKernel> vResult = new ArrayList<>();
		vResult.add(new OclKernel(getKernelName(), parseTemplateCode(getRootTemplateCode())));
		return vResult;
	}
	
	protected String parseTemplateCode(String pTemplateCode)
	{
		StringBuilder vBuilder = new StringBuilder(pTemplateCode);
		Matcher vMatcher = getTemplatePattern().matcher(pTemplateCode);
		
		while (vMatcher.find())
		{
			int vStart = vMatcher.start();
			int vEnd = vMatcher.end();
			String vCode = vMatcher.group();
			String vTemplateCode = getTemplateCode(vCode);
			
			vBuilder.replace(vStart, vEnd, parseTemplateCode(vTemplateCode));
			vMatcher.reset(vBuilder);
		}
		return vBuilder.toString();
	}
	
	protected void checkKernelBuilderOptions()
	{
		if(getKernelBuilderOptions() == null)
		{
			throw new IllegalArgumentException("The KernelBuilderOptions must be set to build the kernel successfully");
		}
	}
	
}
