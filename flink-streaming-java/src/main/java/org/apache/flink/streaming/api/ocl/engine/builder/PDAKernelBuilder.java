package org.apache.flink.streaming.api.ocl.engine.builder;

import org.apache.flink.streaming.api.ocl.common.mappers.StringKeyMapper;
import org.apache.flink.streaming.api.ocl.engine.OclKernel;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.TemplatePluginMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.PDAKernelBuilderPlugin;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class PDAKernelBuilder implements IPDAKernelBuilder
{
	public static final String DEFAULT_ROOT_TEMPLATE = "\n" +
											   "<[helper-functions]>\n" +
											   "\n" +
											   "<[defines]>\n" +
											   "\n" +
											   "__kernel void <[kernel-name]>(\n" +
											   "    <[kernel-args]>)\n" +
											   "{\n" +
											   "    <[kernel-code]>\n" +
											   "}";
	
	public String getKernelNameTemplate()
	{
		return "<[kernel-name]>";
	}
	public String getKernelCodeTemplate()
	{
		return "<[kernel-code]>";
	}
	public String getHelperFunctionsTemplate()
	{
		return "<[helper-functions]>";
	}
	public String getDefinesTemplate()
	{
		return "<[defines]>";
	}
	public String getKernelArgsTemplate()
	{
		return "<[kernel-args]>";
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
	
	private final String mRootTemplate;
	private final TemplatePluginMapper mTemplatePluginMapper;
	private PDAKernelBuilderOptions mPDAKernelBuilderOptions;
	private StringKeyMapper<Object> mExtras;
	
	public PDAKernelBuilder()
	{
		this(DEFAULT_ROOT_TEMPLATE);
	}
	
	public PDAKernelBuilder(String pRootTemplate)
	{
		this(pRootTemplate, new TemplatePluginMapper());
	}
	
	public PDAKernelBuilder(String pRootTemplate, TemplatePluginMapper pTemplatePluginMapper)
	{
		if(pRootTemplate == null)
		{
			throw new IllegalArgumentException("The template can't be null");
		}
		mRootTemplate = pRootTemplate;
		mTemplatePluginMapper = pTemplatePluginMapper;
		mExtras = new StringKeyMapper<>();
		
		this.setUpTemplatePluginMapper();
	}
	
	protected PDAKernelBuilder setUpExtras()
	{
		return this;
	}
	
	protected PDAKernelBuilder setUpTemplatePluginMapper()
	{
		return this.registerPlugin(getKernelNameTemplate(), getKernelNamePlugin())
				   .registerPlugin(getKernelCodeTemplate(), getKernelCodePlugin())
				   .registerPlugin(getHelperFunctionsTemplate(), getHelperFunctionsPlugin())
				   .registerPlugin(getDefinesTemplate(), getDefinesPlugin())
				   .registerPlugin(getKernelArgsTemplate(), getKernelArgsPlugin());
	}
	
	protected IPDAKernelBuilderPlugin getKernelNamePlugin()
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
	
	public PDAKernelBuilder setExtra(String pKey, Object pExtra)
	{
		getExtrasContainer().register(pKey, pExtra);
		return this;
	}
	
	public PDAKernelBuilder clearExtras()
	{
		getExtrasContainer().clear();
		return this;
	}
	
	protected abstract IPDAKernelBuilderPlugin getKernelCodePlugin();
	protected IPDAKernelBuilderPlugin getHelperFunctionsPlugin()
	{
		return PDAKernelBuilderPlugin.HELPER_FUNCTIONS;
	}
	protected IPDAKernelBuilderPlugin getDefinesPlugin()
	{
		return PDAKernelBuilderPlugin.DEFINES;
	}
	protected IPDAKernelBuilderPlugin getKernelArgsPlugin()
	{
		return PDAKernelBuilderPlugin.KERNEL_ARGS;
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
	
	public PDAKernelBuilderOptions getKernelBuilderOptions()
	{
		return mPDAKernelBuilderOptions;
	}
	public IPDAKernelBuilder setPDAKernelBuilderOptions(PDAKernelBuilderOptions pPDAKernelBuilderOptions)
	{
		mPDAKernelBuilderOptions = pPDAKernelBuilderOptions;
		return clearExtras()
			.setUpExtras();
	}
	
	public TemplatePluginMapper getTemplatePluginMapper()
	{
		return mTemplatePluginMapper;
	}
	public PDAKernelBuilder registerPlugin(String pTemplate, IPDAKernelBuilderPlugin pPlugin)
	{
		getTemplatePluginMapper().register(pTemplate, pPlugin);
		return this;
	}
	
	@Override
	public OclKernel build()
	{
		checkKernelBuilderOptions();
		return new OclKernel(getKernelName(), parseTemplateCode(getRootTemplateCode()));
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
