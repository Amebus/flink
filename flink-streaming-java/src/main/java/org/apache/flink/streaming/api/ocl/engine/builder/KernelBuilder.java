package org.apache.flink.streaming.api.ocl.engine.builder;

import org.apache.flink.streaming.api.ocl.common.mappers.StringKeyMapper;
import org.apache.flink.streaming.api.ocl.engine.OclKernel;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.TemplatePluginMapper;
import org.apache.flink.streaming.api.ocl.engine.builder.plugins.PDAKernelBuilderPlugin;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class KernelBuilder implements IKernelBuilder
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
	private KernelBuilderOptions mKernelBuilderOptions;
	private StringKeyMapper<Object> mExtras;
	
	public KernelBuilder()
	{
		this(DEFAULT_ROOT_TEMPLATE);
	}
	
	public KernelBuilder(String pRootTemplate)
	{
		this(pRootTemplate, new TemplatePluginMapper());
	}
	
	public KernelBuilder(String pRootTemplate, TemplatePluginMapper pTemplatePluginMapper)
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
	
	protected KernelBuilder setUpExtras()
	{
		return this;
	}
	
	protected KernelBuilder setUpTemplatePluginMapper()
	{
		return this.registerPlugin(getKernelNameTemplate(), getKernelNamePlugin())
				   .registerPlugin(getKernelCodeTemplate(), getKernelCodePlugin())
				   .registerPlugin(getHelperFunctionsTemplate(), getHelperFunctionsPlugin())
				   .registerPlugin(getDefinesTemplate(), getDefinesPlugin())
				   .registerPlugin(getKernelArgsTemplate(), getKernelArgsPlugin());
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
	
	protected abstract IKernelBuilderPlugin getKernelCodePlugin();
	protected IKernelBuilderPlugin getHelperFunctionsPlugin()
	{
		return PDAKernelBuilderPlugin.HELPER_FUNCTIONS;
	}
	protected IKernelBuilderPlugin getDefinesPlugin()
	{
		return PDAKernelBuilderPlugin.DEFINES;
	}
	protected IKernelBuilderPlugin getKernelArgsPlugin()
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
	
	public KernelBuilderOptions getKernelBuilderOptions()
	{
		return mKernelBuilderOptions;
	}
	public IKernelBuilder setPDAKernelBuilderOptions(KernelBuilderOptions pKernelBuilderOptions)
	{
		mKernelBuilderOptions = pKernelBuilderOptions;
		return clearExtras()
			.setUpExtras();
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
