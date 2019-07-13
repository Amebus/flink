package org.apache.flink.streaming.api.ocl.engine.builder;

import org.apache.flink.streaming.api.ocl.common.IBuilder;
import org.apache.flink.streaming.api.ocl.common.mappers.StringKeyMapper;
import org.apache.flink.streaming.api.ocl.engine.OclKernel;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.TemplatePluginMapper;
import org.apache.flink.streaming.configuration.ITupleDefinition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.streaming.api.ocl.common.utility.IterableHelper.getIterableFromArgs;
import static org.apache.flink.streaming.api.ocl.engine.builder.options.DefaultsValues.*;

public abstract class PDAKernelBuilder implements IPDAKernelBuilder
{
	public String getKernelNameTemplate()
	{
		return "<[kernel-name-dfp]>";
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
		setUpTemplatePluginMapper();
	}
	
	protected void setUpTemplatePluginMapper()
	{
		this.registerPlugin(getKernelNameTemplate(), getKernelNamePlugin())
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
		return getHelperFunctionsDefaultPlugin();
	}
	protected IPDAKernelBuilderPlugin getDefinesPlugin()
	{
		return getDefinesDefaultPlugin();
	}
	protected IPDAKernelBuilderPlugin getKernelArgsPlugin()
	{
		return getKernelArgsDefaultPlugin();
	}
	
	public String getKernelName()
	{
		return getPDAKernelBuilderOptions().getUserFunction().getName();
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
	
	public PDAKernelBuilderOptions getPDAKernelBuilderOptions()
	{
		return mPDAKernelBuilderOptions;
	}
	public IPDAKernelBuilder setPDAKernelBuilderOptions(PDAKernelBuilderOptions pPDAKernelBuilderOptions)
	{
		mPDAKernelBuilderOptions = pPDAKernelBuilderOptions;
		return this;
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
	
	protected IPDAKernelBuilderPlugin getInputVarsPlugin()
	{
		return new InputVarPlugin();
	}
	
	protected IPDAKernelBuilderPlugin getOutputVarsPlugin()
	{
		return new OutputVarPlugin();
	}
	
	protected IPDAKernelBuilderPlugin getOutputUtilityVarsPlugin()
	{
		return new OutputUtilityVarsPlugin();
	}
	
	protected void checkKernelBuilderOptions()
	{
		if(getPDAKernelBuilderOptions() == null)
		{
			throw new IllegalArgumentException("The KernelBuilderOptions must be set to build the kernel successfully");
		}
	}
	
	public static class KernelLogicalVariable
	{
		private String mVarType;
		private String mVarName;
		private int mIndex;
		private int mBytesDim;
		
		public KernelLogicalVariable(String pVarType, String pVarName, int pIndex)
		{
			this(pVarType, pVarName, pIndex, 0);
		}
		
		public KernelLogicalVariable(String pVarType, String pVarName, int pIndex, int pBytesDim)
		{
			mVarType = pVarType;
			mVarName = pVarName;
			mIndex = pIndex;
			mBytesDim = pBytesDim;
		}
		
		public String getVarType()
		{
			return mVarType;
		}
		
		public String getVarName()
		{
			return mVarName;
		}
		
		public int getIndex()
		{
			return mIndex;
		}
		
		public int getBytesDim()
		{
			return mBytesDim;
		}
		
		public boolean isBytesDimSpecified()
		{
			return mBytesDim > 0;
		}
	}
	
	public static class KernelVariablesLine
	{
		private String mVarType;
		private List<String> mVarDefinition;
		
		public KernelVariablesLine(String pVarType)
		{
			mVarType = pVarType;
			mVarDefinition = new LinkedList<>();
		}
		
		public String getVarType()
		{
			return mVarType;
		}
		
		public Iterable<String> getVarDefinition()
		{
			return mVarDefinition;
		}
		
		public KernelVariablesLine setVarType(String pVarType)
		{
			mVarType = pVarType;
			return this;
		}
		
		public KernelVariablesLine addVarDef(String pVarName)
		{
			mVarDefinition.add(pVarName);
			return this;
		}
	}
	
	public static class KernelDeserializationLine
	{
		private String mDeserLine;
		private int mDeserIndexOrder;
		
		public KernelDeserializationLine(String pDeserLine, int pDeserIndexOrder)
		{
			mDeserLine = pDeserLine;
			mDeserIndexOrder = pDeserIndexOrder;
		}
		
		public String getDeserLine()
		{
			return mDeserLine;
		}
		
		public int getDeserIndexOrder()
		{
			return mDeserIndexOrder;
		}
	}
	
	public static abstract class PDAKernelBuilderPlugin implements IPDAKernelBuilderPlugin
	{
		private PDAKernelBuilder mKernelBuilder;
		private StringBuilder mCodeBuilder;
		
		public PDAKernelBuilder getKernelBuilder()
		{
			return mKernelBuilder;
		}
		
		public PDAKernelBuilderOptions getOptions()
		{
			return getKernelBuilder().getPDAKernelBuilderOptions();
		}
		
		public StringBuilder getCodeBuilder()
		{
			return mCodeBuilder;
		}
		
		public <T> T getExtra(String pKey)
		{
			return getKernelBuilder().getExtra(pKey);
		}
		public <T> T getExtra(String pKey, FIGenerateExtra<T> pGenerateExtra)
		{
			if(pGenerateExtra == null)
				throw new IllegalArgumentException(("can't be null"));
			
			T vResult = getExtra(pKey);
			if(vResult == null)
			{
				vResult = pGenerateExtra.generateExtra();
			}
			setExtra(pKey, vResult);
			return vResult;
		}
		public PDAKernelBuilderPlugin setExtra(String pKey, Object pExtra)
		{
			getKernelBuilder().setExtra(pKey, pExtra);
			return this;
		}
		public <T> T removeExtra(String pKey)
		{
			return getKernelBuilder().removeExtra(pKey);
		}
		
		public PDAKernelBuilderPlugin setKernelBuilder(PDAKernelBuilder pKernelBuilder)
		{
			mKernelBuilder = pKernelBuilder;
			return this;
		}
		
		
		public PDAKernelBuilderPlugin setCodeBuilder(StringBuilder pCodeBuilder)
		{
			mCodeBuilder = pCodeBuilder;
			return this;
		}
		
		public PDAKernelBuilderPlugin setKernelAndCodeBuilder(
			PDAKernelBuilder pKernelBuilder,
			StringBuilder pCodeBuilder)
		{
			return setKernelBuilder(pKernelBuilder)
				.setCodeBuilder(pCodeBuilder);
		}
		
		protected String getIntType()
		{
			return DefaultVarTypes.INT;
		}
		protected String getDoubleType()
		{
			return DefaultVarTypes.DOUBLE;
		}
		protected String getStringType()
		{
			return DefaultVarTypes.STRING;
		}
		
		protected Iterable<String> getTypes()
		{
			return getIterableFromArgs(
				getIntType(),
				getDoubleType(),
				getStringType());
		}
		
		@FunctionalInterface
		public interface FIGenerateExtra<T>
		{
			T generateExtra();
		}
		
	}
	
	public static class InputVarPlugin extends PDAKernelBuilderPlugin
	{
		
		protected String getInputLogicalVarsKey()
		{
			return "input-logical-vars";
		}
		protected String getInputLinesKey()
		{
			return "input-lines";
		}
		
		protected List<KernelLogicalVariable> getKernelLogicalVariables()
		{
			return getExtra(getInputLogicalVarsKey(), () ->
			{
				ITupleDefinition vTuple = getOptions().getInputTuple();
				List<KernelLogicalVariable> vResult = new ArrayList<>(vTuple.getArity());
				vTuple.forEach(pVar ->
							   {
								   String vName = "_t" + pVar.getIndex();
								   String vType = pVar.getType().toLowerCase();
					
								   if(vType.startsWith("i"))
								   {
									   vType = getIntType();
								   }
								   else if(vType.startsWith("d"))
								   {
									   vType = getDoubleType();
								   }
								   else if(vType.startsWith("s"))
								   {
									   vType = getStringType();
								   }
					
								   vResult.add(
									   new KernelLogicalVariable(
										   vType,
										   vName,
										   pVar.getIndex(),
										   pVar.getMaxReservedBytes()));
							   });
				return vResult;
			});
		}
		
		protected KernelVariablesLine[] getInputLines()
		{
			return getExtra(getInputLinesKey(),
							() -> new KernelVariablesLine[] {
								new KernelVariablesLine(DefaultVarTypes.INT),
								new KernelVariablesLine(DefaultVarTypes.DOUBLE),
								new KernelVariablesLine(DefaultVarTypes.STRING),
								new KernelVariablesLine(DefaultVarTypes.INT)
							});
		}
		
		protected void setInputLines()
		{
			getKernelLogicalVariables()
				.forEach(pVar ->
						 {
							 String vVarType = pVar.getVarType();
							 String vVarName = pVar.getVarName();
							 int vIndex = 0;
							 if (vVarType.equals(DefaultVarTypes.DOUBLE))
							 {
								 vIndex = 1;
							 }
							 else if(vVarType.equals(DefaultVarTypes.STRING))
							 {
								 vIndex = 2;
								 getInputLines()[3].addVarDef("_tsl" + pVar.getIndex());
							 }
							 getInputLines()[vIndex].addVarDef(vVarName);
						 });
		}
		
		protected void codeFromInputLines()
		{
			StringBuilder vCodeBuilder = getCodeBuilder();
			for (KernelVariablesLine vLine : getInputLines())
			{
				Iterator<String> vIterator = vLine.getVarDefinition().iterator();
				
				if(!vIterator.hasNext())
				{
					continue;
				}
				
				String vType = getExtra("input-var-" + vLine.getVarType());
				
				vCodeBuilder.append(vType)
							.append(" ");
				
				String vVarDef;
				
				while (vIterator.hasNext())
				{
					vVarDef = vIterator.next();
					
					vCodeBuilder.append(vVarDef);
					
					if(vIterator.hasNext())
					{
						vCodeBuilder.append(",");
					}
					else
					{
						vCodeBuilder.append(";\n");
					}
				}
				vCodeBuilder.append("\n");
			}
		}
		
		@Override
		public void parseTemplateCode(PDAKernelBuilder pKernelBuilder, StringBuilder pCodeBuilder)
		{
			setKernelAndCodeBuilder(pKernelBuilder, pCodeBuilder);
			
			pCodeBuilder
				.append("\n")
				.append("// input-tuple")
				.append("\n");
			
			setInputLines();
			
			codeFromInputLines();
		}
	}
	
	public static class OutputVarPlugin extends PDAKernelBuilderPlugin
	{
		protected String getOutputLogicalVarsKey()
		{
			return "output-logical-vars";
		}
		protected String getOutputLinesKey()
		{
			return "output-lines";
		}
		
		protected List<KernelLogicalVariable> getKernelLogicalVariables()
		{
			return getExtra(getOutputLogicalVarsKey(), () ->
			{
				ITupleDefinition vTuple = getOptions().getOutputTuple();
				List<KernelLogicalVariable> vResult = new ArrayList<>(vTuple.getArity());
				
				vTuple
					.forEach(vVar ->
							 {
								 String vName = "_r" + vVar.getIndex();
								 String vType = vVar.getType().toLowerCase();
								
								 if(vType.startsWith("i"))
								 {
									 vType = DefaultVarTypes.INT;
								 }
								 else if(vType.startsWith("d"))
								 {
									 vType = DefaultVarTypes.DOUBLE;
								 }
								 else if(vType.startsWith("s"))
								 {
									 vType = DefaultVarTypes.STRING;
								 }
								 vResult.add(
								 	new KernelLogicalVariable(vType, vName, vVar.getIndex(), vVar.getMaxReservedBytes()));
							 });
				
				return vResult;
			});
		}
		
		protected KernelVariablesLine[] getOutputLines()
		{
			return getExtra(getOutputLinesKey(),
							() -> new KernelVariablesLine[] {
								new KernelVariablesLine(DefaultVarTypes.INT),
								new KernelVariablesLine(DefaultVarTypes.DOUBLE),
								new KernelVariablesLine(DefaultVarTypes.STRING),
								new KernelVariablesLine(DefaultVarTypes.INT)
							});
		}
		
		protected void setOutputLines()
		{
			getKernelLogicalVariables()
				.forEach(pVar ->
						 {
							 String vVarType = pVar.getVarType();
							 String vVarName = pVar.getVarName();
							 int vIndex = 0;
							 if (vVarType.equals(DefaultVarTypes.DOUBLE))
							 {
								 vIndex = 1;
							 }
							 else if(vVarType.equals(DefaultVarTypes.STRING))
							 {
								 vIndex = 2;
								 getOutputLines()[3].addVarDef("_rsl" + pVar.getIndex());
								 vVarName += "[" + pVar.getBytesDim() + "]";
							 }
							 getOutputLines()[vIndex].addVarDef(vVarName);
						 });
		}
		
		protected void codeFromOutputLines()
		{
			StringBuilder vCodeBuilder = getCodeBuilder();
			for (KernelVariablesLine vLine : getOutputLines())
			{
				Iterator<String> vIterator = vLine.getVarDefinition().iterator();
				
				if(!vIterator.hasNext())
				{
					continue;
				}
				
				String vType = getExtra("output-var-" + vLine.getVarType());
				
				vCodeBuilder.append(vType)
							.append(" ");
				
				String vVarDef;
				
				while (vIterator.hasNext())
				{
					vVarDef = vIterator.next();
					
					vCodeBuilder.append(vVarDef);
					
					if(vIterator.hasNext())
					{
						vCodeBuilder.append(",");
					}
					else
					{
						vCodeBuilder.append(";\n");
					}
				}
				vCodeBuilder.append("\n");
			}
		}
		
		@Override
		public void parseTemplateCode(PDAKernelBuilder pKernelBuilder, StringBuilder pCodeBuilder)
		{
			setKernelAndCodeBuilder(pKernelBuilder, pCodeBuilder);
			
			pCodeBuilder
				.append("\n")
				.append("// output-tuple")
				.append("\n");
			
			setOutputLines();
			
			codeFromOutputLines();
		}
	}
	
	public static class OutputUtilityVarsPlugin extends PDAKernelBuilderPlugin
	{
		protected ITupleDefinition getOutputTuple()
		{
			return getKernelBuilder().getPDAKernelBuilderOptions().getOutputTuple();
		}
		
		protected byte getOutputOffset()
		{
			byte vOffset = 0x1;
			vOffset += getOutputTuple().getArity();
			return vOffset;
		}
		
		protected int getOutputTupleDimension()
		{
			final int[] vResult = {0};
			getOutputTuple()
				.forEach( pVar ->
						  {
							  vResult[0] +=pVar.getMaxReservedBytes();
							  if(pVar.getType().startsWith("s"))
							  {
								  vResult[0]+=4;
							  }
						  });
			return vResult[0];
		}
		
		@Override
		public void parseTemplateCode(PDAKernelBuilder pKernelBuilder, StringBuilder pCodeBuilder)
		{
			setKernelAndCodeBuilder(pKernelBuilder, pCodeBuilder);
			
			pCodeBuilder
				.append("\n")
				.append("// output utility vars")
				.append("\n")
				.append("uint _roff = ")
				.append(getOutputOffset())
				.append(";\n")
				.append("uint _otd = ")
				.append(getOutputTupleDimension())
				.append(";\n");
		}
	}
}
