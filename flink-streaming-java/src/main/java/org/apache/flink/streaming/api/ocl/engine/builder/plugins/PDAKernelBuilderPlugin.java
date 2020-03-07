package org.apache.flink.streaming.api.ocl.engine.builder.plugins;

import org.apache.flink.streaming.api.ocl.engine.builder.FIGenerateExtra;
import org.apache.flink.streaming.api.ocl.engine.builder.IKernelBuilderPlugin;
import org.apache.flink.streaming.api.ocl.engine.builder.KernelBuilder;
import org.apache.flink.streaming.api.ocl.engine.builder.KernelBuilderOptions;

import static org.apache.flink.streaming.api.ocl.common.utility.IterableHelper.getIterableFromArgs;
import static org.apache.flink.streaming.api.ocl.common.utility.IterableHelper.getStringIterableFromArgs;
import static org.apache.flink.streaming.api.ocl.common.utility.StreamUtility.streamFrom;

public abstract class PDAKernelBuilderPlugin implements IPluginWithExtra
{
	private KernelBuilder mKernelBuilder;
	private StringBuilder mCodeBuilder;
	
	public KernelBuilder getKernelBuilder()
	{
		return mKernelBuilder;
	}
	
	public KernelBuilderOptions getOptions()
	{
		return getKernelBuilder().getKernelBuilderOptions();
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
	public IPluginWithExtra setExtra(String pKey, Object pExtra)
	{
		getKernelBuilder().setExtra(pKey, pExtra);
		return this;
	}
	public <T> T removeExtra(String pKey)
	{
		return getKernelBuilder().removeExtra(pKey);
	}
	
	public PDAKernelBuilderPlugin setKernelBuilder(KernelBuilder pKernelBuilder)
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
		KernelBuilder pKernelBuilder,
		StringBuilder pCodeBuilder)
	{
		return setKernelBuilder(pKernelBuilder)
			.setCodeBuilder(pCodeBuilder);
	}
	
	
	public static IKernelBuilderPlugin USER_FUNCTION =
		(pBuilder, pCodeBuilder) ->
			pCodeBuilder
				.append("\n")
				.append("// user function\n")
				.append(pBuilder.getKernelBuilderOptions().getUserFunction().getFunction())
				.append("\n");
	
	public static final class Defaults
	{
		private Defaults() { }
		
		public static Iterable<String> getDefaultKernelParameterList()
		{
			return getStringIterableFromArgs("__global unsigned char *_data",
											 "__global int *_dataIndexes",
											 "__global unsigned char *_result");
		}
		
		public static final class VarTypes
		{
			public static final String INT = "int";
			public static final String STRING = "char";
			public static final String DOUBLE = "double";
			public static final String BOOLEAN = "unsigned char";
		}
		
		public static final class LogicalVarTypes
		{
			public static final String INT = "int";
			public static final String STRING = "string";
			public static final String DOUBLE = "double";
			public static final String BOOLEAN = "unsigned char";
		}
		
		public static final class FunctionNames
		{
			//Transformations
			public static final String MAP = "map";
			public static final String FILTER = "filter";
			
			//Actions
			public static final String REDUCE = "reduce";
		}
	}
}
