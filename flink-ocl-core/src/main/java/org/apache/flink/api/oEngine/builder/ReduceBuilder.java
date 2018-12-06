package org.apache.flink.api.oEngine.builder;

public class ReduceBuilder extends KernelWithoutOutputTupleBuilder
{
	public static final String LOCAL_CACHE = "_localCache";
	
	public ReduceBuilder(KernelBuilderOptions pKernelBuilderOptions)
	{
		super(pKernelBuilderOptions);
	}
	
	@Override
	protected String getKernelSignature()
	{
		String vSignature = super.getKernelSignature();
		
		vSignature = vSignature.substring(0, vSignature.length() - 1);
		
		return vSignature + ",\n" +
			   "\t__local unsigned char *" + LOCAL_CACHE + ")";
	}
}
