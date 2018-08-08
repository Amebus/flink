package org.apache.flink.api.engine.kernel.builder;

public class FilterBuilder extends MapBuilder
{
	public FilterBuilder(KernelBuilderOptions pKernelBuilderOptions)
	{
		super(pKernelBuilderOptions);
	}
	
	@Override
	protected String getOutputVarDeclaration()
	{
		return "unsigned char _r0 = 0;\n";
//		return "";
	}
	
	@Override
	protected String getOutputSection()
	{
		return K_RESULT + " = _r0;";
	}
	
	@Override
	protected String getSerializationMacros()
	{
		return "";
	}
}
