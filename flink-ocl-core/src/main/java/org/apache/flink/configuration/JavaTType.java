package org.apache.flink.configuration;

public class JavaTType extends TType
{
	
	public static class JavaTypes
	{
		static final String INTEGER = "Integer";
		static final String DOUBLE = "Double";
		static final String STRING = "String";
	}
	
	private JavaTType(String pType, int pByteDimension, int pMaxByteDimension)
	{
		super(pType, pByteDimension, pMaxByteDimension);
	}
	
	@Override
	public boolean isInteger()
	{
		return getT().equals(JavaTypes.INTEGER);
	}
	
	@Override
	public boolean isDouble()
	{
		return getT().equals(JavaTypes.DOUBLE);
	}
	
	@Override
	public boolean isString()
	{
		return getT().equals(JavaTypes.STRING);
	}
	
	@Override
	public boolean isKnown()
	{
		return isInteger() || isDouble() || isString();
	}
	
	public static class Builder extends TTypeBuilder
	{
		public Builder(String pType)
		{
			super(pType);
		}
		
		public Builder(TType pType)
		{
			super(pType);
		}
		
		public JavaTType build()
		{
			String vType;
			if (isInteger(getType()))
				vType = JavaTypes.INTEGER;
			else if (isDouble(getType()))
				vType = JavaTypes.DOUBLE;
			else
				vType = JavaTypes.STRING;
			return new JavaTType(vType, getByteOccupation(), getMaxByteOccupation());
		}
	}
}
