package org.apache.flink.configuration;

public class CTType extends TType
{
	public static class CTypes
	{
		public static final String INTEGER = "int";
		public static final String DOUBLE = "double";
		public static final String STRING = "char*";
	}
	
	private CTType(String pType, int pByteDimension, int pMaxByteDimension)
	{
		super(pType, pByteDimension, pMaxByteDimension);
	}
	
	@Override
	public boolean isInteger()
	{
		return getT().equals(CTypes.INTEGER);
	}
	
	@Override
	public boolean isDouble()
	{
		return getT().equals(CTypes.DOUBLE);
	}
	
	@Override
	public boolean isString()
	{
		return getT().equals(CTypes.STRING);
	}
	
	@Override
	public boolean isUnknown()
	{
		return !(isInteger() || isDouble() || isString());
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
		
		public CTType build()
		{
			String vType;
			if (isInteger(getType()))
				vType = CTypes.INTEGER;
			else if (isDouble(getType()))
				vType = CTypes.DOUBLE;
			else
				vType = CTypes.STRING;
			return new CTType(vType, getByteDimension(), getMaxByteDimension());
		}
	}
}
