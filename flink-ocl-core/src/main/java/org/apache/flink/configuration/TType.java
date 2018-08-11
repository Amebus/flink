package org.apache.flink.configuration;

import com.sun.org.apache.xerces.internal.impl.xpath.regex.RegularExpression;

import java.io.Serializable;
import java.util.Objects;

public abstract class TType implements Serializable
{
	
	public static class ConfigTypes
	{
		public static final String INT = "int";
		public static final String INTEGER = "integer";
		public static final String DOUBLE = "double";
		public static final String CSTRING = "char*";
		public static final String STRING = "string";
		
		private static final String REGEX_STRING = "(" + CSTRING + "|" + STRING + ")(" + COLUMN + "\\d+)?";
		static final RegularExpression EXPRESSION = new RegularExpression(REGEX_STRING, "i");
	}
	
	private static final String COLUMN = ":";
	private String mType;
	private int mByteDimension;
	private int mMaxByteDimension;
	
	protected TType(String pType, int pByteDimension, int pMaxByteDimension)
	{
		mType = pType;
		mByteDimension = pByteDimension;
		mMaxByteDimension = pMaxByteDimension;
	}
	
	// protected TType(TType pTType)
	// {
	// 	this(pTType.getT(), pTType.getByteDimension(), pTType.getMaxByteDimension());
	// }
	
	public String getT()
	{
		return mType;
	}
	
	public int getByteDimension()
	{
		return mByteDimension;
	}
	
	public int getMaxByteDimension()
	{
		return mMaxByteDimension;
	}
	
	public abstract boolean isInteger();
	public abstract boolean isDouble();
	public abstract boolean isString();
	public abstract boolean isUnknown();
	
	@Override
	public int hashCode()
	{
		return Objects.hash(getByteDimension(), getMaxByteDimension(), getT());
	}
	
	
	@Override
	public boolean equals(Object pOther)
	{
		return pOther instanceof TType && equals((TType) pOther);
	}
	
	public boolean equals(TType pOther)
	{
		return pOther != null &&
			   (pOther == this ||
				getT().equals(pOther.getT()) &&
				getByteDimension() == pOther.getByteDimension() &&
				getMaxByteDimension() == pOther.getMaxByteDimension());
	}
	
	public static boolean isInteger (String pT)
	{
		if (pT == null)
			return false;
		pT = pT.toLowerCase();
		return pT.equalsIgnoreCase(ConfigTypes.INT) || pT.equalsIgnoreCase(ConfigTypes.INTEGER);
	}
	
	public static boolean isDouble (String pT)
	{
		return pT != null && pT.equalsIgnoreCase(ConfigTypes.DOUBLE);
	}
	
	public static boolean isString (String pT)
	{
		if (pT == null)
			return false;
		String vT = pT.toLowerCase();
		return ConfigTypes.EXPRESSION.matches(vT);
	}
	
	protected static abstract class TTypeBuilder
	{
		private String mType;
		private int mByteDimension;
		private int mMaxByteDimension;
		
		
		public TTypeBuilder(String pType)
		{
			mByteDimension = 0;
			mMaxByteDimension = 100;
			mType = pType;
			setBuildParameters(pType);
		}
		
		public TTypeBuilder(TType pType)
		{
			setBuildParameters(pType);
		}
		
		private void setBuildParameters(TType pType)
		{
			mByteDimension = pType.getByteDimension();
			mMaxByteDimension = pType.getMaxByteDimension();
			mType = pType.getT();
		}
		
		private void setBuildParameters(String pType)
		{
			if (isString(pType))
			{
				asStringType(pType);
			}
			else if (isInteger(pType))
			{
				asIntegerType();
			}
			else if (isDouble(pType))
			{
				asDoubleType();
			}
			else
			{
				mType = "";
				throw new IllegalArgumentException("Error with var type " + pType
												   +". The var type must be one between: int|integer or char|character or double or string");
			}
		}
		
		private void asStringType(String pType)
		{
			String[] vStrings = pType.split(COLUMN);
			mByteDimension = -1;
			
			if (vStrings.length > 1)
			{
				mMaxByteDimension = Integer.parseInt(vStrings[1]);
			}
			mType = vStrings[0];
		}
		
		private void asIntegerType()
		{
			mByteDimension = 4;
			mMaxByteDimension = 4;
		}
		
		private void asDoubleType()
		{
			mByteDimension = 8;
			mMaxByteDimension = 8;
		}
		
		public String getType()
		{
			return mType;
		}
		
		public int getByteDimension()
		{
			return mByteDimension;
		}
		
		public int getMaxByteDimension()
		{
			return mMaxByteDimension;
		}
		
	}
}
