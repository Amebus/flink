package org.apache.flink.streaming.configuration;

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
	private int mByteOccupation;
	private int mMaxByteOccupation;
	
	protected TType(String pType, int pByteOccupation, int pMaxByteOccupation)
	{
		mType = pType;
		mByteOccupation = pByteOccupation;
		mMaxByteOccupation = pMaxByteOccupation;
	}
	
	// protected TType(TType pTType)
	// {
	// 	this(pTType.getT(), pTType.getByteOccupation(), pTType.getMaxByteOccupation());
	// }
	
	public String getT()
	{
		return mType;
	}
	
	public int getByteOccupation()
	{
		return mByteOccupation;
	}
	
	public int getMaxByteOccupation()
	{
		return mMaxByteOccupation;
	}
	
	public abstract boolean isInteger();
	public abstract boolean isDouble();
	public abstract boolean isString();
	public abstract boolean isKnown();
	public boolean isUnknown()
	{
		return !isKnown();
	}
	
	@Override
	public int hashCode()
	{
		return Objects.hash(getByteOccupation(), getMaxByteOccupation(), getT());
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
				getByteOccupation() == pOther.getByteOccupation() &&
				getMaxByteOccupation() == pOther.getMaxByteOccupation());
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
		private int mByteOccupation;
		private int mMaxByteOccupation;
		
		
		public TTypeBuilder(String pType)
		{
			mByteOccupation = 0;
			mMaxByteOccupation = 100;
			mType = pType;
			setBuildParameters(pType);
		}
		
		public TTypeBuilder(TType pType)
		{
			setBuildParameters(pType);
		}
		
		private void setBuildParameters(TType pType)
		{
			mByteOccupation = pType.getByteOccupation();
			mMaxByteOccupation = pType.getMaxByteOccupation();
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
			mByteOccupation = -1;
			
			if (vStrings.length > 1)
			{
				mMaxByteOccupation = Integer.parseInt(vStrings[1]);
			}
			mType = vStrings[0];
		}
		
		private void asIntegerType()
		{
			mByteOccupation = 4;
			mMaxByteOccupation = 4;
		}
		
		private void asDoubleType()
		{
			mByteOccupation = 8;
			mMaxByteOccupation = 8;
		}
		
		public String getType()
		{
			return mType;
		}
		
		public int getByteOccupation()
		{
			return mByteOccupation;
		}
		
		public int getMaxByteOccupation()
		{
			return mMaxByteOccupation;
		}
		
	}
}
