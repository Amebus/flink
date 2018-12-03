package org.apache.flink.configuration;

import java.io.Serializable;

public class TupleVarDefinition implements Serializable
{
	private JavaTType mJavaTType;
	private CTType mCTType;
	private Object mIdentityValue;
	private transient int mHashCode;
	
	public TupleVarDefinition(String pVarType)
	{
		setInternalValues(pVarType, null);
	}
	
	public TupleVarDefinition(String pVarType, String pIdentityValue)
	{
		setInternalValues(pVarType, pIdentityValue);
	}
	
	public TupleVarDefinition(TupleVarDefinition pVarDefinition)
	{
		Object vIdentityValue = pVarDefinition.getIdentityValue();
		
		if (vIdentityValue == null)
			setInternalValues(pVarDefinition.getJavaT(), null);
		else
			setInternalValues(pVarDefinition.getJavaT(), vIdentityValue.toString());
	}
	
	/**
	 * Return the Java equivalent type
	 * @return String representing the Java type name
	 */
	public JavaTType getJavaT()
	{
		return mJavaTType;
	}
	
	/**
	 * Return the C equivalent type
	 * @return String representing the C type name
	 */
	public CTType getCT()
	{
		return mCTType;
	}
	
	public Object getIdentityValue()
	{
		return mIdentityValue;
	}
	
	public boolean isWithIdentityValue()
	{
		return mIdentityValue != null;
	}
	
	private void setInternalValues(String pVarType, String pIdentityValue)
	{
		setInternalValues(new CTType.Builder(pVarType).build(), pIdentityValue);
	}
	
	private void setInternalValues (TType pType, String pIdentityValue)
	{
		mCTType = new CTType.Builder(pType).build();
		mJavaTType = new JavaTType.Builder(pType).build();
		mHashCode = getJavaT().hashCode();
		setIdentityValue(pIdentityValue);
	}
	
	private void setIdentityValue(String pIdentityValue)
	{
		if (pIdentityValue == null)
			return;
		
		if(mCTType.isString())
		{
			mIdentityValue = pIdentityValue;
		}
		else if(mCTType.isInteger())
		{
			mIdentityValue = Integer.valueOf(pIdentityValue);
		}
		else if (mCTType.isDouble())
		{
			mIdentityValue = Double.valueOf(pIdentityValue);
		}
	}
	
	@Override
	public int hashCode()
	{
		return mHashCode;
	}
	
	@Override
	public boolean equals(Object pOther)
	{
		if (pOther == null)
		{
			return false;
		}
		if (pOther == this)
		{
			return true;
		}
		if (!(pOther instanceof TupleVarDefinition))
		{
			return false;
		}
		TupleVarDefinition rhs = ((TupleVarDefinition) pOther);
		
		boolean vResult = getJavaT().equals(rhs.getJavaT()) &&
						  isWithIdentityValue() == rhs.isWithIdentityValue();
		
		if(vResult && isWithIdentityValue())
		{
			vResult = (getIdentityValue().equals(rhs.getIdentityValue()));
		}
		return vResult;
	}
}
