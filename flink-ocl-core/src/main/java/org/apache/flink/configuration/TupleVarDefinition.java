package org.apache.flink.configuration;

public class TupleVarDefinition
{
	private JavaTType mJavaTType;
	private CTType mCTType;
	private transient int mHashCode;
	
	public TupleVarDefinition(String pVarType)
	{
		setInternalValues(pVarType);
	}
	
	public TupleVarDefinition(TupleVarDefinition pVarDefinition)
	{
		setInternalValues(pVarDefinition.getJavaT());
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
	
	private void setInternalValues(String pVarType)
	{
		setInternalValues(new CTType.Builder(pVarType).build());
	}
	
	private void setInternalValues (TType pType)
	{
		mCTType = new CTType.Builder(pType).build();
		mJavaTType = new JavaTType.Builder(pType).build();
		mHashCode = getJavaT().hashCode();
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
		
		return getJavaT().equals(rhs.getJavaT());
	}
}
