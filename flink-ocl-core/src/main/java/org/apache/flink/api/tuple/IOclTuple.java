package org.apache.flink.api.tuple;

public interface IOclTuple
{
	
	byte getArityOcl();
	
	Object getFieldOcl(int pos);
	
	@SuppressWarnings("unchecked")
	default <T> T getField(int pos)
	{
		return (T) getFieldOcl(pos);
	}
	
	default boolean equals(IOclTuple pOther)
	{
		if (getArityOcl() != pOther.getArityOcl())
		{
			return false;
		}
		
		boolean vResult = true;
		
		for (int i = 0; i < getArityOcl() && vResult; i++)
		{
			vResult &= getField(i).equals(pOther.getField(i));
		}
		
		return vResult;
	}
}
