package org.apache.flink.api.tuple;

import org.apache.flink.api.java.tuple.Tuple0;

public class Tuple0Ocl extends Tuple0 implements IOclTuple
{
	@Override
	public byte getArityOcl()
	{
		return 0;
	}
	
	@Override
	public Object getFieldOcl(int pos)
	{
		return getField(pos);
	}
	
	/**
	 * Deep equality for tuples by calling equals() on the tuple members.
	 *
	 * @param pOther the object checked for equality
	 * @return true if this is equal to o.
	 */
	@Override
	public boolean equals(Object pOther)
	{
		return pOther != null &&
			   (pOther == this || pOther instanceof IOclTuple && equals((IOclTuple) pOther));
	}
}
