package org.apache.flink.streaming.api.tuple;

import org.apache.flink.api.java.tuple.Tuple3;

public class Tuple3Ocl <T0, T1, T2> extends Tuple3<T0, T1, T2> implements IOclTuple
{
	public Tuple3Ocl()
	{
		super();
	}
	
	public Tuple3Ocl(T0 pT0, T1 pT1, T2 pT2)
	{
		super(pT0, pT1, pT2);
	}
	
	@Override
	public byte getArityOcl()
	{
		return 3;
	}
	
	@Override
	public Object getFieldOcl(int pos)
	{
		switch(pos) {
			case 0: return f0;
			case 1: return f1;
			case 2: return f2;
			default: throw new IndexOutOfBoundsException(String.valueOf(pos));
		}
	}
	
	@Override
	public boolean equals(Object pOther)
	{
		return pOther != null &&
			   (pOther == this || pOther instanceof IOclTuple && equals((IOclTuple) pOther));
	}
}
