package org.apache.flink.api.tuple;

import org.apache.flink.api.java.tuple.Tuple1;

public class Tuple1Ocl<T0> extends Tuple1<T0> implements IOclTuple
{
	public Tuple1Ocl() { super(); }
	
	public Tuple1Ocl(T0 pT0)
	{
		super(pT0);
	}
	
	@Override
	public byte getArityOcl()
	{
		return 1;
	}
	
	@Override
	public Object getFieldOcl(int pos)
	{
		switch(pos) {
			case 0: return this.f0;
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
