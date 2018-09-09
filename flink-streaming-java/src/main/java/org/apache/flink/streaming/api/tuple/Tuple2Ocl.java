package org.apache.flink.streaming.api.tuple;

import org.apache.flink.api.java.tuple.Tuple2;

public class Tuple2Ocl<T0, T1> extends Tuple2<T0, T1> implements IOclTuple
{
	public Tuple2Ocl()
	{
		super();
	}
	
	public Tuple2Ocl(T0 pT0, T1 pT1)
	{
		super(pT0, pT1);
	}
	
	@Override
	public byte getArityOcl()
	{
		return 2;
	}
	
	@Override
	public Object getFieldOcl(int pos)
	{
		switch(pos) {
			case 0: return this.f0;
			case 1: return this.f1;
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
