package org.apache.flink.configuration;

import java.util.Iterator;

public abstract class AbstractTupleDefinition implements ITupleDefinition
{
	protected boolean mWasMaxDimensionCompute = false;
	protected int mMaxDimension = 0;
	
	@Override
	public int getMaxDimension()
	{
		if (!mWasMaxDimensionCompute)
		{
			mMaxDimension = ITupleDefinition.super.getMaxDimension();
			mWasMaxDimensionCompute = true;
		}
		return mMaxDimension;
	}
	
	@Override
	public Iterator<TType> iterator()
	{
		return new JavaTIterator();
	}
	
	public Iterator<TType> reverseIterator ()
	{
		return new JavaTReverseIterator();
	}
	
	@Override
	public Iterator<TType> cIterator ()
	{
		return new CTIterator();
	}
	
	public Iterator<TType> cReverseIterator ()
	{
		return new CTReverseIterator();
	}
	
	public class JavaTIterator extends ForwardTIterator
	{
		@Override
		public TType next()
		{
			return getJavaT(advanceCurrentIndex());
		}
	}
	
	public class JavaTReverseIterator extends ReverseTIterator
	{
		@Override
		public TType next()
		{
			return getJavaT(advanceCurrentIndex());
		}
	}
	
	public class CTIterator extends ForwardTIterator
	{
		@Override
		public TType next()
		{
			return getCT(advanceCurrentIndex());
		}
	}
	
	public class CTReverseIterator extends ReverseTIterator
	{
		@Override
		public TType next()
		{
			return getCT(advanceCurrentIndex());
		}
	}
	
	public abstract class TIterator implements Iterator<TType>
	{
		protected abstract int advanceCurrentIndex();
	}
	
	public abstract class ForwardTIterator extends TIterator
	{
		private int mCurrentIndex;
		private ForwardTIterator()
		{
			mCurrentIndex = 0;
		}
		
		protected int advanceCurrentIndex()
		{
			return mCurrentIndex++;
		}
		
		@Override
		public boolean hasNext()
		{
			return mCurrentIndex < getArity() && mCurrentIndex < T_LIMIT;
		}
	}
	
	public abstract class ReverseTIterator extends TIterator
	{
		private int mCurrentIndex;
		
		private ReverseTIterator()
		{
			mCurrentIndex = getArity() - 1;
		}
		
		protected int advanceCurrentIndex()
		{
			return mCurrentIndex--;
		}
		
		@Override
		public boolean hasNext()
		{
			return mCurrentIndex >= 0;
		}
	}
}
