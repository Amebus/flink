package org.apache.flink.api.bridge;

import org.apache.flink.api.common.IBuilder;

public class OutputTupleInfo
{
	
	private byte[] mBytes;
	
	protected OutputTupleInfo(byte[] pOutputTupleInfo)
	{
		mBytes = pOutputTupleInfo;
	}
	
	public byte[] toJniCompatibleFormat()
	{
		return mBytes;
	}
	
	public static class OutputTupleInfoBuilder implements IBuilder<OutputTupleInfo>
	{
		private byte mArity;
		private byte[] mInfo;
		private int mIndex;
		
		public OutputTupleInfoBuilder()
		{
			mArity = -1;
			mIndex = -1;
		}
		
		public OutputTupleInfoBuilder setArity(byte pArity)
		{
			mArity = pArity;
			mInfo = new byte[1 + mArity];
			mInfo[0] = pArity;
			mIndex = 0;
			return this;
		}
		
		public OutputTupleInfoBuilder setTType(byte pTType)
		{
			return setTType(mIndex++, pTType);
		}
		
		public OutputTupleInfoBuilder setTType(int pTIndex, byte pTType)
		{
			if(pTIndex > mArity)
			{
				throw new IllegalArgumentException("The specified index (" + pTIndex +") is greater or equal" +
												   "to the specified arity (" + mArity + ")");
			}
			else if(pTIndex < 0)
			{
				throw new IllegalArgumentException("The specified index (" + pTIndex +") must be greater than zero");
			}
			mInfo[pTIndex+1] = pTType;
			return this;
		}
		
		@Override
		public OutputTupleInfo build()
		{
			return new OutputTupleInfo(mInfo.clone());
		}
	}
}
