package org.apache.flink.streaming.api.ocl.common.profiling;

public class Stopwatch
{
	private long mStartTime;
	private long mStopTime;
	private long mTotalTime;
	private boolean mIsRunning;
	
	public Stopwatch()
	{
		reset();
	}
	
	public long getElapsedNano()
	{
		return mTotalTime;
	}
	
	public long getElapsedMicro()
	{
		return (long)(mTotalTime * 10e-3);
	}
	
	public long getElapsedMilliseconds()
	{
		return (long)(mTotalTime * 10e-6);
	}
	
	public long getElapsedSeconds()
	{
		return (long)(mTotalTime * 10e-9);
	}
	
	public boolean isRunning()
	{
		return mIsRunning;
	}
	
	public void reset()
	{
		mStartTime = 0;
		mStopTime = 0;
		mTotalTime = 0;
		mIsRunning = false;
	}
	
	public void restart()
	{
		reset();
		start();
	}
	
	public void start()
	{
		if (!mIsRunning)
		{
			mStartTime = System.nanoTime();
			mIsRunning = true;
		}
	}
	
	public Stopwatch startNew()
	{
		return new Stopwatch();
	}
	
	public void stop()
	{
		if (mIsRunning)
		{
			mStopTime = System.nanoTime();
			mIsRunning = false;
			mTotalTime += (mStopTime - mStartTime);
		}
	}
}
