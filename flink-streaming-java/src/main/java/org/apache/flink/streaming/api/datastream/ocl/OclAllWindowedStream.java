package org.apache.flink.streaming.api.datastream.ocl;

import org.apache.flink.streaming.api.bridge.OclContext;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.tuple.IOclTuple;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class OclAllWindowedStream<T extends IOclTuple, W extends Window> extends AllWindowedStream<T, W>
{
	private OclContext mOclContext;
	
	public OclAllWindowedStream(DataStream<T> input, WindowAssigner<? super T, W> windowAssigner, OclContext pOclContext)
	{
		super(input, windowAssigner);
		mOclContext = pOclContext;
	}
	
	public <R> SingleOutputStreamOperator<R> oclFilter(String pUserFunctionName)
	{
		return process(new ProcessAllWindowFunction<T, R, W>()
		{
			@Override
			public void process(Context context, Iterable<T> elements, Collector<R> out)
			{
				int vTuplesCount = 0;
				for (T vElement : elements)
				{
					vTuplesCount++;
				}
				
				Iterable<? extends IOclTuple> vResult = mOclContext.filter(pUserFunctionName, elements, vTuplesCount);
				
				for (IOclTuple vOclTuple : vResult)
				{
					out.collect(vOclTuple.getField(0));
				}
			}
		});
	}
	
	public AllWindowedStream<T, GlobalWindow> oclAAAAA(long size) {
		return  input.windowAll(GlobalWindows.create()).trigger(PurgingTrigger.of(CountTrigger.of(size)));
	}
	
}
