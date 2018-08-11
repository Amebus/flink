package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.List;

public interface OclOneInputOperator<IN extends IOclTuple, OUT extends IOclTuple>
	extends StreamOperator<OUT>
{
	/**
	 * Processes one element that arrived at this operator.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 */
	void processElements(List<IN> elements) throws Exception;
	
	/**
	 * Processes a {@link Watermark}.
	 * This method is guaranteed to not be called concurrently with other methods of the operator.
	 *
	 * @see org.apache.flink.streaming.api.watermark.Watermark
	 */
	void processWatermark(Watermark mark) throws Exception;
	
	void processLatencyMarker(LatencyMarker latencyMarker) throws Exception;
}
