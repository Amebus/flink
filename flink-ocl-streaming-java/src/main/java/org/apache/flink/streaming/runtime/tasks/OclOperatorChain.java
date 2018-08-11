package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;

import java.util.List;

@Internal
public class OclOperatorChain<OUT, OP extends StreamOperator<OUT>> extends OperatorChain<OUT, OP>
{
	
	public OclOperatorChain(StreamTask<OUT, OP> containingTask,
							List<StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>>> streamRecordWriters)
	{
		super(containingTask, streamRecordWriters);
	}
	
	OclOperatorChain(StreamOperator<?>[] allOperators,
					 RecordWriterOutput<?>[] streamOutputs,
					 WatermarkGaugeExposingOutput<StreamRecord<OUT>> chainEntryPoint,
					 OP headOperator)
	{
		super(allOperators, streamOutputs, chainEntryPoint, headOperator);
	}
}
