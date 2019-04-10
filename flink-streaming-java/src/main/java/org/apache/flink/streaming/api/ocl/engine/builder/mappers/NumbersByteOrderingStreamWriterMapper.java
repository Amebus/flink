package org.apache.flink.streaming.api.ocl.engine.builder.mappers;

import org.apache.flink.streaming.api.ocl.common.mappers.GenericOnDemandLoadMapper;
import org.apache.flink.streaming.api.ocl.serialization.StreamWriter;

import java.nio.ByteOrder;

public class NumbersByteOrderingStreamWriterMapper
	extends GenericOnDemandLoadMapper<ByteOrder, StreamWriter>
{

}
