package org.apache.flink.streaming.api.ocl.engine.builder.mappers;

import org.apache.flink.streaming.api.ocl.common.mappers.GenericOnDemandLoadMapper;
import org.apache.flink.streaming.api.ocl.serialization.StreamReader;

import java.nio.ByteOrder;

public class NumbersByteOrderingStreamReaderMapper
	extends GenericOnDemandLoadMapper<ByteOrder, StreamReader>
{
}
