package org.apache.flink.api.engine.builder.mappers;

import org.apache.flink.api.common.mappers.GenericOnDemandLoadMapper;
import org.apache.flink.api.serialization.StreamWriter;

import java.nio.ByteOrder;

public class NumbersByteOrderingStreamWriterMapper
	extends GenericOnDemandLoadMapper<ByteOrder, StreamWriter>
{

}
