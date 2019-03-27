package org.apache.flink.api.engine.builder.mappers;

import org.apache.flink.api.common.mappers.GenericOnDemandLoadMapper;
import org.apache.flink.api.serialization.StreamReader;

import java.nio.ByteOrder;

public class NumbersByteOrderingStreamReaderMapper
	extends GenericOnDemandLoadMapper<ByteOrder, StreamReader>
{
}
