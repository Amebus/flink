package org.apache.flink.streaming.api.ocl.engine.builder.options;

import org.apache.flink.streaming.api.ocl.bridge.identity.BigEndianIdentityValuesConverter;
import org.apache.flink.streaming.api.ocl.bridge.identity.LittleEndianIdentityValuesConverter;
import org.apache.flink.streaming.api.ocl.engine.IOclContextMappings;
import org.apache.flink.streaming.api.ocl.engine.ITupleBytesDimensionGetter;
import org.apache.flink.streaming.api.ocl.engine.builder.*;
import org.apache.flink.streaming.api.ocl.engine.builder.mappers.*;
import org.apache.flink.streaming.api.ocl.serialization.bigendian.BigEndianStreamReader;
import org.apache.flink.streaming.api.ocl.serialization.bigendian.BigEndianStreamWriter;
import org.apache.flink.streaming.api.ocl.serialization.littleendian.LittleEndianStreamReader;
import org.apache.flink.streaming.api.ocl.serialization.littleendian.LittleEndianStreamWriter;

import java.nio.ByteOrder;

public class DefaultsValues
{
	
	public static final ITupleBytesDimensionGetter DEFAULT_TUPLE_BYTES_DIMENSION_GETTER =
		pTupleDefinition ->
		{
			final int[] vResult = {0};
			pTupleDefinition
				.forEach( pVar ->
						  {
							  vResult[0] += pVar.getMaxReservedBytes();
							  if(pVar.getType().startsWith("s"))
							  {
								  vResult[0]+=4;
							  }
						  });
			return vResult[0];
		};
	
	public static class DefaultKernelBuilderMapper extends KernelBuilderMapper
	{
		public DefaultKernelBuilderMapper()
		{
			setUpMappers();
		}
		
		protected void setUpMappers()
		{
			register("map", new MapKernelBuilder());
			register("filter", new FilterKernelBuilder());
			register("reduce", new ReduceKernelBuilder());
		}
	}
	
	public static class DefaultNumbersByteOrderingStreamWriterMapper extends NumbersByteOrderingStreamWriterMapper
	{
		public DefaultNumbersByteOrderingStreamWriterMapper()
		{
			setUpMappers();
		}
		
		protected void setUpMappers()
		{
			register(ByteOrder.LITTLE_ENDIAN, LittleEndianStreamWriter::new);
			register(ByteOrder.BIG_ENDIAN, BigEndianStreamWriter::new);
		}
	}
	
	public static class DefaultNumbersByteOrderingStreamReaderMapper extends NumbersByteOrderingStreamReaderMapper
	{
		public DefaultNumbersByteOrderingStreamReaderMapper()
		{
			setUpMappers();
		}
		
		protected void setUpMappers()
		{
			register(ByteOrder.LITTLE_ENDIAN, LittleEndianStreamReader::new);
			register(ByteOrder.BIG_ENDIAN, BigEndianStreamReader::new);
		}
	}
	
	public static class DefaultNumbersByteOrderingToIdentityValuesConverterMapper
		extends NumbersByteOrderingToIdentityValuesConverterMapper
	{
		public DefaultNumbersByteOrderingToIdentityValuesConverterMapper()
		{
			setUpMappers();
		}
		
		protected void setUpMappers()
		{
			register(ByteOrder.LITTLE_ENDIAN, new LittleEndianIdentityValuesConverter());
			register(ByteOrder.BIG_ENDIAN, new BigEndianIdentityValuesConverter());
		}
	}
	
	public static class DefaultOclContextMappings implements IOclContextMappings
	{
		protected ITupleBytesDimensionGetter mTupleBytesDimensionGetters;
		
		
		protected KernelBuilderMapper mKernelBuilderMapper;
		protected NumbersByteOrderingStreamWriterMapper mNumbersByteOrderingStreamWriterMapper;
		protected NumbersByteOrderingStreamReaderMapper mNumbersByteOrderingStreamReaderMapper;
		protected NumbersByteOrderingToIdentityValuesConverterMapper mNumbersByteOrderingToIdentityValuesConverterMapper;
		
		public DefaultOclContextMappings()
		{
			mTupleBytesDimensionGetters = DEFAULT_TUPLE_BYTES_DIMENSION_GETTER;
			
			
			mKernelBuilderMapper = new DefaultKernelBuilderMapper();
			mNumbersByteOrderingStreamWriterMapper = new DefaultNumbersByteOrderingStreamWriterMapper();
			mNumbersByteOrderingStreamReaderMapper = new DefaultNumbersByteOrderingStreamReaderMapper();
			mNumbersByteOrderingToIdentityValuesConverterMapper = new DefaultNumbersByteOrderingToIdentityValuesConverterMapper();
		}
		
		@Override
		public ITupleBytesDimensionGetter getTupleBytesDimensionGetter()
		{
			return mTupleBytesDimensionGetters;
		}
		
		@Override
		public KernelBuilderMapper getKernelBuilderMapper()
		{
			return mKernelBuilderMapper;
		}
		
		@Override
		public NumbersByteOrderingStreamWriterMapper getNumbersByteOrderingStreamWriterMapper()
		{
			return mNumbersByteOrderingStreamWriterMapper;
		}
		
		@Override
		public NumbersByteOrderingStreamReaderMapper getNumbersByteOrderingStreamReaderMapper()
		{
			return mNumbersByteOrderingStreamReaderMapper;
		}
		
		@Override
		public NumbersByteOrderingToIdentityValuesConverterMapper getByteOrderingToIdentityValuesConverterMapper()
		{
			return mNumbersByteOrderingToIdentityValuesConverterMapper;
		}
	}
	
	public static class DefaultsSerializationTypes
	{
		public static final byte INT = 1;
		public static final byte DOUBLE = 2;
		public static final byte STRING = 3;
	}
}
