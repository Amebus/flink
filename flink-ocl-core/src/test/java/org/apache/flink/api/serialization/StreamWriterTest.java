package org.apache.flink.api.serialization;

import org.apache.flink.api.tuple.Tuple1Ocl;
import org.apache.flink.api.tuple.Tuple2Ocl;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.helpers.Constants.*;
import static org.apache.flink.helpers.StreamsGetter.getStreamFrom;
import static org.apache.flink.helpers.StreamsGetter.getStreamWriterFrom;
import static org.apache.flink.helpers.TupleGetters.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamWriterTest
{
	@Test
	public void Writer_EmptyStreamFromNullList_Ok()
	{
		StreamWriter vWriter = StreamWriter.getStreamWriter();
		StreamWriterResult vResult = vWriter.writeStream();
		
		assertTrue(0 == vResult.getStream().length);
		assertTrue(0 == vResult.getPositions().length);
	}
	
	@Test
	public void Writer_EmptyStreamFormEmptyList_Ok()
	{
		StreamWriter vWriter = StreamWriter.getStreamWriter().setTupleList(getEmptyTupleList());
		StreamWriterResult vResult = vWriter.writeStream();
		
		assertTrue(0 == vResult.getStream().length);
		assertTrue(0 == vResult.getPositions().length);
	}
	
	@Test
	public void Writer_StreamFormUnsupportedTuple_Error()
	{
		StreamWriter vWriter = StreamWriter.getStreamWriter().setTupleList(getListWithUsupportedT());
		boolean vError = false;
		
		try
		{
			vWriter.writeStream();
		}
		catch (IllegalArgumentException ex)
		{
			vError = true;
		}
		assertTrue(vError);
	}
	
	@Test
	public void WriteTuple1Ocl_Integer_Ok()
	{
		Tuple1Ocl<Integer> vTuple = new Tuple1Ocl<>(ITV_0);
		
		byte[] vStream = getStreamFrom(vTuple);
		
		assertEquals(6, vStream.length);
		
		assertEquals(vTuple.getArityOcl(), vStream[0]);
		assertEquals(Types.INT, vStream[1]);
	}
	
	
	@Test
	public void WriteTuple1Ocl_Double_Ok()
	{
		Tuple1Ocl<Double> vTuple = new Tuple1Ocl<>(DTV_0);
		
		byte[] vStream = getStreamFrom(vTuple);
		
		assertEquals(10, vStream.length);
		
		assertEquals(vTuple.getArityOcl(), vStream[0]);
		assertEquals(Types.DOUBLE, vStream[1]);
	}
	
	@Test
	public void WriteTuple1Ocl_String_Ok()
	{
		Tuple1Ocl<String> vTuple = new Tuple1Ocl<>(STV_0);
		
		byte[] vStream = getStreamFrom(vTuple);
		
		assertEquals(2 + 4 + STV_0.length(), vStream.length);
		
		assertEquals(vTuple.getArityOcl(), vStream[0]);
		assertEquals(Types.STRING, vStream[1]);
		
		byte[] vStringLength = Arrays.copyOfRange(vStream, 2, 6);
		
		assertEquals(STV_0.length(), ByteBuffer.wrap(vStringLength).getInt());
	}
	
	@Test
	public void WriteTuple2Ocl_IntegerInteger_Ok()
	{
		Tuple2Ocl<Integer, Integer> vTuple = new Tuple2Ocl<>(ITV_1, ITV_2);
		
		byte[] vStream = getStreamFrom(vTuple);
		
		assertEquals(11, vStream.length);
		
		assertEquals(vTuple.getArityOcl(), vStream[0]);
		assertEquals(Types.INT, vStream[1]);
		assertEquals(Types.INT, vStream[2]);
	}
	
	@Test
	public void WriteTuple2Ocl_IntegerDouble_Ok()
	{
		Tuple2Ocl<Integer, Double> vTuple = new Tuple2Ocl<>(ITV_1, DTV_2);
		byte[] vStream = getStreamFrom(vTuple);
		
		assertEquals(15, vStream.length);
		
		assertEquals(vTuple.getArityOcl(), vStream[0]);
		assertEquals(Types.INT, vStream[1]);
		assertEquals(Types.DOUBLE, vStream[2]);
	}
	
	@Test
	public void WriteTuple2Ocl_IntegerString_Ok()
	{
		Tuple2Ocl<Integer, String> vTuple = new Tuple2Ocl<>(ITV_1, STV_2);
		byte[] vStream = getStreamFrom(vTuple);
		
		assertEquals(11 + STV_2.length(), vStream.length);
		
		assertEquals(vTuple.getArityOcl(), vStream[0]);
		assertEquals(Types.INT, vStream[1]);
		assertEquals(Types.STRING, vStream[2]);
	}
	
	@Test
	public void WriteTuple2Ocl_DoubleInteger_Ok()
	{
		Tuple2Ocl<Double, Integer> vTuple = new Tuple2Ocl<>(DTV_1, ITV_2);
		byte[] vStream = getStreamFrom(vTuple);
		
		assertEquals(15, vStream.length);
		
		assertEquals(vTuple.getArityOcl(), vStream[0]);
		assertEquals(Types.DOUBLE, vStream[1]);
		assertEquals(Types.INT, vStream[2]);
	}
	
	@Test
	public void WriteTuple2Ocl_DoubleDouble_Ok()
	{
		Tuple2Ocl<Double, Double> vTuple = new Tuple2Ocl<>(DTV_1, DTV_2);
		byte[] vStream = getStreamFrom(vTuple);
		
		assertEquals(19, vStream.length);
		
		assertEquals(vTuple.getArityOcl(), vStream[0]);
		assertEquals(Types.DOUBLE, vStream[1]);
		assertEquals(Types.DOUBLE, vStream[2]);
	}
	
	@Test
	public void WriteTuple2Ocl_DoubleString_Ok()
	{
		Tuple2Ocl<Double, String> vTuple = new Tuple2Ocl<>(DTV_1, STV_2);
		byte[] vStream = getStreamFrom(vTuple);
		
		assertEquals(15 + STV_2.length(), vStream.length);
		
		assertEquals(vTuple.getArityOcl(), vStream[0]);
		assertEquals(Types.DOUBLE, vStream[1]);
		assertEquals(Types.STRING, vStream[2]);
	}
	
	@Test
	public void WriteTuple2Ocl_StringString_Ok()
	{
		Tuple2Ocl<String, String> vTuple = new Tuple2Ocl<>(STV_1, STV_2);
		byte[] vStream = getStreamFrom(vTuple);
		
		assertEquals(1 + 10 + STV_1.length() + STV_2.length(), vStream.length);
		
		assertEquals(vTuple.getArityOcl(), vStream[0]);
		assertEquals(Types.STRING, vStream[1]);
		assertEquals(Types.STRING, vStream[2]);
	}
	
	@Test
	public void WriteTuple2Ocl_StringInteger_Ok()
	{
		Tuple2Ocl<String, Integer> vTuple = new Tuple2Ocl<>(STV_1, ITV_2);
		byte[] vStream = getStreamFrom(vTuple);
		
		assertEquals(1 + 10 + STV_1.length(), vStream.length);
		
		assertEquals(vTuple.getArityOcl(), vStream[0]);
		assertEquals(Types.STRING, vStream[1]);
		assertEquals(Types.INT, vStream[2]);
	}
	
	@Test
	public void WriteTuple2Ocl_StringDouble_Ok()
	{
		Tuple2Ocl<String, Double> vTuple = new Tuple2Ocl<>(STV_1, DTV_2);
		byte[] vStream = getStreamFrom(vTuple);
		
		assertEquals(1 + 14 + STV_1.length(), vStream.length);
		
		assertEquals(vTuple.getArityOcl(), vStream[0]);
		assertEquals(Types.STRING, vStream[1]);
		assertEquals(Types.DOUBLE, vStream[2]);
	}
	
	@Test
	public void WriteMultipleTuple1Ocl_Integer_Ok()
	{
		List<Tuple1Ocl<Integer>> vList = getIntegerTupleList();
		
		StreamWriter vStreamWriter = getStreamWriterFrom(vList);
		
		Tuple2Ocl<byte[], int[]> vResult = vStreamWriter.writeStream();
		int vExpectedLength = 2 + vList.size() * Dimensions.INT;
		byte[] vStream = vResult.getField(0);
		int[] vTupleIndexes = vResult.getField(1);
		
		
		assertEquals( vExpectedLength , vStream.length);
		assertEquals( vList.size() , vTupleIndexes.length);
		
		assertEquals( 1, vStream[0]);
		assertEquals( Types.INT, vStream[1]);
	}
}
