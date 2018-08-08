package org.apache.flink.helpers;

import org.apache.flink.api.serialization.Dimensions;
import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.api.tuple.Tuple1Ocl;
import org.apache.flink.api.tuple.Tuple2Ocl;

import java.util.ArrayList;
import java.util.List;

public class TupleGetters
{
	public static class UnsupportedT
	{
		
	}
	
	public static int getExpectedStreamLength(List<? extends IOclTuple> pExpectedList)
	{
		List<? extends IOclTuple> vNewList = new ArrayList<>(pExpectedList);
		vNewList.add(null);
		int[] vPositions = getTupleExpectedPositions(vNewList);
		return vPositions[vPositions.length - 1];
		// vExpectedStreamLength += vExpectedList.stream().mapToInt(x -> x.getT0().length()).sum(); only for strings
	}
	
	public static int[] getTupleExpectedPositions(List<? extends IOclTuple> pExpectedList)
	{
		int[] vExpectedPositions = new int[pExpectedList.size()];
		vExpectedPositions[0] = 1 + pExpectedList.get(0).getArityOcl();
		
		for (int i = 1; i < vExpectedPositions.length; i++)
		{
			int j = i - 1;
			vExpectedPositions[i] = vExpectedPositions[j];
			
			IOclTuple vOclTuple = pExpectedList.get(j);
			Object vT;
			for (int k = 0; k < vOclTuple.getArityOcl(); k++)
			{
				vT = vOclTuple.getFieldOcl(k);
				switch (vT.getClass().getName())
				{
					case "java.lang.Double":
						vExpectedPositions[i] += Dimensions.DOUBLE;
						break;
					case "java.lang.String":
						vExpectedPositions[i] += ((String)vT).length();
					case "java.lang.Integer":
						vExpectedPositions[i] += Dimensions.INT;
						break;
					default:
						throw new IllegalArgumentException("Object type not recognized, unable to serialize it");
				}
			}
		}
		
		return vExpectedPositions;
	}
	
	public static List<IOclTuple> getEmptyTupleList()
	{
		return new ArrayList<>();
	}
	
	public static List<IOclTuple> getListWithUnsupportedT()
	{
		List<IOclTuple> vList = new ArrayList<>();
		
		vList.add(new Tuple1Ocl<>(new UnsupportedT()));
		
		return vList;
	}
	
	public static int QUANTITY = 4;
	
	public static List<Tuple1Ocl<Integer>> getIntegerTupleList()
	{
		List<Tuple1Ocl<Integer>> vResult = new ArrayList<>(QUANTITY);
		
		vResult.add(new Tuple1Ocl<>(Constants.ITV_1));
		vResult.add(new Tuple1Ocl<>(Constants.ITV_2));
		vResult.add(new Tuple1Ocl<>(Constants.ITV_3));
		vResult.add(new Tuple1Ocl<>(Constants.ITV_4));
		
		return vResult;
	}
	
	public static List<Tuple1Ocl<Double>> getDoubleTupleList()
	{
		List<Tuple1Ocl<Double>> vResult = new ArrayList<>(QUANTITY);
		
		vResult.add(new Tuple1Ocl<>(Constants.DTV_1));
		vResult.add(new Tuple1Ocl<>(Constants.DTV_2));
		vResult.add(new Tuple1Ocl<>(Constants.DTV_3));
		vResult.add(new Tuple1Ocl<>(Constants.DTV_4));
		
		return vResult;
	}
	
	public static List<Tuple1Ocl<String>> getStringTupleList()
	{
		List<Tuple1Ocl<String>> vResult = new ArrayList<>(QUANTITY);
		
		vResult.add(new Tuple1Ocl<>(Constants.STV_1));
		vResult.add(new Tuple1Ocl<>(Constants.STV_2));
		vResult.add(new Tuple1Ocl<>(Constants.STV_3));
		vResult.add(new Tuple1Ocl<>(Constants.STV_4));
		
		return vResult;
	}
	
	public static List<Tuple2Ocl<Integer, Integer>> getIntegerIntegerTupleList()
	{
		List<Tuple2Ocl<Integer, Integer>> vResult = new ArrayList<>(QUANTITY);
		
		vResult.add(new Tuple2Ocl<>(Constants.ITV_1, Constants.ITV_4));
		vResult.add(new Tuple2Ocl<>(Constants.ITV_2, Constants.ITV_3));
		vResult.add(new Tuple2Ocl<>(Constants.ITV_3, Constants.ITV_2));
		vResult.add(new Tuple2Ocl<>(Constants.ITV_4, Constants.ITV_1));
		
		return vResult;
	}
	
	public static List<Tuple2Ocl<Integer, Double>> getIntegerDoubleTupleList()
	{
		List<Tuple2Ocl<Integer, Double>> vResult = new ArrayList<>(QUANTITY);
		
		vResult.add(new Tuple2Ocl<>(Constants.ITV_1, Constants.DTV_4));
		vResult.add(new Tuple2Ocl<>(Constants.ITV_2, Constants.DTV_3));
		vResult.add(new Tuple2Ocl<>(Constants.ITV_3, Constants.DTV_2));
		vResult.add(new Tuple2Ocl<>(Constants.ITV_4, Constants.DTV_1));
		
		return vResult;
	}
	
	public static List<Tuple2Ocl<Integer, String>> getIntegerStringTupleList()
	{
		List<Tuple2Ocl<Integer, String>> vResult = new ArrayList<>(QUANTITY);
		
		vResult.add(new Tuple2Ocl<>(Constants.ITV_1, Constants.STV_4));
		vResult.add(new Tuple2Ocl<>(Constants.ITV_2, Constants.STV_3));
		vResult.add(new Tuple2Ocl<>(Constants.ITV_3, Constants.STV_2));
		vResult.add(new Tuple2Ocl<>(Constants.ITV_4, Constants.STV_1));
		
		return vResult;
	}
	
	public static List<Tuple2Ocl<Double, Double>> getDoubleDoubleTupleList()
	{
		List<Tuple2Ocl<Double, Double>> vResult = new ArrayList<>(QUANTITY);
		
		vResult.add(new Tuple2Ocl<>(Constants.DTV_1, Constants.DTV_4));
		vResult.add(new Tuple2Ocl<>(Constants.DTV_2, Constants.DTV_3));
		vResult.add(new Tuple2Ocl<>(Constants.DTV_3, Constants.DTV_2));
		vResult.add(new Tuple2Ocl<>(Constants.DTV_4, Constants.DTV_1));
		
		return vResult;
	}
	
	public static List<Tuple2Ocl<Double, Integer>> getDoubleIntegerTupleList()
	{
		List<Tuple2Ocl<Double, Integer>> vResult = new ArrayList<>(QUANTITY);
		
		vResult.add(new Tuple2Ocl<>(Constants.DTV_1, Constants.ITV_4));
		vResult.add(new Tuple2Ocl<>(Constants.DTV_2, Constants.ITV_3));
		vResult.add(new Tuple2Ocl<>(Constants.DTV_3, Constants.ITV_2));
		vResult.add(new Tuple2Ocl<>(Constants.DTV_4, Constants.ITV_1));
		
		return vResult;
	}
	
	public static List<Tuple2Ocl<Double, String>> getDoubleStringTupleList()
	{
		List<Tuple2Ocl<Double, String>> vResult = new ArrayList<>(QUANTITY);
		
		vResult.add(new Tuple2Ocl<>(Constants.DTV_1, Constants.STV_4));
		vResult.add(new Tuple2Ocl<>(Constants.DTV_2, Constants.STV_3));
		vResult.add(new Tuple2Ocl<>(Constants.DTV_3, Constants.STV_2));
		vResult.add(new Tuple2Ocl<>(Constants.DTV_4, Constants.STV_1));
		
		return vResult;
	}
	
	public static List<Tuple2Ocl<String, String>> getStringStringTupleList()
	{
		List<Tuple2Ocl<String, String>> vResult = new ArrayList<>(QUANTITY);
		
		vResult.add(new Tuple2Ocl<>(Constants.STV_1, Constants.STV_4));
		vResult.add(new Tuple2Ocl<>(Constants.STV_2, Constants.STV_3));
		vResult.add(new Tuple2Ocl<>(Constants.STV_3, Constants.STV_2));
		vResult.add(new Tuple2Ocl<>(Constants.STV_4, Constants.STV_1));
		
		return vResult;
	}
	
	public static List<Tuple2Ocl<String, Integer>> getStringIntegerTupleList()
	{
		List<Tuple2Ocl<String, Integer>> vResult = new ArrayList<>(QUANTITY);
		
		vResult.add(new Tuple2Ocl<>(Constants.STV_1, Constants.ITV_4));
		vResult.add(new Tuple2Ocl<>(Constants.STV_2, Constants.ITV_3));
		vResult.add(new Tuple2Ocl<>(Constants.STV_3, Constants.ITV_2));
		vResult.add(new Tuple2Ocl<>(Constants.STV_4, Constants.ITV_1));
		
		return vResult;
	}
	
	public static List<Tuple2Ocl<String, Double>> getStringDoubleTupleList()
	{
		List<Tuple2Ocl<String, Double>> vResult = new ArrayList<>(QUANTITY);
		
		vResult.add(new Tuple2Ocl<>(Constants.STV_1, Constants.DTV_4));
		vResult.add(new Tuple2Ocl<>(Constants.STV_2, Constants.DTV_3));
		vResult.add(new Tuple2Ocl<>(Constants.STV_3, Constants.DTV_2));
		vResult.add(new Tuple2Ocl<>(Constants.STV_4, Constants.DTV_1));
		
		return vResult;
	}
}
