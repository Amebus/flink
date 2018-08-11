package org.apache.flink.api.engine.kernel.line;

import com.sun.org.apache.xpath.internal.functions.FuncStringLength;
import org.apache.flink.api.common.utility.StreamUtility;
import org.apache.flink.api.engine.tuple.variable.VarDefinition;
import org.apache.flink.configuration.CTType;

import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IntVarDefinitionLine extends VarDefinitionKernelLine
{
	public IntVarDefinitionLine(Iterable<VarDefinition> pVarDefinitions)
	{
		super(getType(pVarDefinitions), getIntVariables(pVarDefinitions));
	}
	
	private static String getType(Iterable<VarDefinition> pVarDefinitions)
	{
		String vResult = "";
		if(StreamUtility.streamFrom(pVarDefinitions)
						.anyMatch( x -> x.getCType().isInteger() || x.getCType().isString()))
		{
			vResult = CTType.CTypes.INTEGER;
		}
		return vResult;
	}
	
	private static Iterable<String> getIntVariables(Iterable<VarDefinition> pVarDefinitions)
	{
		Stream<VarDefinition> vStream = StreamUtility.streamFrom(pVarDefinitions)
													 .filter(x -> x.getCType().isInteger() || x.getCType().isString());
		
		return new ArrayList<>(vStream.map(x ->
										   {
											   VarDefinition vVar = x;
											   if (x.getCType().isString())
											   {
												   vVar = new StringLengthVarDefinition(vVar);
											   }
											   return vVar.getName();
										   }).collect(Collectors.toList()));
	}
	
	private static class StringLengthVarDefinition extends VarDefinition
	{
		private boolean mIsInputVar;
		
		public StringLengthVarDefinition(VarDefinition pVarDefinition)
		{
			super(pVarDefinition.getCType(), pVarDefinition.getIndex());
			mIsInputVar = pVarDefinition.isInputVar();
		}
		
		@Override
		public String getName()
		{
			String vPrefix;
			
			if(isInputVar())
			{
				vPrefix = "_sl";
			}
			else
			{
				vPrefix = "_rsl";
			}
			return getName(vPrefix);
		}
		
		@Override
		public int getLength()
		{
			return getCType().getByteDimension();
		}
		
		@Override
		public boolean isInputVar()
		{
			return mIsInputVar;
		}
	}
}
