package org.apache.flink.streaming.api.engine.tuple.variable;

import org.apache.flink.streaming.common.utility.StreamUtility;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class VarDefinitionHelper
{
	public static Stream<VarDefinition> getStringVarDefinitionsAsStream(Iterable<VarDefinition> pVarDefinitions)
	{
		return StreamUtility.streamFrom(pVarDefinitions)
							.filter(x -> x.getCType().isString())
							.map(StringVarDefinition::new);
	}
	
	public static Iterable<VarDefinition> getStringVarDefinitions(Iterable<VarDefinition> pVarDefinitions)
	{
		return getStringVarDefinitionsAsStream(pVarDefinitions).collect(Collectors.toList());
	}
	
	public static Stream<VarDefinition> getStringLengthVarDefinitionsAsStream(Iterable<VarDefinition> pVarDefinitions)
	{
		return StreamUtility.streamFrom(pVarDefinitions)
							.filter(x -> x.getCType().isString())
							.map(StringLengthVarDefinition::new);
	}
	
	public static Iterable<VarDefinition> getStringLengthVarDefinitions(Iterable<VarDefinition> pVarDefinitions)
	{
		return getStringLengthVarDefinitionsAsStream(pVarDefinitions).collect(Collectors.toList());
	}
	
	public static Stream<VarDefinition> getIntegerVarDefinitionsAsStream(Iterable<VarDefinition> pVarDefinitions,
																  boolean withStringLength)
	{
		return StreamUtility.streamFrom(pVarDefinitions)
							.filter(x -> x.getCType().isInteger() ||
										 (withStringLength && x.getCType().isString()))
							.map(x ->
								 {
									 VarDefinition vVar = x;
									 if (x.getCType().isString())
									 {
										 vVar = new StringLengthVarDefinition(vVar);
									 }
									 return vVar;
								 });
	}
	
	public static Iterable<VarDefinition> getIntegerVarDefinitions(Iterable<VarDefinition> pVarDefinitions)
	{
		return getIntegerVarDefinitions(pVarDefinitions, true);
	}
	
	public static Iterable<VarDefinition> getIntegerVarDefinitions(Iterable<VarDefinition> pVarDefinitions,
															boolean withStringLength)
	{
		return getIntegerVarDefinitionsAsStream(pVarDefinitions, withStringLength)
			.collect(Collectors.toList());
	}
	
	public static Stream<VarDefinition> getDoubleVarDefinitionsAsStream(Iterable<VarDefinition> pVarDefinitions)
	{
		return StreamUtility.streamFrom(pVarDefinitions).filter(x -> x.getCType().isDouble());
	}
	
	public static Iterable<VarDefinition> getDoubleVarDefinitions(Iterable<VarDefinition> pVarDefinitions)
	{
		return getDoubleVarDefinitionsAsStream(pVarDefinitions).collect(Collectors.toList());
	}
	
	private static class StringVarDefinition extends AbstractHelpVarDefinition
	{
		private VarDefinition mOrigin;
		
		public StringVarDefinition(VarDefinition pVarDefinition)
		{
			super(pVarDefinition);
			mOrigin = pVarDefinition;
		}
		
		@Override
		public String getName()
		{
			StringBuilder vBuilder = new StringBuilder().append(mOrigin.getName());
			if(isOutputVar())
			{
				vBuilder.append("[")
						.append(getLength())
						.append("]");
			}
			return vBuilder.toString();
		}
	}
	
	private static class StringLengthVarDefinition extends AbstractHelpVarDefinition
	{
		public StringLengthVarDefinition(VarDefinition pVarDefinition)
		{
			super(pVarDefinition);
		}
		
		@Override
		public String getName()
		{
			return getName(isInputVar() ? "_sl" : "_rsl");
		}
	}
	
	private static abstract class AbstractHelpVarDefinition extends VarDefinition
	{
		private boolean mIsInputVar;
		
		public AbstractHelpVarDefinition(VarDefinition pVarDefinition)
		{
			super(pVarDefinition.getCType(), pVarDefinition.getIndex());
			mIsInputVar = pVarDefinition.isInputVar();
		}
		
		@Override
		public int getLength()
		{
			return isInputVar() ? getCType().getByteOccupation() : getCType().getMaxByteOccupation();
		}
		
		@Override
		public boolean isInputVar()
		{
			return mIsInputVar;
		}
	}
}
