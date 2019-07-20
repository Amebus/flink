package org.apache.flink.streaming.api.ocl.engine.builder.plugins.utility;

import java.util.LinkedList;
import java.util.List;

public class KernelVariablesLine
{
	private String mVarType;
	private List<String> mVarDefinition;
	
	public KernelVariablesLine(String pVarType)
	{
		mVarType = pVarType;
		mVarDefinition = new LinkedList<>();
	}
	
	public String getVarType()
	{
		return mVarType;
	}
	
	public Iterable<String> getVarDefinition()
	{
		return mVarDefinition;
	}
	
	public KernelVariablesLine setVarType(String pVarType)
	{
		mVarType = pVarType;
		return this;
	}
	
	public KernelVariablesLine addVarDef(String pVarName)
	{
		mVarDefinition.add(pVarName);
		return this;
	}
}
