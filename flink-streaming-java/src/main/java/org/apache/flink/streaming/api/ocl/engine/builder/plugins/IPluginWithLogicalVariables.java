package org.apache.flink.streaming.api.ocl.engine.builder.plugins;

import org.apache.flink.streaming.api.ocl.engine.builder.plugins.utility.KernelLogicalVariable;
import org.apache.flink.streaming.configuration.ITupleDefinition;

import java.util.ArrayList;
import java.util.List;

public interface IPluginWithLogicalVariables extends IPluginWithVarTypes
{
	ITupleDefinition getTuple();
	String getVarNamePrefix();
	String getLogicalVarsKey();
	
	default List<KernelLogicalVariable> getKernelLogicalVariables()
	{
		return getExtra(getLogicalVarsKey(), () ->
		{
			ITupleDefinition vTuple = getTuple();
			List<KernelLogicalVariable> vResult = new ArrayList<>(vTuple.getArity());
			
			vTuple
				.forEach(vVar ->
						 {
							 String vName = getVarNamePrefix() + vVar.getIndex();
							 String vType = vVar.getType().toLowerCase();
					
							 if(vType.startsWith("i"))
							 {
								 vType = getIntLogicalType();
							 }
							 else if(vType.startsWith("d"))
							 {
								 vType = getDoubleLogicalType();
							 }
							 else if(vType.startsWith("s"))
							 {
								 vType = getStringLogicalType();
							 }
							 vResult.add(
								 new KernelLogicalVariable(vType, vName, vVar.getIndex(), vVar.getMaxReservedBytes()));
						 });
			
			return vResult;
		});
	}
}
