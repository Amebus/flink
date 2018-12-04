package org.apache.flink.api.defaults;

import org.apache.flink.api.common.IMapperKeyComparerWrapper;
import org.apache.flink.api.common.comparers.StringKeyCaseInsenstiveComparer;
import org.apache.flink.api.engine.kernel.builder.FilterBuilder;
import org.apache.flink.api.engine.kernel.builder.MapBuilder;
import org.apache.flink.api.engine.kernel.builder.ReduceBuilder;
import org.apache.flink.api.engine.mappings.FunctionKernelBuilderMapping;

public class DefaultFunctionKernelBuilderMapping extends FunctionKernelBuilderMapping
{
	
	public DefaultFunctionKernelBuilderMapping(Iterable<String> pFunctionEngineTypes)
	{
		super(new StringKeyCaseInsenstiveComparer(""));
		setUpMappers(pFunctionEngineTypes);
	}
	
	protected void setUpMappers(Iterable<String> pFunctionEngineTypes)
	{
		IMapperKeyComparerWrapper<String> vComparer = getComparer().getNew("");
		pFunctionEngineTypes
			.forEach( pType ->
					  {
						  	//Transformations
							if(vComparer.getNew(DefaultFunctionsNames.MAP).equals(vComparer.getNew(pType)))
							{
								register(DefaultFunctionsNames.MAP, MapBuilder::new);
							}
							else if (vComparer.getNew(DefaultFunctionsNames.FILTER).equals(vComparer.getNew(pType)))
							{
								register(DefaultFunctionsNames.FILTER, FilterBuilder::new);
							}
							//Actions
							else if (vComparer.getNew(DefaultFunctionsNames.REDUCE).equals(vComparer.getNew(pType)))
							{
								register(DefaultFunctionsNames.REDUCE, ReduceBuilder::new);
							}
					  });
	}
	
}
