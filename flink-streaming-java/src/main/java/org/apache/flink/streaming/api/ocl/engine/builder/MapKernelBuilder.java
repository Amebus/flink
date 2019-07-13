package org.apache.flink.streaming.api.ocl.engine.builder;

import org.apache.flink.streaming.api.ocl.engine.builder.mappers.TemplatePluginMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class MapKernelBuilder extends PDAKernelBuilder
{
	public MapKernelBuilder(String pRootTemplate)
	{
		super(pRootTemplate);
		setUpExtras();
	}
	
	public MapKernelBuilder(String pRootTemplate, TemplatePluginMapper pTemplatePluginMapper)
	{
		super(pRootTemplate, pTemplatePluginMapper);
		setUpExtras();
	}
	
	@Override
	protected void setUpTemplatePluginMapper()
	{
		super.setUpTemplatePluginMapper();
		setUpMapTemplatePluginMapper();
	}
	
	protected void setUpMapTemplatePluginMapper()
	{
		this.registerPlugin("<[utility-vars]>", getUtilityVarsPlugin())
			.registerPlugin("<[output-utility-vars]>", getOutputUtilityVarsPlugin())
			.registerPlugin("<[deserialization]>", getDeserializationPlugin())
			.registerPlugin("<[user-function]>", getUserFunctionPlugin())
			.registerPlugin("<[serialization]>", getSerializationPlugin())
			.registerPlugin("<[input-vars]>", getInputVarsPlugin())
			.registerPlugin("<[output-vars]>", getOutputVarsPlugin());
	}
	
	protected void setUpExtras()
	{
		this.setExtra("input-var-int", "int")
			.setExtra("input-var-double", "double")
			.setExtra("input-var-string", "__global char*")
			.setExtra("output-var-int", "int")
			.setExtra("output-var-double", "double")
			.setExtra("output-var-string", "char");
	}
	
	@Override
	protected IPDAKernelBuilderPlugin getKernelCodePlugin()
	{
		return (pBuilder, pCodeBuilder) ->
			pCodeBuilder
				.append("\t\n")
				.append("\t<[utility-vars]>\n")
				.append("\t<[output-utility-vars]>\n")
				.append("\t<[input-vars]>\n")
				.append("\t<[output-vars]>\n")
				.append("\t<[deserialization]>\n")
				.append("\t<[user-function]>\n")
				.append("\t<[serialization]>")
				.append("\t\n");
	}
	
	protected IPDAKernelBuilderPlugin getUtilityVarsPlugin()
	{
		return (pBuilder, pCodeBuilder) ->
			pCodeBuilder
				.append("\n")
				.append("// utility variables")
				.append("\tuint _gId = get_global_id(0);\n")
				.append("\tunsigned char _arity = ")
				.append(pBuilder.getPDAKernelBuilderOptions().getInputTuple().getArity())
				.append(";\n")
				.append("\tint _i = _dataIndexes[_gId];\n")
				.append("\tint _userIndex = _i;\n")
				.append("\tunsigned char* _serializationTemp;\n");
			
	}
	
	protected IPDAKernelBuilderPlugin getDeserializationPlugin()
	{
		return new PDAKernelBuilderPlugin()
		{
			protected String getInputLogicalVarsKey()
			{
				return "input-logical-vars";
			}
			
			protected List<KernelLogicalVariable> getKernelLogicalVariables()
			{
				return getExtra(getInputLogicalVarsKey());
			}
			
			protected KernelDeserializationLine getDeserLine(KernelLogicalVariable pLVar)
			{
				Function<KernelLogicalVariable, KernelDeserializationLine> vF =
					getExtra("deser-" + pLVar.getVarType());
				return vF.apply(pLVar);
			}
			
			@Override
			public void parseTemplateCode(PDAKernelBuilder pKernelBuilder, StringBuilder pCodeBuilder)
			{
				setKernelAndCodeBuilder(pKernelBuilder, pCodeBuilder);
				
				pCodeBuilder
					.append("// deserialization")
					.append("\n");
				
				List<KernelDeserializationLine> vLines = new ArrayList<>();
				List<KernelLogicalVariable> vLogicalVariables = getKernelLogicalVariables();
				
				setExtra("deser-" + getIntType(),
						 (Function<KernelLogicalVariable, KernelDeserializationLine>)(pLVar) ->
						{
							String vLine = "DESER_INT( _data, _i, # );".replace("#", pLVar.getVarName());
							return new KernelDeserializationLine(vLine, pLVar.getIndex());
						})
				.setExtra("deser-" + getDoubleType(),
						 (Function<KernelLogicalVariable, KernelDeserializationLine>)(pLVar) ->
						 {
							 String vLine = "DESER_DOUBLE( _data, _i, # );".replace("#", pLVar.getVarName());
							 return new KernelDeserializationLine(vLine, pLVar.getIndex());
						 })
				.setExtra("deser-" + getStringType(),
						 (Function<KernelLogicalVariable, KernelDeserializationLine>)(pLVar) ->
						 {
							 String vLine = "DESER_STRING( _data, _i, #, @ );"
											.replace("#", pLVar.getVarName())
											.replace("@", "_tsl" + pLVar.getIndex());
							 return new KernelDeserializationLine(vLine, pLVar.getIndex());
						 });
				
				vLogicalVariables.forEach( pLVar -> vLines.add(getDeserLine(pLVar)) );
				
				vLines.sort((o1, o2) ->
							{
								int vResult = 0;
								if(o1.getDeserIndexOrder() > o2.getDeserIndexOrder())
								{
									vResult = 1;
								}
								else if(o1.getDeserIndexOrder() < o2.getDeserIndexOrder())
								{
									vResult = -1;
								}
								return vResult;
							});
				
				vLines.forEach(pLine -> pCodeBuilder.append(pLine.getDeserLine()).append("\n"));
				pCodeBuilder.append("\n");
				
				removeExtra("deser-" + getIntType());
				removeExtra("deser-" + getDoubleType());
				removeExtra("deser-" + getStringType());
			}
		};
	}
	
	protected IPDAKernelBuilderPlugin getUserFunctionPlugin()
	{
		return (pBuilder, pCodeBuilder) ->
			pCodeBuilder
				.append("\n")
				.append("// user function\n")
				.append(pBuilder.getPDAKernelBuilderOptions().getUserFunction().getFunction())
				.append("\n");
	}
	
	protected IPDAKernelBuilderPlugin getSerializationPlugin()
	{
		return new PDAKernelBuilderPlugin()
		{
			protected String getOutputLogicalVarsKey()
			{
				return "output-logical-vars";
			}
			
			protected List<KernelLogicalVariable> getKernelLogicalVariables()
			{
				return getExtra(getOutputLogicalVarsKey());
			}
			
			protected KernelSerializationLine getSerLine(KernelLogicalVariable pLVar)
			{
				Function<KernelLogicalVariable, KernelSerializationLine> vF =
					getExtra("ser-" + pLVar.getVarType());
				return vF.apply(pLVar);
			}
			
			@Override
			public void parseTemplateCode(PDAKernelBuilder pKernelBuilder, StringBuilder pCodeBuilder)
			{
				setKernelAndCodeBuilder(pKernelBuilder, pCodeBuilder);
				
				pCodeBuilder
					.append("// serialization")
					.append("\n");
				
				List<KernelSerializationLine> vLines = new ArrayList<>();
				List<KernelLogicalVariable> vLogicalVariables = getKernelLogicalVariables();
				
				setExtra("ser-" + getIntType(),
						 (Function<KernelLogicalVariable, KernelSerializationLine>)(pLVar) ->
						 {
							 String vLine = "SER_INT( #, @, _result, _serializationTemp );"
								 .replace("#", pLVar.getVarName())
								 .replace("@","_ri" + pLVar.getIndex());
							 return new KernelSerializationLine(vLine, pLVar.getIndex());
						 })
				.setExtra("ser-" + getDoubleType(),
						 (Function<KernelLogicalVariable, KernelSerializationLine>)(pLVar) ->
						 {
							 String vLine = "SER_DOUBLE( #, @, _result, _serializationTemp)"
								 .replace("#", pLVar.getVarName())
								 .replace("@","_ri" + pLVar.getIndex());
							 return new KernelSerializationLine(vLine, pLVar.getIndex());
						 })
				.setExtra("ser-" + getStringType(),
						 (Function<KernelLogicalVariable, KernelSerializationLine>)(pLVar) ->
						 {
							 String vLine = "SER_STRING( #, @, -, _result, _serializationTemp );"
								 .replace("#", pLVar.getVarName())
								 .replace("@","_ri" + pLVar.getIndex())
								 .replace("-", "_rsl" + pLVar.getIndex());
							 return new KernelSerializationLine(vLine, pLVar.getIndex());
						 });
				
				vLogicalVariables.forEach( pLVar -> vLines.add(getSerLine(pLVar)) );
				
				vLines.sort((o1, o2) ->
							{
								int vResult = 0;
								if(o1.getSerIndexOrder() > o2.getSerIndexOrder())
								{
									vResult = 1;
								}
								else if(o1.getSerIndexOrder() < o2.getSerIndexOrder())
								{
									vResult = -1;
								}
								return vResult;
							});
				
				vLines.forEach(pLine -> pCodeBuilder.append(pLine.getSerLine()).append("\n"));
				pCodeBuilder.append("\n");
				
				removeExtra("ser-" + getIntType());
				removeExtra("ser-" + getDoubleType());
				removeExtra("ser-" + getStringType());
			}
		};
	}
	
	public static class KernelSerializationLine
	{
		private String mSerLine;
		private int mSerIndexOrder;
		
		public KernelSerializationLine(String pSerLine, int pSerIndexOrder)
		{
			mSerLine = pSerLine;
			mSerIndexOrder = pSerIndexOrder;
		}
		
		public String getSerLine()
		{
			return mSerLine;
		}
		
		public int getSerIndexOrder()
		{
			return mSerIndexOrder;
		}
	}
}


