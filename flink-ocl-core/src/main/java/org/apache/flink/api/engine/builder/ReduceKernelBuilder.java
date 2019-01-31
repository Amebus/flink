package org.apache.flink.api.engine.builder;

import org.apache.flink.api.common.utility.StreamUtility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class ReduceKernelBuilder extends KernelBuilder
{
	public ReduceKernelBuilder(KernelBuilderOptions pKernelBuilderOptions)
	{
		super(pKernelBuilderOptions);
	}
	
	@Override
	protected String getKernelCode(
		HashMap<String, Iterable<KernelLogicalVariable>> pKernelLogicalVariables,
		HashMap<String, KernelVariablesLine> pKernelVariablesLines)
	{
		return getUtilityFunctions() + "\n" +
			   getSerializationMacros() + "\n" +
			   getDeserializationMacros() + "\n" +
			   getDefines(pKernelLogicalVariables) + "\n" +
			   getKernelSignature() + "\n" +
			   "\n{\n" +
			   getUtilityVars() + "\n" +
			   getVariableDefinitionsLine(pKernelVariablesLines) + "\n"  +
			   getCopyToLocalCacheCode(pKernelLogicalVariables) + "\n"  +
			   getStepsLoop(pKernelLogicalVariables) + "\n"  +
			   getReturnToGlobalMemory() + "\n"  +
			   "\n};\n";
	}
	
	protected String getDefines(HashMap<String, Iterable<KernelLogicalVariable>> pKernelLogicalVariables)
	{
		return getTypesDefine() + "\n" +
			   getStringsMaxBytesDefines(pKernelLogicalVariables) + "\n" +
			   getAdditionalDefines() + "\n";
	}
	
	protected String getTypesDefine()
	{
		return "#define INT 1\n" +
			   "#define DOUBLE 2\n" +
			   "#define STRING 3\n";
	}
	
	protected String getStringsMaxBytesDefines(HashMap<String, Iterable<KernelLogicalVariable>> pKernelLogicalVariables)
	{
		StringBuilder vBuilder = new StringBuilder();
		
		getStringMaxBytesDefinesList(pKernelLogicalVariables)
			.forEach(def -> vBuilder.append(def.getCode()).append("\n"));
		
		return vBuilder.toString();
	}
	
	protected String getAdditionalDefines()
	{
		return "#define LAST_STEP 1";
	}
	
	protected Iterable<StringMaxBytesDefine> getStringMaxBytesDefinesList(
		HashMap<String, Iterable<KernelLogicalVariable>> pKernelLogicalVariables)
	{
		String vInputTuple = getTupleKinds().iterator().next();
		
		Iterable<KernelLogicalVariable> vInputVariables
			= pKernelLogicalVariables.get(vInputTuple);
		
		return StreamUtility.streamFrom(vInputVariables)
							.filter( str -> str.getVarType().equals("string"))
							.map(str -> new StringMaxBytesDefine(str.getIndex(), str.getBytesDim()))
							.collect(Collectors.toList());
	}
	
	protected String getCopyToLocalCacheCode(HashMap<String, Iterable<KernelLogicalVariable>> pKernelLogicalVariables)
	{
		String vS1 = "//copy from global to local\n" +
					 "    unsigned char _types[3];\n" +
					 "    _types[0] = _data[1];\n" +
					 "    _types[1] = _data[2];\n" +
					 "    _types[2] = _data[3];\n" +
					 "    for(int i = _i, k = _lId * _otd, j = 0; _tCounter < _arity; _tCounter++)\n" +
					 "    {\n" +
					 "        if(_types[_tCounter] < STRING)\n" +
					 "        {\n" +
					 "            _copyLength = 4;\n" +
					 "            if(_types[_tCounter] == DOUBLE)\n" +
					 "            {\n" +
					 "                _copyLength = 8;\n" +
					 "            }\n" +
					 "            \n" +
					 "            while(j < _copyLength)\n" +
					 "            {\n" +
					 "                _localCache[k++] = _data[i++];\n" +
					 "                j++;\n" +
					 "            }\n" +
					 "            j = 0;\n" +
					 "        }\n";
		
		String vS2 = "        else\n" +
					 "        {\n" +
					 "            i+=4;\n" +
					 "            do\n" +
					 "            {\n" +
					 "                _localCache[k] = _data[i++];\n" +
					 "                _continueCopy = _localCache[k] != '\\0';\n" +
					 "                k++;\n" +
					 "            } while(_continueCopy);\n" +
					 "\n" +
					 "            int _sMaxBytes = 0;\n" +
					 "            //if to understand the string max length\n" +
					 getStringMaxLengthIf(pKernelLogicalVariables) +
					 "\n" +
					 "            if(k < _sMaxBytes)\n" +
					 "            {\n" +
					 "                for(;k < _sMaxBytes; k++)\n" +
					 "                {\n" +
					 "                    _localCache[k] = '\\0';\n" +
					 "                }\n" +
					 "            }\n" +
					 "        }\n" +
					 "        _continueCopy = 1;\n" +
					 "    }\n";
		
		String vResult = vS1;
		
		if(StreamUtility.streamFrom(getStringMaxBytesDefinesList(pKernelLogicalVariables)).anyMatch(x -> true))
		{
			vResult += vS2;
		}
		else
		{
			vResult += "\t}\n";
		}
		
		return vResult +
			   "\n" +
			   "    barrier(CLK_LOCAL_MEM_FENCE);" +
			   "\n\n";
	}
	
	protected String getStringMaxLengthIf(HashMap<String, Iterable<KernelLogicalVariable>> pKernelLogicalVariables)
	{
		Iterable<StringMaxBytesDefine> vMaxBytesDefines = getStringMaxBytesDefinesList(pKernelLogicalVariables);
		
		return StreamUtility
			.streamFrom(vMaxBytesDefines)
			.map( def -> "if(_tCounter == " + def.getIndex() + ")\n" +
						 "{\n" +
						 "	_sMaxBytes = " + def.getDefineName() +
						 "}\n")
			.reduce( "", (a, b) -> a + "\nelse " + b);
	}
	
	protected String getStepsLoop(HashMap<String, Iterable<KernelLogicalVariable>> pKernelLogicalVariables)
	{
		return "for(uint _currentStep = _steps; _currentStep > 0 && _grId < _outputCount; _currentStep--)\n" +
			   "    {\n" +
			   "        _outputCount = ceil((double)_outputCount/_grSize);\n" +
			   "\n" +
			   getReduceLoop(pKernelLogicalVariables) +
			   "\n" +
			   "        if(_currentStep > LAST_STEP)\n" +
			   "        {\n" +
			   "            for(uint i = 0, j = _gId; i < _otd; i++, j++)\n" +
			   "            {\n" +
			   "                _midResults[j] = _identity[i];\n" +
			   "            }\n" +
			   "            barrier(CLK_GLOBAL_MEM_FENCE);\n" +
			   "\n" +
			   "            for(uint i = 0, j = _grId; i < _otd; i++, j++)\n" +
			   "            {\n" +
			   "                _midResults[j] = _localCache[i];\n" +
			   "            }\n" +
			   "            barrier(CLK_GLOBAL_MEM_FENCE);\n" +
			   "\n" +
			   "        }\n" +
			   "        else\n" +
			   "        {\n" +
			   "            barrier(CLK_LOCAL_MEM_FENCE);\n" +
			   "        }\n" +
			   "\n" +
			   "        if(_grId < _outputCount)\n" +
			   "        {\n" +
			   "            for(uint i = 0, j = _gId, k = _lId * _otd; i < _otd; i++, j++, k++)\n" +
			   "            {\n" +
			   "                _localCache[k] = _midResults[j];\n" +
			   "            }\n" +
			   "            barrier(CLK_LOCAL_MEM_FENCE);\n" +
			   "        }\n" +
			   "    }\n";
	}
	
	protected String getReduceLoop(HashMap<String, Iterable<KernelLogicalVariable>> pKernelLogicalVariables)
	{
		List<String> vToUse = new ArrayList<>();
		vToUse.add("local-a");
		List<String> vToUse2 = new ArrayList<>();
		vToUse2.add("local-b");
		
		return "for(uint _stride = _grSize/2; _stride > 0 ; _stride /= 2)\n" +
			   "{\n" +
			   "	if(_lId < _stride)\n" +
			   "    {\n" +
			   "        _iTemp = _lId;" +
			   "\n\n" +
			   getDeserialization(pKernelLogicalVariables, vToUse) +
			   "		_iTemp = _lId + _stride * _otd;\n" +
			   "\n" +
			   getDeserialization(pKernelLogicalVariables, vToUse2) +
			   "\n" +
			   "        //user function\n" +
			   getUserFunction().getFunction() +
			   "\n\n\n" +
			   "        _iTemp = _lId;\n" +
			   "\n" +
			   getSerialization(pKernelLogicalVariables) +
			   "\n" +
			   "        \n" +
			   "	}   \n" +
			   "    barrier(CLK_LOCAL_MEM_FENCE);\n" +
			   "}\n";
	}
	
	protected String getReturnToGlobalMemory()
	{
		return "if(_gId == 0)\n" +
			   "{\n" +
			   "	for(int i = 0, j = _ri; i < _otd; i++, j++)\n" +
			   "    {\n" +
			   "    	_finalResult[j] = _localCache[i];\n" +
			   "    }\n" +
			   "}";
	}
	
	private static class StringMaxBytesDefine
	{
		private String mBaseDefineName = "STRING_MAX_BYTE";
		private int mIndex;
		private int mMaxBytes;
		
		public StringMaxBytesDefine(int pIndex, int pMaxBytes)
		{
			mIndex = pIndex;
			mMaxBytes = pMaxBytes;
		}
		
		public int getIndex()
		{
			return mIndex;
		}
		
		public String getDefineName()
		{
			return mBaseDefineName + "_" + getIndex();
		}
		
		public int getMaxBytes()
		{
			return mMaxBytes;
		}
		
		public String getCode()
		{
			return "#define " + getDefineName() + " " + getMaxBytes();
		}
	}
}
