package org.apache.flink.api.engine.builder;

import java.util.HashMap;

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
			   getTypesDefine() + "\n" +
			   getKernelSignature() + "\n" +
			   "\n{\n" +
			   getUtilityVars() + "\n" +
			   getVariableDefinitionsLine(pKernelVariablesLines) + "\n"  +
			   getCopyToLocalCacheCode() + "\n"  +
			   getReduceLoop(pKernelLogicalVariables) + "\n"  +
			   getReturnToGlobalMemory() + "\n"  +
			   "\n};\n";
	}
	
	protected String getTypesDefine()
	{
		return "#define INT 1\n" +
			   "#define DOUBLE 2\n" +
			   "#define STRING 3\n";
	}
	
	protected String getCopyToLocalCacheCode()
	{
		return "//copy from global to private\n" +
			   "unsigned char _types[3];\n" +
			   "_types[0] = _data[1];\n" +
			   "_types[1] = _data[2];\n" +
			   "_types[2] = _data[3];\n" +
			   "\n" +
			   "_jobDone[_gId] = 0;\n" +
			   "\n" +
			   "for(int i = _i, k = _lId * _otd, j = 0; _tCounter < _arity; _tCounter++)\n" +
			   "{\n" +
			   "	if(_types[_tCounter] < STRING)\n" +
			   "	{\n" +
			   "		_copyLength = 4;\n" +
			   "        if(_types[_tCounter] == DOUBLE)\n" +
			   "        {\n" +
			   "        	_copyLength = 8;\n" +
			   "        }\n" +
			   "        \n" +
			   "        while(j < _copyLength)\n" +
			   "        {\n" +
			   "			_localCache[k++] = _data[i++];\n" +
			   "			j++;\n" +
			   "        }\n" +
			   "        j = 0;\n" +
			   "	}\n" +
			   "	else\n" +
			   "	{\n" +
			   "		//if per capire la lunghezza massima della stringa\n" +
			   "        i+=4;\n" +
			   "        do \n" +
			   "        {\n" +
			   "            _localCache[k] = _data[i++];\n" +
			   "            _continueCopy = _localCache[k] != '\\0';\n" +
			   "        	k++;\n" +
			   "        } while(_continueCopy);\n" +
			   "\n" +
			   "        if(k < getStringMaxByte())\n" +
			   "        {\n" +
			   "			for(;k < getStringMaxByte(); k++)\n" +
			   "            {\n" +
			   "            	_localCache[k] = '\\0';\n" +
			   "            }\n" +
			   "        }\n" +
			   "	}\n" +
			   "	_continueCopy = 1;\n" +
			   "}\n\n" +
			   "barrier(CLK_LOCAL_MEM_FENCE);" +
			   "\n\n";
	}
	
	protected String getReduceLoop(HashMap<String, Iterable<KernelLogicalVariable>> pKernelLogicalVariables)
	{
		return "for(uint _stride = _grSize/2; _stride > 0 ; _stride /= 2)\n" +
			   "{\n" +
			   "	if(_lId < _stride)\n" +
			   "    {\n" +
			   "    	//dentro al ciclo con lo stride\n" +
			   "        _iTemp = _i;\n" +
			   "\n" +
			   getDeserialization(pKernelLogicalVariables) +
			   "\n" +
			   "        //user function\n" +
			   getUserFunction().getFunction() + "\n" +
			   "\n" +
			   "\n" +
			   "        _iTemp = _ri0;\n" +
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
		return "if(_lId == 0)\n" +
			   "{\n" +
			   "	for(int i = 0, j = _ri0; i < _otd; i++, j++)\n" +
			   "    {\n" +
			   "    	_result[j] = _localCache[i];\n" +
			   "    }\n" +
			   "}\n" +
			   "else if(_jobDone[_gId] == 0)\n" +
			   "{\n" +
			   "    //aggiorna _jobDone per dire che ha già scritto\n" +
			   "    _jobDone[_gId] = 1;\n" +
			   "}\n";
	}
}
