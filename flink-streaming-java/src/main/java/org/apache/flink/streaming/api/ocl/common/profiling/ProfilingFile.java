package org.apache.flink.streaming.api.ocl.common.profiling;

import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class ProfilingFile
{
	private List<ProfilingRecord> mProfilingRecords;
	
	public ProfilingFile()
	{
		mProfilingRecords = new LinkedList<>();
	}
	
	public void addProfilingRecord(ProfilingRecord pProfilingRecord)
	{
		mProfilingRecords.add(pProfilingRecord);
	}
	
	public void writeToCsv(String pPathToFile)
	{
		if (!pPathToFile.toLowerCase().endsWith(".csv"))
			pPathToFile = pPathToFile + ".csv";
		CsvOutputFormat<Tuple7<String, String, Long, Long, Long, Long, Long>> vCsvOutputFormat =
			new CsvOutputFormat(new Path(pPathToFile), ";");
		vCsvOutputFormat.setWriteMode(FileSystem.WriteMode.NO_OVERWRITE);
		
		for (ProfilingRecord pr : mProfilingRecords)
		{
			Tuple7<String, String, Long, Long, Long, Long, Long> vRecord = new Tuple7<>();
			vRecord.f0 = pr.getKernelName();
			vRecord.f1 = pr.getKernelType();
			vRecord.f2 = pr.getTotal();
			vRecord.f3 = pr.getSerialization();
			vRecord.f4 = pr.getDeserialization();
			vRecord.f5 = pr.getJavaToC();
			vRecord.f6 = pr.getKernelComputation();
			
			try
			{
				vCsvOutputFormat.writeRecord(vRecord);
			}
			catch (IOException pE)
			{
				throw new IllegalArgumentException("It wasn't possible to create the file: " + new Path(pPathToFile).getPath(), pE);
			}
		}
		
	}
}
