package org.apache.flink.streaming.api.environment;

import org.apache.flink.api.bridge.OclContext;
import org.apache.flink.api.common.JobExecutionResult;

public class OclStreamExecutionEnvironment
{
	
	private StreamExecutionEnvironment mStreamExecutionEnvironment;
	private OclContext mOclContext;
	
	public OclStreamExecutionEnvironment(StreamExecutionEnvironment pStreamExecutionEnvironment, OclContext pOclContext)
	{
		mStreamExecutionEnvironment = pStreamExecutionEnvironment;
		mOclContext = pOclContext;
	}
	
	/**
	 * Triggers the program execution. The environment will execute all parts of
	 * the program that have resulted in a "sink" operation. Sink operations are
	 * for example printing results or forwarding them to a message queue.
	 *
	 * <p>The program execution will be logged and displayed with the provided name
	 *
	 * @param jobName Desired name of the job
	 * @return The result of the job execution, containing elapsed time and accumulators.
	 *
	 * @throws Exception which occurs during job execution.
	 */
	public JobExecutionResult execute(String jobName) throws Exception
	{
		mOclContext.open();
		
		JobExecutionResult vJobExecutionResult = mStreamExecutionEnvironment.execute(jobName);
		
		mOclContext.close();
		
		return vJobExecutionResult;
	}
}
