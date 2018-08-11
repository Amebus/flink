package org.apache.flink.streaming.api.operators;

import static java.util.Objects.requireNonNull;

public abstract class OclAbstractUdfStreamOperator<OUT>
	extends AbstractStreamOperator<OUT>
	implements OutputTypeConfigurable<OUT>
{
	
	private static final long serialVersionUID = 1L;
	
	
	/** The user function. */
	protected final String mUserFunction;
	
	
	public OclAbstractUdfStreamOperator(String pUserFunction)
	{
		mUserFunction = requireNonNull(pUserFunction);
	}
}
