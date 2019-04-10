package org.apache.flink.streaming.configuration;

import java.io.Serializable;

public interface IOclSettings extends Serializable
{
	IOclContextOptions getContextOptions();
	
	IOclKernelsOptions getOclKernelOptions();
}
