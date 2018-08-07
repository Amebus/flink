package org.apache.flink.configuration;

public interface IOclSettings
{
	IOclContextOptions getContextOptions();
	
	IOclKernelsOptions getOclKernelOptions();
}
