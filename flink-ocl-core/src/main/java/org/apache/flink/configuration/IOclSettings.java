package org.apache.flink.configuration;

import java.io.Serializable;

public interface IOclSettings extends Serializable
{
	IOclContextOptions getContextOptions();
	
	IOclKernelsOptions getOclKernelOptions();
}
