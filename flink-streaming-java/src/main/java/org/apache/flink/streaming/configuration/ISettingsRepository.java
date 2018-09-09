package org.apache.flink.streaming.configuration;

import java.io.Serializable;

public interface ISettingsRepository extends Serializable
{
	IOclContextOptions getContextOptions();
	
	IOclKernelsOptions getKernelsOptions();
}
