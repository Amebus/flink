package org.apache.flink.configuration;

import java.io.Serializable;

public interface ISettingsRepository extends Serializable
{
	IOclContextOptions getContextOptions();
	
	IOclKernelsOptions getKernelsOptions();
}
