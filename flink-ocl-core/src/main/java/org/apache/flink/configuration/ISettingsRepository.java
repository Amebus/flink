package org.apache.flink.configuration;

public interface ISettingsRepository
{
	IOclContextOptions getContextOptions();
	
	IOclKernelsOptions getKernelsOptions();
}
