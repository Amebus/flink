package org.apache.flink.streaming.api.ocl.engine.builder;

public interface IKernelTemplatesRepository
{
	String getRootTemplateCode();
	
	String getTemplateCode(String pTemplateName);
}
