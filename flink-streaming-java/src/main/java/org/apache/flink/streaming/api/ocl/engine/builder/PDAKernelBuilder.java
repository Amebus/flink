package org.apache.flink.streaming.api.ocl.engine.builder;

import org.apache.flink.streaming.api.ocl.common.IBuilder;

import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class PDAKernelBuilder implements IBuilder<String>
{
	private final IKernelTemplatesRepository mKernelTemplatesRepository;
	private final Stack<String> mStack;
	private String mTemplate;
	
	public PDAKernelBuilder(IKernelTemplatesRepository pKernelTemplatesRepository)
	{
		mStack = new Stack<>();
		mKernelTemplatesRepository = pKernelTemplatesRepository;
		mTemplate = getRootTemplateCode();
		
		if(mTemplate == null)
		{
			throw new IllegalArgumentException("The template can't be null");
		}
	}
	
	protected int pushTemplateToStack(String pTemplate)
	{
		mStack.push(pTemplate);
		return getStackedTemplates();
	}
	protected String popTemplateFromStack()
	{
		return mStack.pop();
	}
	protected int getStackedTemplates()
	{
		return mStack.size();
	}
	
	protected IKernelTemplatesRepository getKernelTemplatesRepository()
	{
		return mKernelTemplatesRepository;
	}
	
	protected String getTemplateCode(String vTemplate)
	{
		return getKernelTemplatesRepository().getTemplateCode(vTemplate);
	}
	protected String getRootTemplateCode()
	{
		return getKernelTemplatesRepository().getRootTemplateCode();
	}
	
	protected abstract String getHelperFunctions();
	protected abstract String getDefines();
	protected abstract String getKernelCode();
	
	@Override
	public String build()
	{
		return parseTemplateCode(getRootTemplateCode());
	}
	
 	private final String START_SEQUENCE = "<[";
	private final String END_SEQUENCE = "]>";
	private final String TEMPLATE_STRING_PATTERN = "<\\[.+]>";
	private final Pattern TEMPLATE_PATTERN = Pattern.compile(TEMPLATE_STRING_PATTERN);
	
	private final int START_SEQUENCE_LENGTH = START_SEQUENCE.length();
	private final int END_SEQUENCE_LENGTH = END_SEQUENCE.length();
	
	protected Pattern getTemplatePattern()
	{
		return TEMPLATE_PATTERN;
	}
	
	protected String parseTemplateCode(String pTemplateCode)
	{
		StringBuilder vBuilder = new StringBuilder(pTemplateCode);
		Matcher vMatcher = getTemplatePattern().matcher(pTemplateCode);
		
		while (vMatcher.find())
		{
			int vStart = vMatcher.start();
			int vEnd = vMatcher.end();
			String vCode = vMatcher.group();
			String vTemplateCode = getTemplateCode(vCode);
			
			vBuilder.replace(vStart, vEnd, parseTemplateCode(vTemplateCode));
			vMatcher.reset(vBuilder);
		}
		return vBuilder.toString();
	}
	
	protected boolean isStartTemplateCode(String pTemplateCode, int pStartIndex)
	{
		return pTemplateCode.startsWith(START_SEQUENCE, pStartIndex);
	}
	
	protected boolean isEndTemplateCode(String pTemplateCode, int pStartIndex)
	{
		return pTemplateCode.startsWith(END_SEQUENCE, pStartIndex);
	}
	
	protected boolean containsTheSequence(String pString, int pStartIndex, String pSequence)
	{
		return pString.indexOf(pSequence, pStartIndex) > -1;
	}
	
	@FunctionalInterface
	interface FIAaaa
	{
		String getTemplateCode();
	}
}
