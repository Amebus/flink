package org.apache.flink.api.configuration;

import com.sun.org.apache.xerces.internal.impl.xpath.regex.RegularExpression;
import org.apache.flink.configuration.AbstractTupleDefinition;
import org.apache.flink.configuration.ITupleDefinition;
import org.apache.flink.configuration.TType;
import org.apache.flink.configuration.TupleVarDefinition;

import java.util.*;

public class JsonTupleDefinition extends AbstractTupleDefinition
{
	private static class Keys
	{
		private static final String T = "t";
	}
	
	private transient int mHashCode;
	private transient byte mArity;
	
	private String mName;
	private Map<String, TupleVarDefinition> mVarDefinitionMap = new HashMap<>();
	
	
	public JsonTupleDefinition()
	{
		mArity = 0;
	}
	
	public JsonTupleDefinition(String pName, Map<String, String> pTypesMap)
	{
		this();
		mName = pName;
//		mTypesMap = pTypesMap;
		postDeserialize(pTypesMap);
	}
	
	public JsonTupleDefinition(ITupleDefinition pDefinition)
	{
		this();
		
		mName = pDefinition.getName();
		mArity = pDefinition.getArity();
		
		if(pDefinition instanceof  JsonTupleDefinition)
			fromSameImplementation((JsonTupleDefinition) pDefinition);
		else
			fromDifferentImplementation(pDefinition);
		computeHashCode();
	}
	
	private void fromSameImplementation(JsonTupleDefinition pDefinition)
	{
		mVarDefinitionMap = new HashMap<>(pDefinition.mVarDefinitionMap);
	}
	
	private void fromDifferentImplementation(ITupleDefinition pDefinition)
	{
		mVarDefinitionMap = new HashMap<>();
		int vI = 0;
		Iterator<TupleVarDefinition> vIterator = pDefinition.tupleVarIterator();
		while ( vIterator.hasNext())
		{
			TupleVarDefinition vT = vIterator.next();
			TupleVarDefinition vTToInsert;
			
			if(vT.getIdentityValue() == null)
				vTToInsert = new TupleVarDefinition(vT.getCT().getT(), null);
			else
				vTToInsert = new TupleVarDefinition(vT.getCT().getT(), vT.getIdentityValue().toString());
			
			
			mVarDefinitionMap.put(getKey(vI), vTToInsert);
			vI++;
		}
	}
	
	private void postDeserialize(Map<String, String> pTypesMap)
	{
		final RegularExpression vExpression = new RegularExpression("t\\d+");
		Set<String> vKeySet = new HashSet<>(pTypesMap.keySet());
		vKeySet.forEach( x ->
						 {
							 if(vExpression.matches(x))
							 {
								 String vTempString = x.substring(1);
								 int vValue = Integer.parseInt(vTempString);
								 if( 0 <= vValue && vValue < T_LIMIT)
								 {
									 return;
								 }
							 }
							 pTypesMap.remove(x);
						 });
		
		mArity = (byte)pTypesMap.size();
		
		pTypesMap.forEach( (k,v) -> mVarDefinitionMap.put(k, getTupleVarDefinitionFromString(v)));
		
		computeHashCode();
	}
	
	private TupleVarDefinition getTupleVarDefinitionFromString(String pString)
	{
		if(pString.contains("-"))
		{
			String[] vChunks = pString.split("-");
			return new TupleVarDefinition(vChunks[0], vChunks[1]);
		}
		
		return new TupleVarDefinition(pString);
	}
	
	private void computeHashCode()
	{
		final int[] vResult = {17};
		reverseIterator().forEachRemaining( x -> vResult[0] += x.hashCode());
		mHashCode = vResult[0];
	}
	
	@Override
	public String getName()
	{
		return mName;
	}
	
	@Override
	public Byte getArity()
	{
		return mArity;
	}
	
	@Override
	public Iterator<TupleVarDefinition> tupleVarIterator()
	{
		return mVarDefinitionMap.values().iterator();
	}
	
	@Override
	public int hashCode()
	{
		return mHashCode;
	}
	
	@Override
	public boolean equals(Object other)
	{
		if (other == null)
		{
			return false;
		}
		if (other == this)
		{
			return true;
		}
		if (!(other instanceof JsonTupleDefinition))
		{
			return false;
		}
		JsonTupleDefinition rhs = ((JsonTupleDefinition) other);
		
		if (!getArity().equals(rhs.getArity()))
		{
			return false;
		}
		
		if(!getName().equals(rhs.getName()))
			return false;
		
		final boolean[] vResult = {true};
		Iterator<TType> vRhsIterator = rhs.iterator();
		
		forEach(x -> vResult[0] &= x.equals(vRhsIterator.next()));
		
		return vResult[0];
	}
	
	@Override
	public TupleVarDefinition getT(int pIndex)
	{
		return getT(getKey(pIndex));
	}
	
	private TupleVarDefinition getT(String pKey)
	{
		return mVarDefinitionMap.get(pKey);
	}
	
	private String getKey(int pIndex)
	{
		return Keys.T + pIndex;
	}
}
