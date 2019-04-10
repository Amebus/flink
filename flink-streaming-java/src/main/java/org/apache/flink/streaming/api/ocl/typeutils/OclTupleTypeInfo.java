package org.apache.flink.streaming.api.ocl.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.streaming.api.ocl.tuple.IOclTuple;
import org.apache.flink.streaming.api.ocl.tuple.OclTupleUtils;

import java.util.*;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

public class OclTupleTypeInfo <T extends Tuple & IOclTuple> extends TupleTypeInfoBase<T>
{
	
	private static final long serialVersionUID = 1L;
	
	protected final String[] fieldNames;
	
	@Override
	protected TypeComparatorBuilder<T> createTypeComparatorBuilder()
	{
		return new TupleTypeComparatorBuilder();
	}
	
	@SuppressWarnings("unchecked")
	public OclTupleTypeInfo(TypeInformation<?>... types)
	{
		this((Class<T>) OclTupleUtils.getOclTupleClass(types.length), types);
	}
	
	public OclTupleTypeInfo(Class<T> tupleType, TypeInformation<?>... types)
	{
		super(tupleType, types);
		
		checkArgument(
			types.length <= Tuple.MAX_ARITY,
			"The tuple type exceeds the maximum supported arity.");
		
		this.fieldNames = new String[types.length];
		
		for (int i = 0; i < types.length; i++) {
			fieldNames[i] = "f" + i;
		}
	}
	
	/**
	 * Returns the names of the composite fields of this type. The order of the returned array must
	 * be consistent with the internal field index ordering.
	 */
	@Override
	public String[] getFieldNames()
	{
		return fieldNames;
	}
	
	@Override
	public Map<String, TypeInformation<?>> getGenericParameters()
	{
		Map<String, TypeInformation<?>> m = new HashMap<>(types.length);
		for (int i = 0; i < types.length; i++) {
			m.put("T" + i, types[i]);
		}
		return m;
	}
	
	/**
	 * Returns the field index of the composite field of the given name.
	 *
	 * @param fieldName
	 * @return The field index or -1 if this type does not have a field of the given name.
	 */
	@Override
	public int getFieldIndex(String fieldName)
	{
		for (int i = 0; i < fieldNames.length; i++)
		{
			if (fieldNames[i].equals(fieldName))
			{
				return i;
			}
		}
		return -1;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof OclTupleTypeInfo)
		{
			@SuppressWarnings("unchecked")
			OclTupleTypeInfo<T> other = (OclTupleTypeInfo<T>) obj;
			return other.canEqual(this) &&
				   super.equals(other) &&
				   Arrays.equals(fieldNames, other.fieldNames);
		} else
			{
			return false;
		}
	}
	
	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof TupleTypeInfo;
	}
	
	@Override
	public int hashCode() {
		return 31 * super.hashCode() + Arrays.hashCode(fieldNames);
	}
	
	@Override
	public String toString() {
		return "Java " + super.toString();
	}
	
	/**
	 * Creates a serializer for the type. The serializer may use the ExecutionConfig
	 * for parameterization.
	 *
	 * @param pExecutionConfig The config used to parameterize the serializer.
	 * @return A serializer for this type.
	 */
	@Override
	public TypeSerializer<T> createSerializer(ExecutionConfig pExecutionConfig)
	{
		TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[getArity()];
		for (int i = 0; i < types.length; i++)
		{
			fieldSerializers[i] = types[i].createSerializer(pExecutionConfig);
		}
		
		Class<T> tupleClass = getTypeClass();
		
		return new TupleSerializer<T>(tupleClass, fieldSerializers);
	}
	
	private class TupleTypeComparatorBuilder implements TypeComparatorBuilder<T>
	{
		
		private final ArrayList<TypeComparator> fieldComparators = new ArrayList<TypeComparator>();
		private final ArrayList<Integer> logicalKeyFields = new ArrayList<Integer>();
		
		@Override
		public void initializeTypeComparatorBuilder(int size)
		{
			fieldComparators.ensureCapacity(size);
			logicalKeyFields.ensureCapacity(size);
		}
		
		@Override
		public void addComparatorField(int fieldId, TypeComparator<?> comparator)
		{
			fieldComparators.add(comparator);
			logicalKeyFields.add(fieldId);
		}
		
		@Override
		public TypeComparator<T> createTypeComparator(ExecutionConfig config)
		{
			checkState(
				fieldComparators.size() > 0,
				"No field comparators were defined for the TupleTypeComparatorBuilder."
					  );
			
			checkState(
				logicalKeyFields.size() > 0,
				"No key fields were defined for the TupleTypeComparatorBuilder."
					  );
			
			checkState(
				fieldComparators.size() == logicalKeyFields.size(),
				"The number of field comparators and key fields is not equal."
					  );
			
			final int maxKey = Collections.max(logicalKeyFields);
			
			checkState(
				maxKey >= 0,
				"The maximum key field must be greater or equal than 0."
					  );
			
			TypeSerializer<?>[] fieldSerializers = new TypeSerializer<?>[maxKey + 1];
			
			for (int i = 0; i <= maxKey; i++)
			{
				fieldSerializers[i] = types[i].createSerializer(config);
			}
			
			return new TupleComparator<T>(
				listToPrimitives(logicalKeyFields),
				fieldComparators.toArray(new TypeComparator[fieldComparators.size()]),
				fieldSerializers
			);
		}
	}
	
	private static int[] listToPrimitives(ArrayList<Integer> ints)
	{
		int[] result = new int[ints.size()];
		for (int i = 0; i < result.length; i++)
		{
			result[i] = ints.get(i);
		}
		return result;
	}
	
	public static <X extends Tuple & IOclTuple> OclTupleTypeInfo<X> getBasicTupleTypeInfo(Class<?>... basicTypes)
	{
		if (basicTypes == null || basicTypes.length == 0) {
			throw new IllegalArgumentException();
		}
		
		TypeInformation<?>[] infos = new TypeInformation<?>[basicTypes.length];
		for (int i = 0; i < infos.length; i++) {
			Class<?> type = basicTypes[i];
			if (type == null) {
				throw new IllegalArgumentException("Type at position " + i + " is null.");
			}
			
			TypeInformation<?> info = BasicTypeInfo.getInfoFor(type);
			if (info == null) {
				throw new IllegalArgumentException("Type at position " + i + " is not a basic type.");
			}
			infos[i] = info;
		}
		
		@SuppressWarnings("unchecked")
		OclTupleTypeInfo<X> tupleInfo = new OclTupleTypeInfo<>(infos);
		return tupleInfo;
	}
}
