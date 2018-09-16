package org.apache.flink.api.tuple;

public class OclTupleUtils
{
	
	/**
	 * Gets the class corresponding to the tuple of the given arity (dimensions). For
	 * example, {@code getOclTupleClass(3)} will return the {@code Tuple3.class}.
	 *
	 * @param arity The arity of the tuple class to get.
	 * @return The tuple class with the given arity.
	 */
	@SuppressWarnings("unchecked")
	public static Class<? extends IOclTuple> getOclTupleClass(int arity) {
		if (arity < 0 || arity > 3) {
			throw new IllegalArgumentException("The tuple arity must be in [0, " + 3 + "].");
		}
		return (Class<? extends IOclTuple>) CLASSES[arity];
	}
	
	private static final Class<?>[] CLASSES = new Class<?>[] {
		Tuple0Ocl.class, Tuple1Ocl.class, Tuple2Ocl.class, Tuple3Ocl.class
	};
	
}
