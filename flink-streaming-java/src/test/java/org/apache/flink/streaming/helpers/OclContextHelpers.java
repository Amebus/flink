package org.apache.flink.streaming.helpers;

import org.apache.flink.api.bridge.OclContext;
import org.apache.flink.api.configuration.JsonSettingsRepository;
import org.apache.flink.api.configuration.JsonTupleRepository;
import org.apache.flink.api.engine.JsonUserFunctionRepository;
import org.apache.flink.api.engine.builder.options.DefaultsValues;
import org.apache.flink.api.tuple.IOclTuple;
import org.apache.flink.api.tuple.Tuple1Ocl;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.stream.Stream;

public class OclContextHelpers
{
	public static abstract class OclTestClass
	{
		private OclContext mOclContext;
		
		@Before
		public void init()
		{
			setOclContext();
		}
		
		protected String getResourcesDirectory()
		{
			return Constants.RESOURCES_DIR;
		}
		
		protected String getFunctionsDirectory()
		{
			return Constants.FUNCTIONS_DIR;
		}
		protected abstract String getFunctionsFileName();
		
		protected String getTuplesDirectory()
		{
			return Constants.TUPLES_DIR;
		}
		protected abstract String getTuplesFileName();
		
		protected OclContext getCurrentOclContext()
		{
			return mOclContext;
		}
		
		protected void setOclContext(OclContext pOclContext)
		{
			mOclContext = pOclContext;
		}
		
		protected void setOclContext()
		{
			setOclContext(getNewOclContext());
		}
		
		public OclContext getNewOclContext()
		{
			return new OclContext(new JsonSettingsRepository(getResourcesDirectory()),
								  new JsonTupleRepository
									  .Builder(getTuplesDirectory())
									  .setFileName(getFunctionsFileName())
									  .build(),
								  new JsonUserFunctionRepository
									  .Builder(getFunctionsDirectory())
									  .setFileName(getTuplesFileName())
									  .build(),
								  new DefaultsValues.DefaultOclContextMappings());
		}
		
		protected void doInCurrentContext(FIInContextAction pAction)
		{
			OclContext vOclContext = getCurrentOclContext();
			vOclContext.open();
			pAction.doInContext(getCurrentOclContext());
			vOclContext.close();
		}
		
		protected <R> R getWithCurrentContext(FIInContextFunction<R> pFunction)
		{
			R vResult;
			OclContext vOclContext = getCurrentOclContext();
			vOclContext.open();
			vResult = pFunction.getInContext(vOclContext);
			vOclContext.close();
			return vResult;
		}
		
		@FunctionalInterface
		public interface FIInContextAction
		{
			void doInContext(OclContext pOclContext);
		}
		
		@FunctionalInterface
		public interface  FIInContextFunction<R>
		{
			R getInContext(OclContext pOclContext);
		}
	}
	
	public static class OclRandomGenerator extends Random
	{
		public static final int DEFAULT_STRING_LENGTH = 20;
		
		public static class Sources
		{
			public static final String UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
			public static final String LOWER = UPPER.toLowerCase(Locale.ROOT);
			public static final String DIGITS = "0123456789";
			public static final String ALPHANUM = UPPER + LOWER + DIGITS;
		}
		
		
		
		/**
		 * Creates a new random number generator. This constructor sets
		 * the seed of the random number generator to a value very likely
		 * to be distinct from any other invocation of this constructor.
		 */
		public OclRandomGenerator()
		{
		}
		/**
		 * Creates a new random number generator using a single {@code long} seed.
		 * The seed is the initial value of the internal state of the pseudorandom
		 * number generator which is maintained by method {@link #next}.
		 *
		 * <p>The invocation {@code new Random(seed)} is equivalent to:
		 * <pre> {@code
		 * Random rnd = new Random();
		 * rnd.setSeed(seed);}</pre>
		 *
		 * @param seed the initial seed
		 * @see #setSeed(long)
		 */
		public OclRandomGenerator(long seed)
		{
			super(seed);
		}
		
		
		
		public String nextString()
		{
			return nextString(DEFAULT_STRING_LENGTH);
		}
		
		public String nextString(int pLength)
		{
			return nextString(pLength, pLength);
		}
		
		public String nextString(int pMinLength, int pMaxLength)
		{
			return nextString(pMinLength, pMaxLength, Sources.ALPHANUM );
		}
		
		public String nextString(int pMinLength, int pMaxLength, String pSource)
		{
			if (pMinLength < 1)
			{
				throw new IllegalArgumentException("Minimum length must be grater than zero.");
			}
			
			if(pMaxLength < pMinLength)
			{
				throw  new IllegalArgumentException("Maximum length must be grater or equal than the Minimum length.");
			}
			
			StringBuilder vBuilder = new StringBuilder(pMaxLength);
			
			for (int i = 0 ; i < pMinLength; i++)
			{
				vBuilder.append(nextChar(pSource));
			}
		
			boolean vStop = pMinLength == pMaxLength;
			for (int i =  pMinLength; !vStop && i < pMaxLength; i++)
			{
				vStop = nextBoolean();
				if(!vStop)
				{
					vBuilder.append(nextChar(pSource));
				}
			}
			
			return vBuilder.toString();
		}
		
		public char nextChar()
		{
			return nextChar(Sources.ALPHANUM);
		}
		
		public char nextChar(String pSource)
		{
			int vSourceBound = pSource.length();
			checkBound(vSourceBound);
			return pSource.charAt(nextInt(vSourceBound));
		}
		
		public char nextChar(char[] pSource)
		{
			int vSourceBound = pSource.length;
			checkBound(vSourceBound);
			return pSource[nextInt(vSourceBound)];
		}
		
		protected void checkBound(int pSourceBound)
		{
			if (pSourceBound < 1)
			{
				throw new IllegalArgumentException("The source must contains at least one element.");
			}
		}
	}
	
	public static List<IOclTuple> GetIntegerZeroMeanTuples()
	{
		int vBound = 1000000;
		int vMeanDisplacement = vBound/2;
		OclRandomGenerator vRnd = new OclRandomGenerator();
		int vMax = 50000;
		
		return GetTestTuple1OclList(vMax, () -> vRnd.nextInt(vBound) - vMeanDisplacement);
	}
	
	public static List<IOclTuple> GetIntegerTestTuples()
	{
		List<IOclTuple> vConstTuples = new ArrayList<>();
		vConstTuples.add(new Tuple1Ocl<>(-1679099059));
		vConstTuples.add(new Tuple1Ocl<>(528136394));
		vConstTuples.add(new Tuple1Ocl<>(-1528862540));
		vConstTuples.add(new Tuple1Ocl<>(-1348335996));
		
		OclRandomGenerator vRnd = new OclRandomGenerator();
		
		return GetTestTuple1OclList(vConstTuples, vRnd::nextInt);
	}
	
	public static List<IOclTuple> GetDoubleTestTuples()
	{
		double vBound = 1000000;
		double vMeanDisplacement = vBound/2;
		OclRandomGenerator vRnd = new OclRandomGenerator();
		return GetTestTuple1OclList(() -> vRnd.nextDouble() * vBound - vMeanDisplacement);
	}
	
	public static List<IOclTuple> GetStringTestTuples()
	{
		return GetStringTestTuples(20);
	}
	
	public static List<IOclTuple> GetStringTestTuples(int pMaxStringLength)
	{
		OclRandomGenerator vRnd = new OclRandomGenerator();
		return GetTestTuple1OclList(() -> vRnd.nextString(15, pMaxStringLength));
	}
	
	public static <R> List<IOclTuple> GetTestTuple1OclList(Tuple1ValueGetter<R> pTuple1ValueGetter)
	{
		return GetTestTuple1OclList(50000, pTuple1ValueGetter);
	}
	
	public static <R> List<IOclTuple> GetTestTuple1OclList(
		List<IOclTuple> pConstTuples,
		Tuple1ValueGetter<R> pTuple1ValueGetter)
	{
		return GetTestTuple1OclList(pConstTuples,50000, pTuple1ValueGetter);
	}
	
	public static <R> List<IOclTuple> GetTestTuple1OclList(
		int pMax,
		Tuple1ValueGetter<R> pTuple1ValueGetter)
	{
		return GetTestTuple1OclList(new ArrayList<>(), pMax, pTuple1ValueGetter);
	}
	
	public static <R> List<IOclTuple> GetTestTuple1OclList(
		List<IOclTuple> pConstTuples,
		int pMax,
		Tuple1ValueGetter<R> pTuple1ValueGetter)
	{
		List<IOclTuple> vTuples = new ArrayList<>(pMax);
		vTuples.addAll(pConstTuples);
		
		int vMaxConstDiff = pConstTuples.size();
		for (int vI = 0; vI < pMax - vMaxConstDiff; vI++)
		{
			vTuples.add(new Tuple1Ocl<>(pTuple1ValueGetter.getValue()));
		}
		return vTuples;
	}
	
	public static class TupleListInfo
	{
		private List<IOclTuple> mOclTuples;
		
		public TupleListInfo(List<IOclTuple> pOclTuples)
		{
			mOclTuples = pOclTuples;
		}
		
		public List<IOclTuple> getOclTuples()
		{
			return mOclTuples;
		}
		
		public Stream<IOclTuple> getTuplesStream()
		{
			return mOclTuples.stream();
		}
		
		public int size()
		{
			return mOclTuples.size();
		}
		
		public int count(java.util.function.Predicate< ? super IOclTuple> pFilter)
		{
			return (int) getTuplesStream().filter(pFilter).count();
		}
		
		public int countGreaterThan(int pLimit)
		{
			return count( pIOclTuple -> pIOclTuple.<Integer>getField(0) > pLimit);
		}
		
		public int countGreaterThan(double pLimit)
		{
			return count( pIOclTuple -> pIOclTuple.<Double>getField(0) > pLimit);
		}
		
		public int countLessOrEqualThan(int pLimit)
		{
			return size() - countGreaterThan(pLimit);
		}
		
		public int countLessOrEqualThan(double pLimit)
		{
			return size() - countGreaterThan(pLimit);
		}
	}
	
	@FunctionalInterface
	private interface Tuple1ValueGetter<R>
	{
		R getValue();
	}
	
	@FunctionalInterface
	public interface OclTupleListGetter
	{
		List<IOclTuple> GetTupleList();
	}
}
