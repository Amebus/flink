package org.apache.flink.streaming.helpers;

import org.apache.commons.math3.random.RandomAdaptor;
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
	
	public static List<IOclTuple> GetIntegerZeroMeanTuples()
	{
		int vBound = 1000000;
		int vMeanDisplacement = vBound/2;
		Random vRnd = new Random();
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
		
		Random vRnd = new Random();
		int vMax = 50000;
		
		return GetTestTuple1OclList(vConstTuples, vMax, vRnd::nextInt);
	}
	
	public static List<IOclTuple> GetDoubleTestTuples()
	{
		double vBound = 1000000;
		double vMeanDisplacement = vBound/2;
		Random vRnd = new Random();
		int vMax = 50000;
		return GetTestTuple1OclList(vMax, () -> vRnd.nextDouble() * vBound - vMeanDisplacement);
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
