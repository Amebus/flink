package org.apache.flink.streaming.helpers;

public class Constants
{
	public static final String RESOURCES_DIR = "src/test/resources";
	public static final String FUNCTIONS_DIR = RESOURCES_DIR + "/functions";
	public static final String TUPLES_DIR = RESOURCES_DIR + "/tuples";
	
	public static final String OCL_FILTER_TEST_DIR = RESOURCES_DIR + "/OclFilterTest";
	public static final String OCL_MAP_TEST_DIR = RESOURCES_DIR + "/OclMapTest";
	public static final String OCL_REDUCE_TEST_DIR = RESOURCES_DIR + "/OclReduceTest";
	
	public static final Integer ITV_0 = -12;
	public static final Double DTV_0 = -12.57;
	public static final String STV_0 = "string";
	
	
	public static final Integer ITV_1 = 0xFFFFFFA8;
	public static final Integer ITV_2 = 0x00015BD5;
	public static final Integer ITV_3 = 0xFFF6D540;
	public static final Integer ITV_4 = 0x05F6BD93;
	
	public static final Double DTV_1 = Double.longBitsToDouble(0x00015BD5FFFFFFA8L);
	public static final Double DTV_2 = Double.longBitsToDouble(0xFFF6D54000015BD5L);
	public static final Double DTV_3 = Double.longBitsToDouble(0x05F6BD93FFF6D540L);
	public static final Double DTV_4 = Double.longBitsToDouble(0xFFFFFFA805F6BD93L);
	
	public static final String STV_1 = "string";
	public static final String STV_2 = "i topi non avevano nipoti";
	public static final String STV_3 = "itopin onaveva non ipot i";
	public static final String STV_4 = "gnirts";
}
