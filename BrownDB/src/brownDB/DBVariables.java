package brownDB;


public interface DBVariables 
{
	//String variables
		static String DIR_NAME = "data";
		static String CAT_NAME = "catalog";
		static String USER_DAT = "user_data";
		static String TBL_EXT = ".tbl";
		static String IND_EXT = ".idx";
		static String DB_NAME = "BrownDB";
		static String PRO_LAB = "BrownDB> ";
		static String TBLS_NAME = "davisbase_tables";
		static String COLS_NAME = "davisbase_columns";
		
		//rightmost table value
		static int RIGHT = 2147483647;
		
		//page size variable
		static int pageSize = 512;
		
		public static boolean isExit = false;
		
		static String DATETIME_FORMAT = "YYYY_MM_DD_hh:mm:ss";
		static String DATE_FORMAT = "YYYY_MM_DD";
		
		//data type serial code identifier variables
		static byte ONE_NULL = 0x00;
		static byte TWO_NULL = 0x01;
		static byte FOUR_NULL = 0x02;
		static byte EIGHT_NULL = 0x03;
		static byte TINYINT = 0x04;
		static byte SMALLINT = 0x05;
		static byte INT = 0x06;
		static byte BIGINT = 0x07;
		static byte REAL = 0x08;
		static byte DOUBLE = 0x09;
		static byte DATETIME = 0x0A;
		static byte DATE = 0x0B;
		static byte TEXT = 0x0C;
		
		//BTree page identifier variables
		static byte INT_INDEX = 0x02;
		static byte INT_TABLE = 0x05;
		static byte LEAF_INDEX = 0x0A;
		static byte LEAF_TABLE = 0x0D;
		
		//Page Offset Identifiers
		static byte PAGE_TYPE = 0x00;
		static byte NUM_REC = 0x01;
		static byte REC_START = 0x03;
		static byte RIGHT_PAGE_NUM = 0x05;
		static byte LOC = 0x09;
		
		//Cell variables
		static int CELL_HEAD = 6;
		
		//content size in bytes for records
		static int NULL1 = 1;
		static int NULL2 = 2;
		static int NULL4 = 4;
		static int NULL8 = 8;
		static int TINYINT_SIZE = 1;
		static int SMALLINT_SIZE = 2;
		static int INT_SIZE = 4;
		static int BIGINT_SIZE = 8;
		static int REAL_SIZE = 4;
		static int DOUBLE_SIZE = 8;
		static int DATETIME_SIZE = 8;
		static int DATE_SIZE = 8;

}
