package brownDB;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import brownDB.DBPage;
import brownDB.DBIndex;

public class DBTable implements DBVariables
{
	public static RandomAccessFile davisbase_tables;
	public static RandomAccessFile davisbase_columns;
	public static RandomAccessFile table_roots;
	public static RandomAccessFile new_table;
	//function to test methods and program
	public static void test()
	{
		
		
	}
	//method to display the tables
	public static void Show(String userCommand)
	{
		System.out.println("--------------");
		
		selectAll("davisbase_tables");
	}
	//method to create a table
	public static void createTable(String table, String[] columns, String[] typ, String[] nullable)
	{
		try{	
			//create directory and table file
			File dir = new File(DIR_NAME + "/"+USER_DAT + "/" +table);
			dir.mkdir();
			RandomAccessFile file = new RandomAccessFile(DIR_NAME + "/"+USER_DAT + "/" +table+"/"+ table + TBL_EXT, "rw");
			file.setLength(pageSize);
			file.seek(0);
			file.writeByte(0x0D);
			file.writeShort(0);
			file.writeShort(pageSize-1);
			file.writeInt(RIGHT);
			file.close();
		}
		catch (Exception excep)
		{
			System.out.println("Error at createTable, first try" + excep);
		}
			
		try
		{
			davisbase_tables = new  RandomAccessFile(DIR_NAME + "/" + CAT_NAME + "/" + TBLS_NAME + TBL_EXT, "rw");
			//add the tablename to the davisbase_tables table
			String[] data = new String[1];
			data[0] = table;
			String[] type1 = new String[1];
			type1[0] = "TEXT";			
			int right_leaf = DBPage.getRightLeaf(davisbase_tables, 1);
			short offset = DBPage.getOffset(davisbase_tables, right_leaf);
			int row = DBPage.getRowID(davisbase_tables, right_leaf);
			int payload = DBPage.getPayloadSize(data, type1);
			boolean room = DBPage.checkLeaf(davisbase_tables, right_leaf, payload);
			if(room == true)
			{
				DBPage.leafInsert(davisbase_tables, right_leaf, offset, row, data, type1);
			}
			else
			{
				DBPage.increasePage(davisbase_tables, "davisbase_tables");
				right_leaf = DBPage.getRightLeaf(davisbase_tables, 1);
				offset = DBPage.getOffset(davisbase_tables, right_leaf);
				DBPage.leafInsert(davisbase_tables, right_leaf, offset, row, data, type1);		
				
			}
		}
		catch (Exception excep)
		{
			System.out.println("Error at createTable, 2nd try" + excep);
		}
		try
		{
			
			davisbase_columns = new RandomAccessFile(DIR_NAME + "/" + CAT_NAME + "/" + COLS_NAME + TBL_EXT, "rw");
			//insert columns into davisbase_columns
			String[] val = new String[5];
			Byte counter = 0;
			for( int i = 0; i < columns.length; i++)
			{
				counter++;
				val[0] = table;
				val[1] = columns[i];
				val[2] = typ[i];
				val[3] = Byte.toString(counter);
				val[4] = nullable[i];
				
				insertInto("davisbase_columns", val);
			}
		}
		catch (Exception excep)
		{
			System.out.println("Error at createTable, 2nd try" + excep);
		}
		try
		{
			table_roots = new RandomAccessFile(DIR_NAME + "/" + CAT_NAME + "/table_roots" + TBL_EXT, "rw");
			String[] type1 = new String[2];
			type1[0] = "TEXT";
			type1[1] = "INT";
			String[] data = new String[2];
			data[0] = table;
			data[1] = "1";
			//insert the first leaf as the root into the roots tables
			int right_leaf = DBPage.getRightLeaf(table_roots, 1);
			short offset = DBPage.getOffset(table_roots, right_leaf);
			int row = DBPage.getRowID(table_roots, right_leaf);
			int payload = DBPage.getPayloadSize(data, type1);
			boolean room = DBPage.checkLeaf(table_roots, right_leaf, payload);
			if(room == true)
			{
				DBPage.leafInsert(table_roots, right_leaf, offset, row, data, type1);
			}
			else
			{
				DBPage.increasePage(table_roots, "table_roots");
				right_leaf = DBPage.getRightLeaf(table_roots, 1);
				offset = DBPage.getOffset(table_roots, right_leaf);
				DBPage.leafInsert(table_roots, right_leaf, offset, row, data, type1);	
			}
			
		}
		catch(Exception e)
		{
			System.out.println("Error at createTable: third try." + e);
		}
	}
	//method to drop a table
	public static void dropTable(String table)
	{
		try
		{
			//Delete the table file directory, table, and all indexes.
			File table1 = new File(DIR_NAME+ "\\" + USER_DAT + "\\"+ table);
			String[] files = table1.list();
			for(String f:files)
			{
				File dropFile = new File(DIR_NAME+"\\"+USER_DAT+"\\"+table,f);
				dropFile.delete();
			}
			table1.delete();
		}
		catch (Exception excep)
		{
			System.out.println("Error at drop, first try: " + excep);
		}
		
		try
		{
			davisbase_tables = new RandomAccessFile(DIR_NAME + "/" + CAT_NAME + "/" + TBLS_NAME + TBL_EXT, "rw");
			// delete the offset from davisbase_tables
			short off = DBPage.findOffset(davisbase_tables, table);
			int length = CELL_HEAD + 2 + table.length();
			davisbase_tables.seek(off);
			for(int i = 0; i < length; i++)
			{
				davisbase_tables.writeByte(0);
			}
			DBPage.removeOffset(davisbase_tables, off);
		}
		catch (Exception excep)
		{
			System.out.println("Error at drop, 2nd try: " + excep);
		}
		try
		{
			davisbase_columns = new RandomAccessFile(DIR_NAME + "/" + CAT_NAME + "/" + COLS_NAME + TBL_EXT, "rw");
			// delete the data from the davisbase_columns
			boolean found = false;
			short off = 0;
			while(found == false)
			{
				off = DBPage.findOffset(davisbase_columns, table);
				if(off != 0)
				{
					DBPage.removeOffset(davisbase_columns, off);
					
				}
				else
				{
					found = true;
				}
			}
			
		
			System.out.println("Table deleted successfully.");
		}
		catch(Exception excep)
		{
			System.out.println("Error at drop" + excep);
		}
	}
	//method to insert columns into table.
	public static void insertInto(String table, String[] values)
	{
		File dir = new File(DIR_NAME + "\\" + USER_DAT + "\\" + table);
		
		if(dir.exists())
		{
			String[] type = new String[0];
			try 
			{
				//get the column data types for the table
				davisbase_columns = new RandomAccessFile(DIR_NAME + "/" + CAT_NAME + "/" + COLS_NAME + TBL_EXT, "rw");
				type =	DBPage.getColTypes(davisbase_columns, table);
				System.out.println("type length: " + type.length);
				
			}
			catch (FileNotFoundException excep) 
			{
				System.out.println("Error in insertinto first if: " + excep);
			}
			try
			{
				RandomAccessFile file = new RandomAccessFile(DIR_NAME + "/" + USER_DAT +"/" + table + "/" + table + TBL_EXT, "rw");
				//get the right most leaf to insert into
				int rightmost = DBPage.getRightLeaf(file, 1);
				//get payloadsize for the record to check the space availability
				int payload = DBPage.getPayloadSize(values, type);
				int rowid = DBPage.getRowID(file, rightmost);
				//check the space available for insertion
				boolean room = DBPage.checkLeaf(file, rightmost, payload);
				//2 options based on the boolean value, if true insert, if false increase table size and insert
				if(room == true)
				{
					//get the last offset
					short offset = DBPage.getOffset(file, rightmost);
					System.out.println("offset: " + offset);
					//add the record to the table
					DBPage.leafInsert(file, rightmost, offset, rowid, values, type);
					
				}
				else
				{
					DBPage.increasePage(file, table);
					rightmost = DBPage.getRightLeaf(file, 1);
					short offset = DBPage.getOffset(file, rightmost);
					//add the record to the table
					DBPage.leafInsert(file, rightmost, offset, rowid, values, type);
					
				}				
			} 
			catch (FileNotFoundException excep) 
			{
				System.out.println("Error in insertinto first if: " + excep);
			}
			
		}
		else if(table.equals("davisbase_tables")||table.equals("davisbase_columns"))
		{
			try 
			{
				RandomAccessFile file2 = new RandomAccessFile(DIR_NAME + "/" + CAT_NAME +"/" + table + TBL_EXT, "rw");
				
				String[] type = new String[0];
				type = DBPage.getColTypes(davisbase_columns, table);
				//get the right most leaf to insert into
				int rightmost = DBPage.getRightLeaf(file2, 1);
				//get payloadsize for the record to check the space availability
				int payload = DBPage.getPayloadSize(values, type);
				//check the space available for insertion
				boolean room = DBPage.checkLeaf(file2, rightmost, payload);
				//2 options based on the boolean value, if true insert, if false increase table size and insert
				int rowid = DBPage.getRowID(file2, 1);
				short offset = DBPage.getOffset(file2, rightmost);
				if(room == true)
				{
					//System.out.println("offset: " + offset);
					//add the record to the table
					DBPage.leafInsert(file2, rightmost, offset, rowid, values, type);
					
				}
				else
				{
					DBPage.increasePage(file2, table);
					rightmost = DBPage.getRightLeaf(file2, 1);
					//System.out.println("rightmost leaf: "+rightmost);
					offset = DBPage.getOffset(file2, rightmost);
					//System.out.println("offset: " + offset);
					//add the record to the table
					DBPage.leafInsert(file2, rightmost, offset, rowid, values, type);
					
				}
				
				
			} 
			catch (FileNotFoundException excep) 
			{
				System.out.println("Error in insertinto elseif: " + excep);
			}
		}
		else
		{
			System.out.println("The table does not exist, please create the table first.");
		}
	}
	//method to update a record in a table
	public static void update(String table ,String colName, String value, String operator)
	{
		try
		{
			RandomAccessFile file;
			if(table.equals("davisbase_tables")||table.equals("davisbase_columns"))
			{
				file = new RandomAccessFile(DIR_NAME + "/" + CAT_NAME + "/" + table + TBL_EXT, "rw");
			}
			else
			{
				file = new RandomAccessFile(DIR_NAME + "/" + USER_DAT + "/" + table + "/" + table + TBL_EXT, "rw");
			}
			Byte pos = DBPage.getColPos(table, colName);
			
			switch(operator)
			{
			case "=":
				
				break;
			case "<":
				
				break;
			case ">":
				
				break;
			case ">=":
				
				break;
			case "<=":
				
				break;
			case "<>":
				
				break;
			}
			
			file.close();						
		}
		catch (Exception excep)
		{
			System.out.println("Error in update: " + excep);
		}
	}
	//method to delete a specific record from the table
	public static void deleteFrom(String table, String colName, String value, String operator)
	{
		try
		{
			RandomAccessFile file;
			if(table.equals("davisbase_tables")||table.equals("davisbase_columns"))
			{
				file = new RandomAccessFile(DIR_NAME + "/" + CAT_NAME + "/" + table + TBL_EXT, "rw");
			}
			else
			{
				file = new RandomAccessFile(DIR_NAME + "/" + USER_DAT + "/" + table + "/" + table + TBL_EXT, "rw");
			}
			Byte pos = DBPage.getColPos(table, colName);
			
			switch(operator)
			{
			case "=":
				
				break;
			case "<":
				
				break;
			case ">":
				
				break;
			case ">=":
				
				break;
			case "<=":
				
				break;
			case "<>":
				
				break;
			}
		
			file.close();
		}
		catch (Exception excep)
		{
			System.out.println("Error in update: " + excep);
		}
	}
	//method for selecting with where clause
	public static void selectWhere(String table, String colName, String value, String operator)
	{
		try
		{
			RandomAccessFile file;
			if(table.equals("davisbase_tables")||table.equals("davisbase_columns"))
			{
				file = new RandomAccessFile(DIR_NAME + "/" + CAT_NAME + "/" + table + TBL_EXT, "rw");
			}
			else
			{
				file = new RandomAccessFile(DIR_NAME + "/" + USER_DAT + "/" + table + "/" + table + TBL_EXT, "rw");
			}
			switch(operator)
			{
			case "=":
				
				break;
			case "<":
			
				break;
			case ">":
				
				break;
			case ">=":
				
				break;
			case "<=":
				
				break;
			case "<>":
				
				break;
			}
			
			file.close();
							
		}
		catch (Exception excep)
		{
			System.out.println("Error in update: " + excep);
		}
	}	
	//method for selection of all
	public static void selectAll(String table)
		{
			try
			{
				System.out.println();
				davisbase_columns = new RandomAccessFile(DIR_NAME + "/" + CAT_NAME + "/" + COLS_NAME + TBL_EXT, "rw");
				String[] columnName = DBPage.getColNames(table);
				System.out.print("row_id  ");
				for(int i = 0; i < columnName.length; i++)
				{
					System.out.print(columnName[i] + "  ");
				}
				System.out.println("\n-----------------------------------------------------------------------------");
				if(table.contentEquals("davisbase_columns") || table.contentEquals("davisbase_tables"))
				{
					RandomAccessFile file = new RandomAccessFile(DIR_NAME + "/" + CAT_NAME + "/" + table + TBL_EXT, "rw");
					int page = DBTable.get_Num_Pages(file);
					for(int i = 0; i < page; i++)
					{
						file.seek((i)*pageSize);
						Byte type1 = file.readByte();
						if(type1 == LEAF_TABLE)
						{
							short[] offsets = DBPage.getOffsetArray(file, (i+1));
							for(int j = 0; j < offsets.length; j++)
							{
								int row = DBPage.retRowID(file, offsets[j]);
								System.out.print(row + "  ");
								String[] temp = DBPage.getRecord(file, i, offsets[j]);
								for(int k = 0; k < temp.length; k++)
								{
									System.out.print(temp[k] + "  ");
								}
								System.out.println();
							}
							
						}
					}
				}
				else
				{
					RandomAccessFile file = new RandomAccessFile(DIR_NAME + "/" + USER_DAT + "/" + table +"/"+table+ TBL_EXT, "rw");
					int page = DBTable.get_Num_Pages(file);
					for(int i = 0; i < page; i++)
					{
						file.seek((i)*pageSize);
						Byte type1 = file.readByte();
						if(type1 == LEAF_TABLE)
						{
							short[] offsets = DBPage.getOffsetArray(file, (i+1));
							for(int j = 0; j < offsets.length; j++)
							{
								int row = DBPage.retRowID(file, offsets[j]);
								System.out.print(row + "  ");
								String[] temp = DBPage.getRecord(file, i, offsets[j]);
								for(int k = 0; k < temp.length; k++)
								{
									System.out.print(temp[k] + "  ");
								}
								System.out.println();
							}
							
						}
					}
				}
			}
			catch(Exception e)
			{
				System.out.println("Error at select" + e);
			}
			System.out.println();
	}
		
	public static int get_Num_Pages(RandomAccessFile file)
	{
		long num = DBPage.getTotalPages(file);
		int convert_num = (int) num;
		return convert_num;
	}
		
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	public static void screenMessage()
	{
		System.out.println(line("*",75));
		System.out.println("Welcome to BrownDB");
		System.out.println("Authors: Divya, Han, Mansi, Yesha, Cliff\n");
		System.out.println("\nType help to display supported commands.\n\n");
		System.out.println(line("*",75));
	}
	//method to display the supported commands
	public static void help()
	{
		System.out.println(line("*",75));
        System.out.println("Supported commands\n");
        System.out.println("All commands listed below are case insensitive.\n");
        
        System.out.println("SHOW TABLES - Display the name of all the tables.");
        System.out.println("\tSyntax: SHOW TABLES;\n");
        
        System.out.println("CREATE TABLE - create a new table.");
        System.out.println("\tSyntax: CREATE TABLE table_name " + "(<col_name1> <col_type> primary key, "
                                  + "<col_name2> <col_type> not null, <col_name3> <col_type>..);\n");
        
        System.out.println("DROP TABLE - delete the table.");
        System.out.println("\tSyntax: DROP TABLE <table_name>; \n");
        
        System.out.println("INSERT - Inserts single record into a table");
        System.out.println("\tSyntax: INSERT INTO table_name (column_list) VALUES (value1,value2,value3,...);\n");
        
        System.out.println("DELETE - Delets one or more records from the table");
        System.out.println("\tSyntax: DELETE FROM TABLE table_name WHERE row_id = value;\n");
        
        System.out.println("EXIT - exit the program.\n");
        
        System.out.println("UPDATE - Modifies one or more records");
        System.out.println("\tSyntax: UPDATE TABLE <table name> SET <column name> = <value> WHERE <condition>\n");
        
        System.out.println("SELECT - Display all columns or one column with condition");
        System.out.println("\tSyntax 1: SELECT * FROM table_name; ");
        System.out.println("\tSyntax 2: SELECT * FROM table_name WHERE row_id = <value>;\n");
        
        System.out.println("HELP - Display this help information.");
        System.out.println("\tSyntax: HELP;\n");
        
        System.out.println("EXIT - Exits the program and saves all table information");
        System.out.println("\tSyntax: EXIT;\n");
        
        System.out.println(line("*",75));

	}
	//method to check the catalog directory is there
	public static boolean dir()
	{
		File dir = new File(DIR_NAME + "\\" + CAT_NAME);
		if(dir.exists())
			return true;
		else
			return false;
	}
	//method to create the data directory
	public static void createDataDirectory()
	{
		//create the data directory at the current OS location
		try
		{
		File data_dir = new File (DIR_NAME);
		data_dir.mkdir();
		}					
		catch(SecurityException excep)
		{
			System.out.println("Error in initializeDataStore(): " + excep);
		}		
	}
	//method to initiate data store if the files are not there.
	public static void initializeDataStore()
	{
		try
		{
			createDataDirectory();
			File data_dir = new File (DIR_NAME + "/" + CAT_NAME);
			data_dir.mkdir();
			
			File user_dir = new File (DIR_NAME + "/" + USER_DAT);
			user_dir.mkdir();
			/*	
			String[] TableFiles;
			TableFiles = data_dir.list();
			
			for(int i = 0; i < TableFiles.length; i++)
			{
				File newTable = new File(data_dir, TableFiles[i]);
				newTable.delete();
			}
			*/
		}
		catch(SecurityException excep)
		{
			System.out.println("Error in initializeDataStore(): " + excep);
		}
		
		//create the davisbase_tables catalog.
		try
		{
			davisbase_tables = new RandomAccessFile(DIR_NAME + "/" + CAT_NAME + "/" + TBLS_NAME + TBL_EXT, "rw");
			//file starts as 1 page in length or 512 bytes in size
			davisbase_tables.setLength(pageSize);
			//set the file pointer to the beginning of the file.
			davisbase_tables.seek(0);
			//write the page header type, indicate its a leaf node starting out. pointer auto-increments to the next byte
			davisbase_tables.write(LEAF_TABLE);
			//write to indicate there are no cells in the table.
			davisbase_tables.writeShort(2);
			//offset for davisbase_tables, includes cell header, data type byte, and data
			int offset1 = 24;
			//offset of davisbase_columns
			int offset2 = 25;
			//write the offset of the last value, in this case the location of davis_base columns
			davisbase_tables.writeShort((short)(pageSize-1-offset1-offset2));
			//indicate this is the rightmost page
			davisbase_tables.writeInt(RIGHT);
			//cell offset of the first record
			davisbase_tables.writeShort((short)(pageSize-1-offset1));
			//cell offset of the second record
			davisbase_tables.writeShort((short)((pageSize-1-offset1-offset2)));
			//go to the first offset
			davisbase_tables.seek(pageSize-1-offset1);
			//write the record size
			davisbase_tables.writeShort((short)18);
			//write the row id
			davisbase_tables.writeInt(1);
			//write the number of columns
			davisbase_tables.writeByte(1);
			//write the column type, in this case text
			davisbase_tables.writeByte(28);
			//write the table name
			davisbase_tables.writeBytes("davisbase_tables");
			//go to the next offset
			davisbase_tables.seek(pageSize-1-offset1-offset2);
			//repeat process as above.
			davisbase_tables.writeShort((short)19);
			davisbase_tables.writeInt(2);
			davisbase_tables.writeByte(1);
			davisbase_tables.writeByte(29);
			davisbase_tables.writeBytes("davisbase_columns");			
			//davisbase_tables.close();
			
		}
		catch(Exception excep)
		{
			System.out.println("Error in initializeDataStore(), create table: " + excep);
		}
		// create the columns system catalog
		try
		{
			davisbase_columns = new RandomAccessFile(DIR_NAME + "/" + CAT_NAME + "/" + COLS_NAME + TBL_EXT, "rw");
			//file is initially 1 page in length
			davisbase_columns.setLength(pageSize);
			//set the file pointer to the beginning of the file
			davisbase_columns.seek(0);
			//make the table a leaf node to begin with.
			davisbase_columns.write(LEAF_TABLE);
			
			//write 8 to the number of cells in the table.
			davisbase_columns.writeShort(6);
			int offset2 = 45; //offset of the first record including the cell header
			int offset4 = 46;
			int offset5 = 47;
			int offset6 = 45;
			int offset7 = 55;
			int offset8 = 47;
			
			//write the last cell position
			davisbase_columns.writeShort((short)(pageSize-1-offset2-offset4-offset5-offset6-offset7-offset8));
			//write the rightmost page value
			davisbase_columns.writeInt(RIGHT);
			davisbase_columns.writeShort((short)(pageSize-1-offset2));
			davisbase_columns.writeShort((short)(pageSize-1-offset2-offset4));
			davisbase_columns.writeShort((short)(pageSize-1-offset2-offset4-offset5));
			davisbase_columns.writeShort((short)(pageSize-1-offset2-offset4-offset5-offset6));
			davisbase_columns.writeShort((short)(pageSize-1-offset2-offset4-offset5-offset6-offset7));
			davisbase_columns.writeShort((short)(pageSize-1-offset2-offset4-offset5-offset6-offset7-offset8));

			davisbase_columns.seek(pageSize-1-offset2);
			davisbase_columns.writeShort(offset2 - CELL_HEAD);
			davisbase_columns.writeInt(1);
			davisbase_columns.writeByte(5);
			davisbase_columns.writeByte(28);
			davisbase_columns.writeByte(22);
			davisbase_columns.writeByte(16);
			davisbase_columns.writeByte(0x04);
			davisbase_columns.writeByte(14);
			davisbase_columns.writeBytes("davisbase_tables");
			davisbase_columns.writeBytes("table_name");
			davisbase_columns.writeBytes("TEXT");
			davisbase_columns.writeByte(0x01);
			davisbase_columns.writeBytes("NO");
			
			davisbase_columns.seek(pageSize-1-offset2-offset4);
			davisbase_columns.writeShort(offset4- CELL_HEAD);
			davisbase_columns.writeInt(2);
			davisbase_columns.writeByte(5);
			davisbase_columns.writeByte(29);
			davisbase_columns.writeByte(22);
			davisbase_columns.writeByte(16);
			davisbase_columns.writeByte(0x04);
			davisbase_columns.writeByte(14);
			davisbase_columns.writeBytes("davisbase_columns");
			davisbase_columns.writeBytes("table_name");
			davisbase_columns.writeBytes("TEXT");
			davisbase_columns.writeByte(0x01);
			davisbase_columns.writeBytes("NO");
			
			davisbase_columns.seek(pageSize-1-offset2-offset4-offset5);
			davisbase_columns.writeShort(offset5- CELL_HEAD);
			davisbase_columns.writeInt(3);
			davisbase_columns.writeByte(5);
			davisbase_columns.writeByte(29);
			davisbase_columns.writeByte(23);
			davisbase_columns.writeByte(16);
			davisbase_columns.writeByte(0x04);
			davisbase_columns.writeByte(14);
			davisbase_columns.writeBytes("davisbase_columns");
			davisbase_columns.writeBytes("column_name");
			davisbase_columns.writeBytes("TEXT");
			davisbase_columns.writeByte(0x02);
			davisbase_columns.writeBytes("NO");
			
			davisbase_columns.seek(pageSize-1-offset2-offset4-offset5-offset6);
			davisbase_columns.writeShort(offset6- CELL_HEAD);
			davisbase_columns.writeInt(4);
			davisbase_columns.writeByte(5);
			davisbase_columns.writeByte(29);
			davisbase_columns.writeByte(21);
			davisbase_columns.writeByte(16);
			davisbase_columns.writeByte(0x04);
			davisbase_columns.writeByte(14);
			davisbase_columns.writeBytes("davisbase_columns");
			davisbase_columns.writeBytes("data_type");
			davisbase_columns.writeBytes("TEXT");
			davisbase_columns.writeByte(0x03);
			davisbase_columns.writeBytes("NO");
			
			davisbase_columns.seek(pageSize-1-offset2-offset4-offset5-offset6-offset7);
			davisbase_columns.writeShort(offset7- CELL_HEAD);
			davisbase_columns.writeInt(5);
			davisbase_columns.writeByte(5);
			davisbase_columns.writeByte(29);
			davisbase_columns.writeByte(28);
			davisbase_columns.writeByte(19);
			davisbase_columns.writeByte(0x04);
			davisbase_columns.writeByte(14);
			davisbase_columns.writeBytes("davisbase_columns");
			davisbase_columns.writeBytes("ordinal_position");
			davisbase_columns.writeBytes("TINYINT");
			davisbase_columns.writeByte(0x04);
			davisbase_columns.writeBytes("NO");
			
			davisbase_columns.seek(pageSize-1-offset2-offset4-offset5-offset6-offset7-offset8);
			davisbase_columns.writeShort(offset8- CELL_HEAD);
			davisbase_columns.writeInt(6);
			davisbase_columns.writeByte(5);
			davisbase_columns.writeByte(29);
			davisbase_columns.writeByte(23);
			davisbase_columns.writeByte(16);
			davisbase_columns.writeByte(0x04);
			davisbase_columns.writeByte(14);
			davisbase_columns.writeBytes("davisbase_columns");
			davisbase_columns.writeBytes("is_nullable");
			davisbase_columns.writeBytes("TEXT");
			davisbase_columns.writeByte(0x05);
			davisbase_columns.writeBytes("NO");			
		}
		catch(Exception excep)
		{
			System.out.println("Error in initializeDataStore(), create columns: "+ excep);
		}
		//create the table to hold the root pages of the table files
		try
		{
			table_roots = new RandomAccessFile(DIR_NAME + "/" + CAT_NAME + "/table_roots" + TBL_EXT, "rw");
			table_roots.setLength(pageSize);
			table_roots.seek(0);
			//make the table a leaf node to begin with.
			table_roots.write(LEAF_TABLE);
			table_roots.writeShort(2);
			short offset1 = pageSize - 1 - 29;
			short offset2 = (short) (offset1 - 30);
			table_roots.writeShort(offset2);
			table_roots.writeInt(RIGHT);
			table_roots.writeShort(offset1);
			table_roots.writeShort(offset2);
			table_roots.seek(offset1);
			table_roots.writeShort(20);
			table_roots.writeInt(1);
			table_roots.writeByte(2);
			table_roots.writeByte(28);
			table_roots.writeByte(6);
			table_roots.writeBytes("davisbase_tables");
			table_roots.writeInt(1);
			table_roots.seek(offset2);
			table_roots.writeShort(21);
			table_roots.writeInt(2);
			table_roots.writeByte(2);
			table_roots.writeByte(29);
			table_roots.writeByte(6);
			table_roots.writeBytes("davisbase_columns");
			table_roots.writeInt(1);
						
		}
		catch (Exception excep)
		{
			System.out.println("Error initialDataStore, create root table: " + excep);
		}		
	}
	
	
	
}
