package brownDB;

import java.io.RandomAccessFile;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class DBPage implements DBVariables
{
		public static RandomAccessFile table_roots;
		//method to create a leaf page in the file.  Returns the page number
		public static int createLeaf(RandomAccessFile file)
		{
			//initialize a variable to return the page number
			int num = 0;
			try 
			{
				//gets the number of pages in the file and adds 1 to it for the new page
				num = (int) (file.length()/ pageSize) + 1;
				//sets the new length of the page
				file.setLength(pageSize * num);
				System.out.println("new page size: "+pageSize*num);
				//finds the first byte of the new page
				file.seek((num - 1)* pageSize);
				//writes the leaf indicator to the first byte
				file.writeByte(LEAF_TABLE);
				//writes number of records as 0
				file.writeShort(0);
				//writes offset as the end of the page
				file.writeShort((pageSize*num)-1);
		
				//makes this cell the rightmost
				file.writeInt(RIGHT);
			}
			catch (Exception excep)
			{
				System.out.println("Error in createLeaf: " + excep);
			}
			
			return num;
		}
		//method to create an interior page in the file and return the page number
		public static int createInterior(RandomAccessFile file)
		{
			//initialize a variable to return the page number
			int num = 0;
			try
			{
				num = (int) (file.length()/ pageSize) + 1;
				file.setLength(pageSize * num);
				file.seek((num - 1)* pageSize);
				file.writeByte(INT_TABLE);
				file.writeShort(0);
				file.writeShort((pageSize*num)-1);
				
			}
			catch(Exception excep)
			{
				System.out.println("Error in createInterior: " + excep);
			}
			return num;
		}
		//method to calculate the size of a record as a short for the cell header
		public static short getRecordSize(String[] data, String[] type)
		{
			int size = 1;
			//get the length for the number of columns, each is 1 byte to indicate data type
			size = type.length + size;
			for(int i = 0; i < type.length; i++)
			{
				//get the data types of the record for the lengths.
				String temp = type[i];
				switch(temp)
				{
				case "TINYINT":
					size = size + TINYINT_SIZE;
					break;
				case "SMALLINT":
					size = size + SMALLINT_SIZE;
					break;
				case "INT":
					size = size + INT_SIZE;
					break;
				case "BIGINT":
					size = size + BIGINT_SIZE;
					break;
				case "REAL":
					size = size + REAL_SIZE;
					break;
				case "DOUBLE":
					size = size + DOUBLE_SIZE;
					break;
				case "DATETIME":
					size = size + DATETIME_SIZE;
					break;
				case "DATE":
					size = size + DATE_SIZE;
					break;
				case "TEXT":
					String txt = data[i];
					size = size + txt.length();
					break;
				default:
					break;
				}
			}
			return (short) size;
		}
		//method to calculate the pay load size
		public static int getPayloadSize(String[] data, String[] type)
		{
			int size = 0;
			size = type.length + 1;
			for(int i = 0; i < type.length; i++)
			{
				String temp = type[i];
				switch(temp)
				{
				case "TINYINT":
					size = size + TINYINT_SIZE;
					break;
				case "SMALLINT":
					size = size + SMALLINT_SIZE;
					break;
				case "INT":
					size = size + INT_SIZE;
					break;
				case "BIGINT":
					size = size + BIGINT_SIZE;
					break;
				case "REAL":
					size = size + REAL_SIZE;
					break;
				case "DOUBLE":
					size = size + DOUBLE_SIZE;
					break;
				case "DATETIME":
					size = size + DATETIME_SIZE;
					break;
				case "DATE":
					size = size + DATE_SIZE;
					break;
				case "TEXT":
					//String txt = data[i];
					size = size + data[i].length();
					break;
				default:
					break;
				}
			}			
			size = CELL_HEAD + size;
			return size;
		}
		//insert a record into a leaf page
		//accepts the parameters of file, page number as page, the last cell offset as offset, the incremented row id as rowID,
		//list of the value as a string array, and the list of the data types as a string array.
		public static void leafInsert(RandomAccessFile file, int page, short offset, int rowId, String[] data, String[] type)
		{
			try
			{
				//update the number of records in the table
				short cell = 0;
				cell = getCellNumber(file, page);
				cell++;
				setCellNumber(file, page, cell);
				int payloadSize = getPayloadSize(data, type);
				file.seek((page-1)*pageSize + 3);
				file.writeShort(offset-(short)payloadSize);
				//add the new offset to the header array
				file.seek((page-1)*pageSize + 9 + 2 *(cell-1));
				file.writeShort(offset - (short)payloadSize);
				file.seek(offset - payloadSize);
				file.writeShort(getRecordSize(data, type));
				file.writeInt(rowId+1);
				file.writeByte(type.length);
				for(int i = 0; i < type.length; i++) 
				{
					switch(type[i])
					{
						case "ONE_NULL":
							file.writeByte(0);
							break;
						case "TWO_NULL":
							file.writeByte(1);
							break;
						case "FOUR_NULL":
							file.writeByte(2);
							break;
						case "EIGHT_NULL":
							file.writeByte(3);
							break;
						case "TINYINT":
							file.writeByte(4);
							break;
						case "SMALLINT":
							file.writeByte(5);
							break;
						case "INT":
							file.writeByte(6);
							break;
						case "BIGINT":
							file.writeByte(7);
							break;
						case "REAL":
							file.writeByte(8);
							break;
						case "DOUBLE":
							file.writeByte(9);
							break;
						case "DATETIME":
							file.writeByte(0x0A);
							break;
						case "DATE":
							file.writeByte(0x0B);
							break;
							//default writes the text data type
						case "TEXT":
							byte tmp = (byte)(12 + data[i].length());
							file.writeByte(tmp);
							break;	
						default:
							break;		
					}
				}
				for(int i = 0; i < data.length; i++)
				{
					switch(type[i])
					{
					case "ONE_NULL":
						file.writeByte(0);
						break;
					case "TWO_NULL":
						file.writeShort(0);
						break;
					case "FOUR_NULL":
						file.writeInt(0);
						break;
					case "EIGHT_NULL":
						file.writeLong(0);
						break;
					case "TINYINT":
						file.writeByte(new Byte(data[i]));
						break;
					case "SMALLINT":
						file.writeShort(new Short(data[i]));
						break;
					case "INT":
						file.writeInt(new Integer(data[i]));
						break;
					case "BIGINT":
						file.writeLong(new Long(data[i]));
						break;
					case "REAL":
						file.writeFloat(new Float(data[i]));
						break;
					case "DOUBLE":
						file.writeDouble(new Double(data[i]));
						break;
					case "DATETIME":
						String str = data[i];
						Date temp = new SimpleDateFormat(DATETIME_FORMAT).parse(str);
						long time = temp.getTime();
						file.writeLong(time);
						break;
					case "DATE":
						String str1 = data[i];
						Date temp1 = new SimpleDateFormat(DATE_FORMAT).parse(str1);
						long time1 = temp1.getTime();
						file.writeLong(time1);
						break;
						//default writes the text data type
					case "TEXT":
						file.writeBytes(data[i]);
						break;
					default:
							break;
						
					}
					
				}
				
				System.out.println("Record Insertion Successful.");				
			}
			catch (Exception excep)
			{
				System.out.println("Error in leafInsert: " + excep);
			}
			
		}
		//insert a record into an interior page
		//record is the row id as an integer and the page number as an integer 8 bytes in total
		public static void interiorInsert(RandomAccessFile file, int page, int rowID, int pagenum)
		{
			try
			{
				//get the interior page in question
				file.seek((page -1)*pageSize + 1);
				//read the number of records
				short numrecords = file.readShort();
				//write the number of records
				file.seek((page -1)*pageSize + 1);
				setCellNumber(file, page, (short)(numrecords+1));
				//get the offset
				short offset = file.readShort();
				//write the offset to the page header
				file.seek((page-1)*pageSize +3);
				file.writeShort((short)offset-8);
				//write the offset to the array
				file.seek((page -1)*pageSize + 9 +(numrecords *2));
				file.writeShort((short)(offset-8));
				//go to the offset to begin writing the record.
				file.seek((int)(offset-8));
				file.writeInt(rowID);
				file.writeInt(pagenum);				
			}
			catch (Exception excep)
			{
				System.out.println("error in interiorInsert: " + excep);
			}
		}
		//overwrite a pages old right most pointer in the header
		public static void overwriteRight(RandomAccessFile file, int page, int rightmost)
		{
			try
			{
				file.seek((page -1)*pageSize +5);
				file.writeInt(rightmost);				
			}
			catch (Exception excep)
			{
				System.out.println("error in overwriteRight: " + excep);
			}
		}
		//update a record in a leaf page
		//accepts the parameters file, page number as page, cell offset as offset, offset of the column as column, column offset
		//starts at the data entry
		//and the data as a string variable.
		public static void updateLeaf(RandomAccessFile file, int page, int offset, int column, String data, String type)
		{
			try
			{
				file.seek((page-1)*pageSize + offset + CELL_HEAD);
				byte cols = file.readByte();
				byte[] array = new byte[(int)cols];
				for(int i = 0 ; i <(int) cols; i++)
				{
					array[i] = file.readByte();
				}
				int size = 0;
				for(int i = 1; i < column; i++)
				{
					switch(array[i-1])
					{
					case 0x00:
						size = size + 1;
						break;
					case 0x01:
						size = size + 2;
						break;
					case 0x02:
						size = size + 4;
						break;
					case 0x03:
						size = size + 8;
						break;
					case 0x04:
						size = size + 1;
						break;
					case 0x05:
						size = size + 2;
						break;
					case 0x06:
						size = size + 4;
						break;
					case 0x07:
						size = size + 8;
						break;
					case 0x08:
						size = size + 4;
						break;
					case 0x09:
						size = size + 8;
						break;
					case 0x0A:
						size = size + 8;
						break;
					case 0x0B:
						size = size + 8;
						break;
					default:
						byte length = (byte) (array[i-1] - 12);
						size = size + length;
						break;
					}
				}
				file.seek((page-1)*pageSize + offset + CELL_HEAD+1+(int)cols+size);
				switch(type)
				{
				case "ONE_NULL":
					file.writeByte(0);
					break;
				case "TWO_NULL":
					file.writeByte(0);
					break;
				case "FOUR_NULL":
					file.writeByte(0);
					break;
				case "EIGHT_NULL":
					file.writeByte(0);
					break;
				case "TINYINT":
					file.writeByte(new Byte(data));
					break;
				case "SMALLINT":
					file.writeShort(new Short(data));
					break;
				case "INT":
					file.writeInt(new Integer(data));
					break;
				case "BIGINT":
					file.writeLong(new Long(data));
					break;
				case "REAL":
					file.writeFloat(new Float(data));
					break;
				case "DOUBLE":
					file.writeDouble(new Double(data));
					break;
				case "DATETIME":
					Date temp = new SimpleDateFormat(DATETIME_FORMAT).parse(data);
					long time = temp.getTime();
					file.writeLong(time);
					break;
				case "DATE":
					Date temp1 = new SimpleDateFormat(DATE_FORMAT).parse(data);
					long time1 = temp1.getTime();
					file.writeLong(time1);
					break;
					//default writes the text data type
				default:
					file.writeBytes(data);
					break;				
				}
				
			}
			catch (Exception excep)
			{
				System.out.println("Error in updateLeaf: " + excep);
			}
		}
		//method to get the rightmost page for insertion, accepts a file parameter and a parameter of the total number of pages
		public static int getRightLeaf(RandomAccessFile file, int page)
		{
			int num = 0;
			try
			{
				file.seek((page-1)*pageSize + 5);
				int pg = file.readInt();
				if(pg == RIGHT)
				{
					num = page;
				}
				else
					return getRightLeaf(file, page+1);
				
			}
			catch (Exception excep)
			{
				System.out.println("Error getRightLeaf: " + excep);
			}
			
			return num;
		}
		//method to get the total number of pages returns the number of pages as an integer
		public static int getTotalPages(RandomAccessFile file)
		{
			long num = 0;
			try
			{
				num = file.length() / pageSize;
			}
			catch(Exception excep)
			{
				System.out.println("Error getTotalPages." + excep);
			}
			return (int) num;
		}
		//method to get the offset from a page
		public static short getOffset(RandomAccessFile file, int page)
		{
			short off = 0;
			try
			{
				file.seek((page - 1)* pageSize + 3);
				off = file.readShort();
				
			}
			catch(Exception excep)
			{
				System.out.println("Error in getOffset: " + excep);
			}
			return off;
		}
		//method to get the last row id
		public static int getRowID(RandomAccessFile file, int page)
		{
			int row = 0;
			try
			{
				int right = getRightLeaf(file, page);
				file.seek((right-1)*pageSize + 1);
				short cells = file.readShort();
				if(cells > 0)
				{
					short num = file.readShort();
					file.seek(num +2);
					row = file.readInt();
				}
				else if(page == 1 && cells ==0)
				{
					row = 0;
				}
				else
				{
					file.seek((right-2)*pageSize + 3);
					short num = file.readShort();
					file.seek(num +2);
					row = file.readInt();
					
				}
				
			}
			catch (Exception excep)
			{
				System.out.println("Error in getRowID: " + excep);
			}
			return row;
		}
		//method to expand the size of the page
		public static void increasePage(RandomAccessFile file, String table)
		{
			try
			{				
				//base case, the file is only 1 page, create interior then new leaf
				if(file.length() < 513)
				{
					//get the last row id
					int row = getRowID(file, 1);
					//create new interior and return the page number
					int newpage = createInterior(file);
					System.out.println("new int page #: " + newpage);
					//write the last rowID to the interior file
					interiorInsert(file, newpage, row, 1);
					//overwrite the root in the root table.
					table_roots = new RandomAccessFile(DIR_NAME + "/" + CAT_NAME + "/table_roots" + TBL_EXT, "rw");
					overwriteRoot(table_roots, table, newpage);
					//create a new leaf file
					int newleaf = createLeaf(file);
					//rewrite the old rightmost leaf pointer in the old leaf
					overwriteRight(file, 1, newleaf);
					//write the new leaf as the right child in the interior header
					overwriteRight(file, newpage, newleaf);
				}
				else //add a new record to the interior leaf
				{
					//get the total number of pages
					int num = getTotalPages(file);
					//get the rightmost leaf
					int rightleaf = getRightLeaf(file, num);
					//get the last rowID from the rightmost leaf
					int row = getRowID(file, rightleaf);
					//insert the new data into the interior page
					interiorInsert(file, 2, row, rightleaf);
					//create a new leaf
					int newleaf = createLeaf(file);
					//rewrite the old rightmost leaf pointer
					overwriteRight(file, rightleaf, newleaf);
					//rewrite the old rightmost child in the interior page
					overwriteRight(file, 2, newleaf);
				}
				
			}
			catch (Exception excep)
			{
				System.out.println("Error in increaseTable function: " + excep);
			}
		}
		//get cell number
		public static short getCellNumber(RandomAccessFile file, int page)
		{
			short val = 0;
			try
			{	//getting file pointer to second position of page header 
				file.seek((page-1)*pageSize+1);
				val = file.readShort();
			}
			catch(Exception excep)
			{
				System.out.println("Error at getCellNumber: " + excep);
			}
			return val;
		}
		//set the total number of cells
		public static void setCellNumber(RandomAccessFile file, int page, short num)
		{
			try
			{
				file.seek((page-1)*pageSize+1);
				file.writeShort(num);
			}
			catch(Exception excep)
			{
				System.out.println("Error at setCellNumber" + excep);
			}
		}
		//method to check the space available in a leaf page
		public static boolean checkLeaf(RandomAccessFile file, int page, int recordSize)
		{
			boolean value = false;
			try
			{
				short cells = getCellNumber(file, page);
				short offset = getOffset(file, page);
				short header = (short) (9 + 2 * cells);
				header = (short) (header +((page-1)*pageSize));
				int space = offset - header - 2;
				if(space > recordSize)
					value = true;
				else
					value = false;			
			}
			catch (Exception excep)
			{
				System.out.println("Error in checkLeaf" + excep);
			}
			
			return value;			
		}
		//method to check the space in an interior cell
		public static boolean checkInterior(RandomAccessFile file, int page)
		{
			boolean value = false;
			try
			{
				short cells = getCellNumber(file, page);
				short offset = getOffset(file, page);
				short header = (short) (9 + 2 * cells);
				int space = offset - header + 2;
				if(space > 8)
					value = true;
				else
					value = false;
				System.out.println("space size: " + space);	
			}
			catch(Exception excep)
			{
				System.out.println("Error in checkInterior: " + excep);
			}
			return value;
		}
		//method to return a record
		public static String[] getRecord(RandomAccessFile file, int page, short offset)
		{
			String[] record = new String[0];
			try
			{
				file.seek(offset+6);
				byte columns = file.readByte();
				record = new String[(int)columns];
				Byte[] type = new Byte[(int)columns];
				for(int i = 0; i < (int)columns;i++)
				{
					type[i] = file.readByte();
				}
				for(int i = 0; i < (int)columns; i++)
				{
					switch(type[i])
					{
					case 0x00:
						file.readByte();
						record[i] = "null";
						break;
					case 0x01:
						file.readShort();
						record[i] = "null";
						break;
					case 0x02:
						file.readInt();
						record[i] = "null";
						break;
					case 0x03:
						file.readLong();
						record[i] = "null";
						break;
					case 0x04:
						record[i] = Byte.toString(file.readByte());
						break;
					case 0x05:
						record[i] = Short.toString(file.readShort());
						break;
					case 0x06:
						record[i] = Integer.toString(file.readInt());
						break;
					case 0x07:
						record[i] = Long.toString(file.readLong());
						break;
					case 0x08:
						record[i] = Integer.toString(file.readInt());
						break;
					case 0x09:
						record[i] = Long.toString(file.readLong());
						break;
					case 0x0A:
						record[i] = Long.toString(file.readLong());
						break;
					case 0x0B:
						record[i] = Long.toString(file.readLong());
						break;
						//default writes the text data type
					default:
						byte tmp = type[i];
						tmp = (byte)(tmp - 12);
						byte[] arr = new byte[tmp];
						for(int j = 0; j < tmp; j++)
						{
							arr[j] = file.readByte();
						}
						record[i] = new String(arr);
						break;
					}
				}
				
			}
			catch (Exception excep)
			{
				System.out.println("Error in getRecord: " + excep);
			}
			
			return record;
		}
		//method to get the cell offset array
		public static short[] getOffsetArray(RandomAccessFile file, int page) 
		{
			short[] cellArray = new short[0];
			try 
			{
				short numberofcells = getCellNumber(file, page);
				cellArray = new short[numberofcells];
				// getting to the 9th position in the header?
				file.seek((page - 1) * pageSize + 9);
				for (short i = 0; i < numberofcells; i++)
				{
					cellArray[i] = file.readShort();
				}
			} 
			catch (Exception e) 
			{
				System.out.println("Error in getCellArray method." + e);
			}
			return (cellArray);
		}
		//method to get the root page of the table
		public static int getRoot(RandomAccessFile file, String table)
		{
			int root = 0;
			try
			{
				short cells = DBPage.getCellNumber(file, 1);
				short[] offsets = new short[cells];
				offsets = DBPage.getOffsetArray(file,1);
				for(int i = 0; i < (int)cells; i++)
				{
					String[] record = new String[0];
					record = DBPage.getRecord(file, 1, offsets[i]);
					if(table.equals(record[0]))
					{
						root = Integer.parseInt(record[1]);
					}
				}
				
			}
			catch (Exception excep)
			{
				System.out.println("error in getRoot: " + excep);
			}		
			return root;
		}
		//method to change the root
		public static void overwriteRoot(RandomAccessFile file, String table, int root)
		{
			try
			{
				short cells = DBPage.getCellNumber(file, 1);
				short[] offsets = new short[cells];
				offsets = DBPage.getOffsetArray(file,1);
				for(int i = 0; i < (int)cells; i++)
				{
					String[] record = new String[0];
					record = DBPage.getRecord(file, 1, offsets[i]);
					if(record[0].equals(table))
					{
						updateLeaf(file, 1, offsets[i], 2, Integer.toString(root), "INT");
					}
				}
			}
			catch (Exception excep)
			{
				System.out.println("Error in overwriteRoot: " + excep);
			}			
		}
		//method to retrieve the data types of the columns
		public static String[] getColTypes(RandomAccessFile file, String table)
		{
			String[] arr = new String[0];
			try
			{
				file = new RandomAccessFile(DIR_NAME + "/" + CAT_NAME + "/" + COLS_NAME + TBL_EXT, "rw");
				ArrayList<String> cols = new ArrayList<String>();				
				int num = getTotalPages(file);
				for (int i = 0; i < num; i++)
				{
					file.seek((i)*pageSize);
					byte pageType = file.readByte();
					//System.out.println(pageType);
					if(pageType == LEAF_TABLE)
					{
						
						int rec = getCellNumber(file,(i+1));
						short[] offsets = new short[0];
						offsets = getOffsetArray(file, (i+1));
						for(int j = 0; j < rec; j++)
						{
							String[] temp = new String[0];
							temp = getRecord(file, (i+1), offsets[j]);
							if(temp[0].equals(table))
							{
								cols.add(temp[2]);
							}
						}
					}
				}
				arr = new String[cols.size()];
				for(int i = 0; i < cols.size(); i++)
				{
					arr[i] = cols.get(i);
				}
			}
			catch(Exception excep)
			{
				System.out.println("Error in getcoltypes: " + excep);
			}
			return arr;
		}
		//use this to find the offset to delete out of tables and column data
		public static short findOffset(RandomAccessFile file, String data)
		{
			short off = 0;
			try
			{
				int pages = getTotalPages(file);
				for (int i = 1; i < pages+1; i++)
				{
					file.seek((i-1)*pageSize);
					byte pageType = file.readByte();
					if(pageType == LEAF_TABLE)
					{
						int rec = DBPage.getCellNumber(file,i);
						short[] offsets = new short[0];
						offsets = DBPage.getOffsetArray(file, i);
						for(int j = 0; j < rec; j++)
						{
							String[] temp = new String[0];
							temp = DBPage.getRecord(file, i, offsets[j]);
							if(temp[0].equals(data))
							{
								off = offsets[j];
								break;
							}
						}
					}
				}
			}
			catch(Exception excep)
			{
				System.out.println("Error in find offset: " + excep);
			}
			
			return off;
			
		}
		//method to delete an offset
		public static void removeOffset(RandomAccessFile file, short offset)
		{
			try
			{
				int page = offset / pageSize + 1;
				short[] offsets = new short[0];
				offsets = getOffsetArray(file, page);
				file.seek((page-1)*pageSize+9);
				for(int i = 0; i < offsets.length; i++) 
				{
					file.writeShort(0);
				}
				file.seek((page-1)*pageSize+9);
				for(int i = 0; i < offsets.length; i++)
				{
					if(offsets[i]!= offset)
					{
						file.writeShort(offsets[i]);
					}
				}
				file.seek((page-1)*pageSize+1);
				file.writeShort((short)(offsets.length-1));
				
			}
			catch (Exception excep)
			{
				System.out.println("Error in removeOffset: "+ excep );
			}
		}
		//method to find a specified row ID
		public static short findRowID(RandomAccessFile file, int root, int rowID)
		{
			short off = 0;
			try
			{
				file.seek((root-1)*pageSize);
				Byte type = file.readByte();
				System.out.println("Cell type: "+ type);
				if(type == LEAF_TABLE)
				{
					short[] offsets = new short[0];
					offsets = getOffsetArray(file, root);
					for(int i = 0; i < offsets.length; i++)
					{
						file.seek(offsets[i] +2);
						int temp = file.readInt();
						if(temp == rowID)
						{
							off = offsets[i];
							break;
						}
					}
					return off;
				}
				else
				{
					short[] offsets = new short[0];
					offsets = getOffsetArray(file, root);
					int page = 0;
					for(int i = 0; i < offsets.length; i++)
					{
						file.seek(offsets[i]);
						int temp = file.readInt();
						if(temp > rowID)
						{
							page = file.readInt();
							break;
						}
					}
					return findRowID(file, page, rowID);
				}
			}
			catch (Exception excep)
			{
				System.out.println("Error in findRowID: " + excep);
			}
			
			return off;
		}
		//method to get column names for display
		public static String[] getColNames( String table)
		{
			String[] arr = new String[0];
			try
			{
				RandomAccessFile file = new RandomAccessFile(DIR_NAME + "/" + CAT_NAME + "/" + COLS_NAME + TBL_EXT, "rw");
				ArrayList<String> cols = new ArrayList<String>();				
				int num = getTotalPages(file);
				for (int i = 0; i < num; i++)
				{
					file.seek((i)*pageSize);
					byte pageType = file.readByte();
					if(pageType == LEAF_TABLE)
					{
						
						int rec = getCellNumber(file,(i+1));
						short[] offsets = new short[0];
						offsets = getOffsetArray(file, (i+1));
						for(int j = 0; j < rec; j++)
						{
							String[] temp = new String[0];
							temp = getRecord(file, (i+1), offsets[j]);
							if(temp[0].equals(table))
							{
								cols.add(temp[1]);
							}
						}
					}
				}
				arr = new String[cols.size()];
				for(int i = 0; i < cols.size(); i++)
				{
					arr[i] = cols.get(i);
				}
			}
			catch(Exception excep)
			{
				System.out.println("Error in getcoltypes: " + excep);
			}
			return arr;
		}
		//method to retrieve the row Id from a given offset
		public static int retRowID(RandomAccessFile file, short offset)
		{
			int row = 0;
			try
			{
				file.seek(offset+2);
				row = file.readInt();
			}
			catch (Exception excep)
			{
				System.out.println("Error in getRowID: " + excep);
			}
			return row;
		}
		//method to get column ordinal position number
		public static byte getColPos(String table, String colName)
		{
			byte pos = 0;
			try
			{
				RandomAccessFile file = new RandomAccessFile(DIR_NAME + "/" + CAT_NAME + "/" + COLS_NAME + TBL_EXT, "rw");
				int num = getTotalPages(file);
				for (int i = 0; i < num; i++)
				{
					file.seek((i)*pageSize);
					byte pageType = file.readByte();
					if(pageType == LEAF_TABLE)
					{						
						int rec = getCellNumber(file,(i+1));
						short[] offsets = new short[0];
						offsets = getOffsetArray(file, (i+1));
						for(int j = 0; j < rec; j++)
						{
							String[] temp = new String[0];
							temp = getRecord(file, (i+1), offsets[j]);
							if(temp[0].equals(table))
							{
								pos = Byte.parseByte(temp[3]);
							}
						}
					}
				}
				
			}
			catch(Exception excep)
			{
				System.out.println("Error in getColPos: " + excep);
			}
			
			return pos;
		}
		
		
		
		
		
		
}
