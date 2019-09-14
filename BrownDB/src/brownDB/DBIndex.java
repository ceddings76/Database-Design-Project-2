package brownDB;

import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.Date;

import brownDB.DBPage;

public class DBIndex implements DBVariables 
{
	
	
	
	//method to create an index leaf and return the page number
		public static int createIndexLeaf(RandomAccessFile file)
		{
			int num = 0;
			try
			{
				num = (int) (file.length()/ pageSize) + 1;
				file.setLength(pageSize * num);
				file.seek((num - 1)* pageSize);
				//indicator of the page type
				file.writeByte(LEAF_INDEX);
	
				file.seek((num - 1)* pageSize+1);
				//number of cells=0
				file.writeShort(0);
				//setting offset as the end of the file
				file.writeShort((pageSize*num)-1);
				//setting page number of right sibling =0
				file.writeInt(0);
				
			}
			catch (Exception excep)
			{
				System.out.println("Error creating index leaf: " + excep);
			}
			return num;
		}
				
		//method to create an index interior page and return the page number
		public static int createIndexInterior(RandomAccessFile file)
		{
			int num = 0;
			try
			{	
				//incrementing the page number
				num = (int) (file.length()/ pageSize) + 1;
				//setting new length
				file.setLength(pageSize * num);
				file.seek((num - 1)* pageSize);
				//indicator to the page type
				file.writeByte(INT_INDEX);
				file.seek((num - 1)* pageSize+1);
				//setting number of cells=0
				file.writeShort(0);
				//setting offset as the end of the file
				file.writeShort((pageSize*num)-1);
				
				
			}
			catch (Exception excep)
			{
				System.out.println("Error creating interior index page: " + excep);
			}
			return num;
		}
				
		//method to find the middle location offset to send it up to a new page
		public static short getMiddleLoc(RandomAccessFile file, int page)
		{
			short value = 0;
			try
			{
				file.seek((page-1)*pageSize+1); //+1 here gets us at the start of the number of cells column
				short cells = file.readShort();
				short mid = (short)(( cells/2)+1);
				file.seek((page-1)*pageSize + 9 + (mid-1) * 2);
				value = file.readShort();			
			}
			catch(Exception excep)
			{
				System.out.println("Error in getMiddle method: " + excep);
			}
			return value;
		}
		
		
		//to set the number of cells in page header
		public static void setCellNumber(RandomAccessFile file, int page, byte num)
		{
			try{
				file.seek((page-1)*pageSize+1);
				file.writeByte(num);
			}catch(Exception e){
				System.out.println("Error at setCellNumber");
			}
		}
		
		
		
		// Return the number of cells in the page
		public static byte getCellNumber(RandomAccessFile file, int page)
		{
			byte val = 0;
			try
			{	//getting file pointer to second position of page header 
				file.seek((page-1)*pageSize+1);
				val = file.readByte();
			}
			catch(Exception e)
			{
			System.out.println(e);
			System.out.println("Error at getCellNumber");
			}
			return val;
		}
				
				
				
		//change return types of the functions to the right ones
		public static int getPayloadSize(String nodeType, String datatype, int no_of_rowid,String data)
		{
			//here data is the indexvalue
            int payloadsize=0; int rightNodeType=1;
            payloadsize= no_of_rowid*4;
                 
            if(nodeType.equals("0x0A")||nodeType.equals("0x0a")||nodeType.equals("0X0A")||nodeType.equals("0X0a"))
            {
            	payloadsize=payloadsize+2+1+1;
            }
            else if(nodeType=="0x02") 
            {
               	payloadsize=payloadsize+4+2+1+1;
            }
            else 
            {
               	System.out.println("Wrong node type");
               	rightNodeType=0;
            }
            if(rightNodeType==1) 
            {
                switch(datatype)
                {
                    case "TINYINT":
                                 payloadsize=payloadsize+TINYINT_SIZE;
                                 break;
                    case "SMALLINT":
                                 payloadsize= payloadsize+SMALLINT_SIZE;
                    case "INT":
                                 payloadsize= payloadsize+ INT_SIZE*no_of_rowid;
                                 break;
                    case "BIGINT":
                                 payloadsize= payloadsize+BIGINT_SIZE;
                                 break;
                    case "REAL":
                                 payloadsize= payloadsize+REAL_SIZE;
                                 break;
                    case "DOUBLE":
                                 payloadsize= payloadsize+DOUBLE_SIZE;
                                 break;
                    case "DATETIME":
                                 payloadsize= payloadsize+DATETIME_SIZE;
                                 break;
                    case "DATE":
                                 payloadsize= payloadsize+DATE_SIZE;
                                 break;
                    case "TEXT":
                                 String text="";
                                 text = data;
                                 payloadsize= payloadsize+text.length();
                                 break;
                    default:
                                 break;
                    	}
               }
            return payloadsize;
      }
		
				
				
		//to get the size of Record
        public static int getRecordSize(String datatype, int no_of_rowid, String data){
                    	//here data is index value
                        int rsize=0;
                        rsize= 2+no_of_rowid*4;

                       

                        switch(datatype)

                        {

                        case "TINYINT":

                                     rsize=rsize+TINYINT_SIZE;

                                     break;

                        case "SMALLINT":

                                     rsize=rsize+SMALLINT_SIZE;

                        case "INT":

                                     rsize=rsize+INT_SIZE;

                                     break;

                        case "BIGINT":

                                     rsize=rsize+BIGINT_SIZE;

                                     break;

                        case "REAL":

                                     rsize=rsize+REAL_SIZE;

                                     break;

                        case "DOUBLE":

                                     rsize=rsize+DOUBLE_SIZE;

                                     break;

                        case "DATETIME":

                                     rsize=rsize+DATETIME_SIZE;

                                     break;

                        case "DATE":

                                     rsize=rsize+DATE_SIZE;

                                     break;

                        case "TEXT":

                                     String text="";

                                     text = data;

                                     rsize= rsize+text.length();

                                     break;

                        default:

                                     break;

          

                        }
                        
                        return rsize;
                    }
                        
    		
                    
		//check space left in the leaf node
		public static boolean checkLeafSpace(RandomAccessFile file, int page, int recordSize) 
		{
			boolean value = false;
			try
			{
				short pageHeaderSize= 9+2; //2 added is to have space for adding the offset of this cell now
				short cellSize= (short) (2+1+1+recordSize);
				short startOffset=(short) (((page-1)*pageSize)+3);
				short space= (short) (((page)*pageSize)-startOffset-pageHeaderSize);
				if(space>cellSize) {//should it be > or >=?
					value=true;
				}
				else {
					value=false;
				}
				
				
			}
			catch(Exception ex)
			{
				System.out.println("Error in checkLeafSpace" + ex);
			}
			return value;
		}
		
		
		
		
		//check space left in interior node
		public static boolean checkInteriorSpace(RandomAccessFile file, int page, int recordSize) 
		{
			boolean value = false;
			try
			{
				short pageHeaderSize= 9+2; //2 added is to have space for adding the offset of this cell now
				short cellSize= (short) (4+2+1+1+recordSize);
				
				short startOffset=(short) (((page-1)*pageSize)+3);
				short space= (short) (((page)*pageSize)-startOffset-pageHeaderSize);
				
				if(space>cellSize) {//should it be > or >=?
					value=true;
				}
				else {
					value=false;
				}
			}
			catch (Exception excep)
			{
				System.out.println("Error in checkLeafSpace" + excep);
			}
			return value;
		}
		
		public static void insertLeaf(RandomAccessFile file,int page,short offset,String indexValue, String type, int num_rowid, int[] rowids, int pageRight) {
			try {
				
				byte cells = getCellNumber(file, page);
				cells++;
				setCellNumber(file, page, cells);
				file.seek((page-1)*pageSize);
				byte nodeType=file.readByte();
				char ch=(char)nodeType;
				String nodeT= Character.toString(ch);//would it return the code?? do we need it? its  a leaf page anyway
				int payloadSize = getPayloadSize(nodeT,type, num_rowid, indexValue);
				file.seek((page-1)*pageSize + 3);
				file.writeShort(offset-(short)payloadSize);
				file.writeInt(pageRight);
						
				file.seek((page-1)*pageSize + 9 + 2 *(cells-1));
				file.writeShort(offset - (short)payloadSize);
				file.seek(offset - payloadSize);
				String data= Integer.toString(num_rowid)+type+indexValue;
				for(int i=0;i<num_rowid;i++) {
					data=data+rowids[i];
				}
				file.writeShort(getRecordSize(type, num_rowid, data));
				file.writeByte(num_rowid);
				//Byte type_ob= new Byte(type);
				//file.writeByte(type_ob);
				
				//storing code for a data type
				switch(type) {
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
				default:
					byte tmp = (byte)(12 + indexValue.length()); //check this once
					file.writeByte(tmp);
					break;	
			}
				
							
				
				//writing the index value
				switch(type) {
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
					file.writeBytes(indexValue); //check if for writing to one byte, using writeBytes is right
					break;
				case "SMALLINT":
					Short obj = new Short(indexValue);
					file.writeShort(obj);
					break;
				case "INT":
					Integer ob= new Integer(indexValue);
					file.writeInt(ob);
					break;
                case "BIGINT":
                	Long obj2= new Long(indexValue);
                	file.writeLong(obj2);
                	break;
                case "REAL":
                	Float ob2 = new Float(indexValue);
                	file.writeFloat(ob2);
                	break;
                case "DOUBLE":
                	Double obj3 = new Double(indexValue);
                	file.writeDouble(obj3);
                	break;
                case "DATETIME":
                	String str = indexValue;
					Date temp = new SimpleDateFormat(DATETIME_FORMAT).parse(str);
					long time = temp.getTime();
                	file.writeLong(time);
                	break;
                case "DATE":
                	String str1 = indexValue;
					Date temp1 = new SimpleDateFormat(DATE_FORMAT).parse(str1);
					long time1 = temp1.getTime();
                	file.writeLong(time1);
                	break;
                case "TEXT":
                	file.writeBytes(indexValue);
                	break;         	
                	
                default:

                    break;
                					
				}
				
				//storing n bytes of list of rowids
				for(int i=0;i<num_rowid;i++) {
					file.writeInt(rowids[i]);
				}
			}
			
			catch(Exception exp) {
				System.out.println("Error in insertLeaf: "+exp);
			}
		}
		
		
		public static short[] getoffsetArray(RandomAccessFile file, int page) 
		{
			short[] cellArray = new short[0];
			try 
			{
				short numberofcells = getCellNumber(file, page);
				cellArray = new short[numberofcells];
				// getting the 9 th position in the header?
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
		
		
		//method to delete an offset - check it once
				public static void removeOffset(RandomAccessFile file, short offset)
				{
					try
					{
						int page = offset / pageSize + 1;
						short[] offsets = new short[0];
						offsets = getoffsetArray(file, page);
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
						
					}
					catch (Exception excep)
					{
						System.out.println("Error in removeOffset: "+ excep );
					}
				}

		

}