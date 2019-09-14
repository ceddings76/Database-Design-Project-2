package brownDB;

import java.util.Scanner;
import java.util.ArrayList;
import java.io.File;
import java.util.Stack;
import brownDB.DBVariables;
import brownDB.DBTable;


public class DBPrompt implements DBVariables
{
public static boolean isExit = false;
	
	public static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	public static void main(String[] args) 
	{
		//initialize the directories and catalog tables if not there.
		boolean directory = DBTable.dir();
		if(!directory)
		{
			DBTable.initializeDataStore();
			System.out.println("Initialize directories and catalog");
		}
		/* Display the welcome screen */
		DBTable.screenMessage();

		// Variable to collect user input from the prompt 
		String userQuery = ""; 
		
		while( isExit != true) 
		{
			System.out.print(PRO_LAB);
			userQuery = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			userQuery = userQuery.replace("\n", "");
			parseUserCommand(userQuery);
			
			
		}
		System.out.println("Exiting...");
		System.exit(0); 
		
	}
	
	public static void parseUserCommand (String userCommand) 
	{
		String[] command = userCommand.split(" ");
	
		command[0].trim();
		switch (command[0]) 
		{
			case "create":
				parserString(userCommand);
				break;
			case "show":
				DBTable.Show(userCommand);
				break;
			case "insert":
				if(command[1].equals("into"))
				{
					String Table = command[2];
					Table = Table.replace(" ", "").replace("(", "").replace(")", "").trim();
					if(command[3].equals("values"))
					{
						ArrayList<String> val = new ArrayList<String>();
						for(int i = 4; i < command.length; i++)
						{
							String tmp = command[i].replace("(", "").replace(")", "").replace(",", "").trim();
							val.add(tmp);
						}
						
						String[] values = new String[val.size()];
						for(int i = 0; i < val.size(); i++)
						{
							values[i] = val.get(i);
						}
						
						DBTable.insertInto(Table, values);
					}
					else
					{
						System.out.println("Syntax Error, Enter the query again.");
					}
				}
				else
				{
					System.out.println("Syntax Error, Enter the query again.");
				}
				break;
			case "drop":
				if(command[1].equals("table"))
				{
					System.out.println("Table to drop: " + command[2]);
					String table = command[2].replace(" ", "").replace(";", "").trim();
					DBTable.dropTable(table);
				}
				else
				{
					System.out.println("Syntax Error, Enter the query again.");
				}
				break;
			case "select":
				if(command.length<5)
				{
					String temp = command[3].trim();
					DBTable.selectAll(temp);
				}
				else if(command.length>= 5)
				{
					selectParser(userCommand);
				}
				else
				{
					System.out.println("Syntax error, please try again.");
				}
				break; 
			case "update":
				updateParseFunc(userCommand);				
				//if(command[2].equals("set"))
				//{
				//	DBTable.update(command[1], command[3], command[5], command[4]);
				//}
				break;
			case "delete":
				deleteParser(userCommand);
				System.out.println();
				break;
			case "help":
				DBTable.help();
				break;
			case "exit":
				isExit = true;
				break;
			default:
				System.out.println("The command: \"" + userCommand + "\" is not supported.");
				break;
		}
	}

	public static void parserString(String queryParse)
	{
		//if the query has any brackets coming in the it then this will keep a track as we don't need 
		//brackets to parse... we need to check if there's more than one pair of '(' and ')'
		Stack<Character> stk = new Stack<>();
        int firstBracket = -1;
        int firstClosing = -1;
        
        //this function starts checking the chars from the beginning index 0...
        //then moves till the first ( is seen and then changes the value of firstbracket from -1 to the position
        //where it was found
			for (int idx = 0; idx < queryParse.length(); idx++) {
				if (queryParse.charAt(idx) == '(') {
					firstBracket = idx;
					break;
				}
			}
		
		//we select the substring from the entire string
		//example: create table employee(id int, name text); will give output
		//create table employee in "command" var.
		//and employee in "tableName" var
		String command = queryParse.substring(0, firstBracket);
			//System.out.println("first command store   "+command);
		String tableName = command.replaceAll("( )+", " ").split(" ")[2]; 

		command = queryParse.substring(0, firstBracket - tableName.length()).trim();
			//System.out.println("second command store   "+command);

		for (int idx = firstBracket; idx < queryParse.length(); idx++) {
			if (queryParse.charAt(idx) == '(') {
				stk.add(queryParse.charAt(idx));
				}
			else if (queryParse.charAt(idx) == ')') {
				char c = stk.pop();
				if (stk.size() == 0) {
					firstClosing = idx;
					break;
				}
			}
		}
		
		String attrString = queryParse.substring(firstBracket + 1, firstClosing);
			//System.out.println("the attribute string   "+attrString);
		String attrArr[] = attrString.split(",");
		boolean notNull[] = new boolean[attrArr.length];
		boolean primaryKey[] = new boolean[attrArr.length];
		
//		String notnull[] = new String[attrArr.length];
//		String primkey[] = new String[attrArr.length];
//			
		String attributes[] = new String[attrArr.length];
		String datatype[] = new String[attrArr.length];
		
		
		for (int idx = 0; idx < attrArr.length; idx++) {
			String t[] = attrArr[idx].replace("( )+", " ").trim().split(" ");

			attributes[idx] = t[0];
			datatype[idx] = t[1].toUpperCase();

			if (t.length > 2) {
				String x = "";
				int ind = 0;
				for (int idx1 = 2; idx1 < t.length; idx1++) {
					x += t[idx1] + " ";

				}
				if (x.contains("not null")) {
					notNull[idx] = true;
					//notnull[idx] = "YES";
				} 
//				else if(!x.contains("not null")) {
//					notnull[idx] = "NO";
//				}
				if (x.contains("primary key")) {
					primaryKey[idx] = true;
					//primkey[idx] = "YES";
				} 
//				

			}

		}
		
		
		String[] notnull = new String[attrArr.length];
		String[] primkey = new String[attrArr.length];
		for(int idx = 0; idx < attrArr.length; idx++)
		{
			
			if(notNull[idx]==false)
			{
				notnull[idx]="NO";
			}
			else
			{
				notnull[idx]="YES";
			}
			
			if(primaryKey[idx]==false){
				primkey[idx]="NO";
			}
			else
			{
				primkey[idx]="YES";
			}
			if(primaryKey[idx]==true&&notNull[idx]==false)
			{
				primkey[idx]="YES";
				notnull[idx]="YES";
			}
		}

		DBTable.createTable(tableName, attributes, datatype, notnull);
		/*
		System.out.println(command);
		System.out.println(tableName);
		for (int idx = 0; idx < attributes.length; idx++) 
		{
			System.out.println("attribute : " + attributes[idx] );
			System.out.println("data type " + datatype[idx] );
			System.out.println("is it not null?" + notnull[idx]);
			System.out.println("primary key present? " + primkey[idx]);
			
		}
		*/
	}

	public static void selectParser(String selectQuery)
	{
			String url2 = selectQuery;
			String[] url2Split = url2.split("\\s+");
			String tbName = url2Split[3];
			System.out.println("the table name is:  "+tbName);
			//System.out.println("LENGTH AFTER SPLITTING URL2  "+url2Split.length);
					//to split token when where is detected
					//if(url2.length())
					
					if (url2Split.length == 6 || url2Split.length == 7 || url2Split.length == 8) {
						String[] operatorList = new String[] { "=", ">", "<", ">=", "<=", "<>" };
						boolean check = false;
						String operatorFound="";
						String[] finalSplit;
						String columnName = "";
						String op = "";
						String[] value;
						String val = "";
						
						// ("=|>|<|>=|<=");

						// check if it contains that operator is found.
						// check which one is found
						// store it another variable
						for (String s : operatorList) {
							if (url2.contains(s)) {
								check = true;
								operatorFound = s;
								//System.out.println("THE OPERATOR WE FOUND IS  " + operatorFound);
							}
						}

						if (check == true) {
							//System.out.println("IT CONTAINS OPERATORS");

							String[] tempUrl = url2.split("where");
							System.out.println("TEMP URL LENGTH   " + tempUrl.length);
							for(int m=0;m<tempUrl.length;m++){
								System.out.println(tempUrl[m].trim());
							}
							String part2Split = tempUrl[1].trim().replaceAll("( )+", "");
							System.out.println("PART 2 SPLIT (replace) "+ part2Split);
							//if(tempUrl.length)
							switch (operatorFound) {
							case "=":
								System.out.println("this is equals part");
								part2Split = part2Split.replaceAll("=", " = ");
								System.out.println(part2Split);
								finalSplit = part2Split.split(" ");
								System.out.println("LENGTH OF FINAL SPLIT  "+ finalSplit.length);
								for(int i=0;i<finalSplit.length;i++){
									//System.out.println("FOR LOOP");
									System.out.println(finalSplit[i].trim());
								}
								columnName=finalSplit[0];
								op=operatorFound;
								value=finalSplit[2].split(";");
								val = value[0];
								System.out.println("COL_NAME=  "+columnName);
								System.out.println("OPERATOR=  "+ op);
								System.out.println(value.length);
								if(value.length==0)
								{
									System.out.println("THE QUERY SHOULD NOT WORK");
									break;
								}
								else{
								//for(int i=0;i<value.length;i++){
									System.out.println("VALUE= "+ val);
								//}
								}
								break;
							case ">":
								System.out.println("this is greater than part");
								part2Split = part2Split.replaceAll(">", " > ");
								//System.out.println(part2Split);
								finalSplit = part2Split.split(" ");
								//System.out.println("LENGTH OF FINAL SPLIT  "+ finalSplit.length);
								for(int i=0;i<finalSplit.length;i++){
									//System.out.println("FOR LOOP");
									//System.out.println(finalSplit[i].trim());
								}
								columnName=finalSplit[0];
								op=operatorFound;
								value=finalSplit[2].split(";");
								System.out.println("COL_NAME=  "+columnName);
								System.out.println("OPERATOR=  "+ op);
								//System.out.println(value.length);
								if(value.length==0)
								{
									System.out.println("THE QUERY SHOULD NOT WORK");
									break;
								}
								else{
								for(int i=0;i<value.length;i++){
									System.out.println("VALUE= "+ value[i]);
								}
								}
								break;
							case "<":
								System.out.println("this is lesser than part");
								part2Split = part2Split.replaceAll("<", " < ");
								//System.out.println(part2Split);
								finalSplit = part2Split.split(" ");
								//System.out.println("LENGTH OF FINAL SPLIT  "+ finalSplit.length);
								for(int i=0;i<finalSplit.length;i++){
									//System.out.println("FOR LOOP");
									//System.out.println(finalSplit[i].trim());
								}
								columnName=finalSplit[0];
								op=operatorFound;
								value=finalSplit[2].split(";");
								System.out.println("COL_NAME=  "+columnName);
								System.out.println("OPERATOR=  "+ op);
								//System.out.println(value.length);
								if(value.length==0)
								{
									System.out.println("THE QUERY SHOULD NOT WORK");
									break;
								}
								else{
								for(int i=0;i<value.length;i++){
									System.out.println("VALUE= "+ value[i]);
								}
								}
								break;
							case ">=":
								System.out.println("this is greater than equals part");
								part2Split = part2Split.replaceAll(">=", " >= ");
								//System.out.println(part2Split);
								finalSplit = part2Split.split(" ");
								//System.out.println("LENGTH OF FINAL SPLIT  "+ finalSplit.length);
								for(int i=0;i<finalSplit.length;i++){
									//System.out.println("FOR LOOP");
									//System.out.println(finalSplit[i].trim());
								}
								columnName=finalSplit[0];
								op=operatorFound;
								value=finalSplit[2].split(";");
								System.out.println("COL_NAME=  "+columnName);
								System.out.println("OPERATOR=  "+ op);
								//System.out.println(value.length);
								if(value.length==0)
								{
									System.out.println("THE QUERY SHOULD NOT WORK");
									break;
								}
								else{
								for(int i=0;i<value.length;i++){
									System.out.println("VALUE= "+ value[i]);
								}
								}
								break;
							case "<=":
								System.out.println("this is lesser than equals part");
								part2Split = part2Split.replaceAll("<=", " <= ");
								//System.out.println(part2Split);
								finalSplit = part2Split.split(" ");
								//System.out.println("LENGTH OF FINAL SPLIT  "+ finalSplit.length);
								for(int i=0;i<finalSplit.length;i++){
									//System.out.println("FOR LOOP");
									//System.out.println(finalSplit[i].trim());
								}
								columnName=finalSplit[0];
								op=operatorFound;
								value=finalSplit[2].split(";");
								System.out.println("COL_NAME=  "+columnName);
								System.out.println("OPERATOR=  "+ op);
								System.out.println(value.length);
								if(value.length==0)
								{
									System.out.println("THE QUERY SHOULD NOT WORK");
									break;
								}
								else{
								for(int i=0;i<value.length;i++){
									System.out.println("VALUE= "+ value[i]);
								}
								}
								break;
							case "<>":
								System.out.println("this is NOT equals part");
								part2Split = part2Split.replaceAll("<>", " <> ");
								//System.out.println(part2Split);
								finalSplit = part2Split.split(" ");
								//System.out.println("LENGTH OF FINAL SPLIT  "+ finalSplit.length);
								for(int i=0;i<finalSplit.length;i++){
									//System.out.println("FOR LOOP");
									//System.out.println(finalSplit[i].trim());
								}
								columnName=finalSplit[0];
								op=operatorFound;
								value=finalSplit[2].split(";");
								System.out.println("COL_NAME=  "+columnName);
								System.out.println("OPERATOR=  "+ op);
								//System.out.println(value.length);
								if(value.length==0)
								{
									System.out.println("THE QUERY SHOULD NOT WORK.");
									break;
								}
								else{
								for(int i=0;i<value.length;i++){
									System.out.println("VALUE= "+ value[i]);
								}
								}
							}
							
							DBTable.selectWhere(tbName, columnName, val, op);
							 
						}

						else {
							System.out.println("DOES NOT CONTAIN ANY OPERATOR. THROW ERROR");
						}

					}		
					
					
					else if(url2Split.length==5 ) {
						System.out.println("THE FORMAT IS INCORRECT.");
						System.out.println("The syntax is: SELECT * FROM <TABLENAME> WHERE <COLNAME> <OPERATOR> <VALUE>;");
					}
					
					else
					{
						System.out.println("this is select all portion. call the select all method");
					}
					


	}

	public static void deleteParser(String deleteQuery){


	    String url2=deleteQuery;
	    String[] url2Split = url2.split("\\s+");
			String tbName = url2Split[2];
			System.out.println("the table name is:  "+tbName);
			//System.out.println("LENGTH AFTER SPLITTING URL2  "+url2Split.length);
					//to split token when where is detected
					//if(url2.length())
					
					if (url2Split.length == 5 || url2Split.length == 6 || url2Split.length == 7) {
						String[] operatorList = new String[] { "=", ">", "<", ">=", "<=", "<>" };
						boolean check = false;
						String operatorFound="";
						String[] finalSplit;
						String columnName="",op="";
						String[] value;
						String val="";
						
						
						// ("=|>|<|>=|<=");

						// check if it contains that operator is found.
						// check which one is found
						// store it another variable
						for (String s : operatorList) {
							if (url2.contains(s)) {
								check = true;
								operatorFound = s;
								//System.out.println("THE OPERATOR WE FOUND IS  " + operatorFound);
							}
						}

						if (check == true) {
							//System.out.println("IT CONTAINS OPERATORS");

							String[] tempUrl = url2.split("where");
							//System.out.println("TEMP URL LENGTH   " + tempUrl.length);
							for(int m=0;m<tempUrl.length;m++){
								//System.out.println(tempUrl[m].trim());
							}
							String part2Split = tempUrl[1].trim().replaceAll("( )+", "");
							//System.out.println("PART 2 SPLIT (replace) "+ part2Split);
							//if(tempUrl.length)
							switch (operatorFound) {
							case "=":
								System.out.println("this is equals part");
								part2Split = part2Split.replaceAll("=", " = ");
								//System.out.println(part2Split);
								finalSplit = part2Split.split(" ");
								//System.out.println("LENGTH OF FINAL SPLIT  "+ finalSplit.length);
								for(int i=0;i<finalSplit.length;i++){
									//System.out.println("FOR LOOP");
									//System.out.println(finalSplit[i].trim());
								}
								columnName=finalSplit[0];
								op=operatorFound;
								value=finalSplit[2].split(";");
								System.out.println("COL_NAME=  "+columnName);
								System.out.println("OPERATOR=  "+ op);
								//System.out.println(value.length);
								if(value.length==0)
								{
									System.out.println("THE QUERY SHOULD NOT WORK");
									break;
								}
								else{
								for(int i=0;i<value.length;i++){
									//System.out.println("VALUE= "+ value[i]);
									val=value[i];
									System.out.println("value = "+val);
								}
								}
								break;
							case ">":
								System.out.println("this is greater than part");
								part2Split = part2Split.replaceAll(">", " > ");
								//System.out.println(part2Split);
								finalSplit = part2Split.split(" ");
								//System.out.println("LENGTH OF FINAL SPLIT  "+ finalSplit.length);
								for(int i=0;i<finalSplit.length;i++){
									//System.out.println("FOR LOOP");
									//System.out.println(finalSplit[i].trim());
								}
								columnName=finalSplit[0];
								op=operatorFound;
								value=finalSplit[2].split(";");
								System.out.println("COL_NAME=  "+columnName);
								System.out.println("OPERATOR=  "+ op);
								//System.out.println(value.length);
								if(value.length==0)
								{
									System.out.println("THE QUERY SHOULD NOT WORK");
									break;
								}
								else{
								for(int i=0;i<value.length;i++){
									val=value[i];
									System.out.println("value = "+val);
								}
								}
								break;
							case "<":
								System.out.println("this is lesser than part");
								part2Split = part2Split.replaceAll("<", " < ");
								//System.out.println(part2Split);
								finalSplit = part2Split.split(" ");
								//System.out.println("LENGTH OF FINAL SPLIT  "+ finalSplit.length);
								for(int i=0;i<finalSplit.length;i++){
									//System.out.println("FOR LOOP");
									//System.out.println(finalSplit[i].trim());
								}
								columnName=finalSplit[0];
								op=operatorFound;
								value=finalSplit[2].split(";");
								System.out.println("COL_NAME=  "+columnName);
								System.out.println("OPERATOR=  "+ op);
								//System.out.println(value.length);
								if(value.length==0)
								{
									System.out.println("THE QUERY SHOULD NOT WORK");
									break;
								}
								else{
								for(int i=0;i<value.length;i++){
									val=value[i];
									System.out.println("value = "+val);
								}
								}
								break;
							case ">=":
								System.out.println("this is greater than equals part");
								part2Split = part2Split.replaceAll(">=", " >= ");
								//System.out.println(part2Split);
								finalSplit = part2Split.split(" ");
								//System.out.println("LENGTH OF FINAL SPLIT  "+ finalSplit.length);
								for(int i=0;i<finalSplit.length;i++){
									//System.out.println("FOR LOOP");
									//System.out.println(finalSplit[i].trim());
								}
								columnName=finalSplit[0];
								op=operatorFound;
								value=finalSplit[2].split(";");
								System.out.println("COL_NAME=  "+columnName);
								System.out.println("OPERATOR=  "+ op);
								//System.out.println(value.length);
								if(value.length==0)
								{
									System.out.println("THE QUERY SHOULD NOT WORK");
									break;
								}
								else{
								for(int i=0;i<value.length;i++){
									val=value[i];
									System.out.println("value = "+val);
								}
								}
								break;
							case "<=":
								System.out.println("this is lesser than equals part");
								part2Split = part2Split.replaceAll("<=", " <= ");
								//System.out.println(part2Split);
								finalSplit = part2Split.split(" ");
								//System.out.println("LENGTH OF FINAL SPLIT  "+ finalSplit.length);
								for(int i=0;i<finalSplit.length;i++){
									//System.out.println("FOR LOOP");
									//System.out.println(finalSplit[i].trim());
								}
								columnName=finalSplit[0];
								op=operatorFound;
								value=finalSplit[2].split(";");
								System.out.println("COL_NAME=  "+columnName);
								System.out.println("OPERATOR=  "+ op);
								//System.out.println(value.length);
								if(value.length==0)
								{
									System.out.println("THE QUERY SHOULD NOT WORK");
									break;
								}
								else{
								for(int i=0;i<value.length;i++){
									val=value[i];
									System.out.println("value = "+val);
								}
								}
								break;
							case "<>":
								System.out.println("this is NOT equals part");
								part2Split = part2Split.replaceAll("<>", " <> ");
								//System.out.println(part2Split);
								finalSplit = part2Split.split(" ");
								//System.out.println("LENGTH OF FINAL SPLIT  "+ finalSplit.length);
								for(int i=0;i<finalSplit.length;i++){
									//System.out.println("FOR LOOP");
									//System.out.println(finalSplit[i].trim());
								}
								columnName=finalSplit[0];
								op=operatorFound;
								value=finalSplit[2].split(";");
								System.out.println("COL_NAME=  "+columnName);
								System.out.println("OPERATOR=  "+ op);
								//System.out.println(value.length);
								if(value.length==0)
								{
									System.out.println("THE QUERY SHOULD NOT WORK.");
									break;
								}
								else{
								for(int i=0;i<value.length;i++){
									val=value[i];
									System.out.println("value = "+val);
								}
								}
							}
							 
						}

						else {
							System.out.println("DOES NOT CONTAIN ANY OPERATOR. THROW ERROR");
						}

					}		
					
					
					else if(url2Split.length==4 ) {
						System.out.println("THE FORMAT IS INCORRECT.");
						System.out.println("The syntax is: DELETE FROM <TABLENAME> WHERE <COLNAME> <OPERATOR> <VALUE>;");
					}
					
					else
					{
						System.out.println("this is delete all portion. call the sleect all method");
					}




	}

	public static void updateParseFunc (String updateQuery) {
	    String url2 = updateQuery;
			String[] url2Split = url2.split("\\s+");
			//System.out.println("url2split length " + url2Split.length);
			String tbName = url2Split[1];
			
			//THE TABLE NAME IS HERE
			System.out.println("TABLE NAME :  " + tbName);

			String[] url3 = url2.split("set");
			//System.out.println("Split after SET length " + url3.length);
			for (int i = 0; i < url3.length; i++) {
				//System.out.println(url3[i].trim().replaceAll("( )+", ""));
			}
			String firstPart = url3[0];
			//System.out.println("the first part is " + firstPart.trim());

			String secondPart = url3[1];
			//System.out.println("the second part is " + secondPart.trim());
			String[] whereSplit = secondPart.split("where");
			//System.out.println("Split after WHERE length " + whereSplit.length);
			for (int i = 0; i < whereSplit.length; i++) {
				//System.out.println(whereSplit[i].trim().replaceAll("( )", ""));
			}
			String firstCol = whereSplit[0];
			String secondCol = whereSplit[1];
			//System.out.println("firstCol " + firstCol);
			//System.out.println("secondCol " + secondCol);

			String[] operatorList = new String[] { "=", ">", "<", ">=", "<=", "<>" };
			String operatorFound1 = "";
			String operatorFound2 = "";
			if (url2Split.length == 10 || url2Split.length == 6 || url2Split.length == 8 || url2Split.length ==9 || url2Split.length == 7) {
				boolean check1 = false; // for set
				boolean check2 = false; //for where
				for (String s : operatorList) {
					if (firstCol.contains(s)) {
						check1 = true;
						operatorFound1 = s;
						//System.out.println("THE OPERATOR WE FOUND NEAR SET IS  " + operatorFound1);
					}
				}
					
				for (String s1 : operatorList) {
						if (secondCol.contains(s1)) {
							check2 = true;
							operatorFound2 = s1;
							//System.out.println("THE OPERATOR WE FOUND NEAR WHERE IS  " + operatorFound2);
						}

				}
				//STORES ALL VARIABLE DETAILS OF SET PART
				String part2Split = firstCol.trim().replaceAll("( )", "");
				String[] finalSplit1;
				String columnName1="",op="";
				String[] value1;
				String val1="";
				
				//STORES ALL VARIABLE DETAILS OF WHERE PART
				String part3Split = secondCol.trim().replaceAll("( )", "");
				String[] finalSplit2;
				String columnName2="",op2="";
				String[] value2;
				String val2="";
				
				
				
				if (check1 == true && check2 == true) {
					//CHECK FOR SET PART
					if (check1 == true) {

						switch (operatorFound1) {

						case "=":
							System.out.println("this is equals part");
							part2Split = part2Split.replaceAll("=", " = ");
							// System.out.println(part2Split);
							finalSplit1 = part2Split.split(" ");
							// System.out.println("LENGTH OF FINAL SPLIT "+
							// finalSplit1.length);
							for (int i = 0; i < finalSplit1.length; i++) {
								// System.out.println("FOR LOOP");
								// System.out.println(finalSplit1[i].trim());
							}
							columnName1 = finalSplit1[0];
							op = operatorFound1;
							value1 = finalSplit1[2].split(" ");
							System.out.println("COL_NAME (SET) =  " + columnName1);
							System.out.println("OPERATOR (SET) =  " + op);
							//System.out.println(value1.length);
							if (value1.length == 0) {
								System.out.println("THE QUERY SHOULD NOT WORK");
								break;
							} else {
								for (int i = 0; i < value1.length; i++) {
									//System.out.println("VALUE= "+ value1[i]);
									val1 = value1[i];
									System.out.println("value = " + val1);
								}
							}
							break;
						case ">":
							System.out.println("this is greater than part");
							part2Split = part2Split.replaceAll(">", " > ");
							// System.out.println(part2Split);
							finalSplit1 = part2Split.split(" ");
							// System.out.println("LENGTH OF FINAL SPLIT "+
							// finalSplit1.length);
							for (int i = 0; i < finalSplit1.length; i++) {
								// System.out.println("FOR LOOP");
								// System.out.println(finalSplit1[i].trim());
							}
							columnName1 = finalSplit1[0];
							op = operatorFound1;
							value1 = finalSplit1[2].split(" ");
							System.out.println("COL_NAME (SET) =  " + columnName1);
							System.out.println("OPERATOR (SET) =  " + op);
							//System.out.println(value1.length);
							if (value1.length == 0) {
								System.out.println("THE QUERY SHOULD NOT WORK");
								break;
							} else {
								for (int i = 0; i < value1.length; i++) {
									// System.out.println("VALUE= "+ value1[i]);
									val1 = value1[i];
									System.out.println("value = " + val1);
								}
							}
							break;

						case "<":
							System.out.println("this is lesser than part");
							part2Split = part2Split.replaceAll("<", " < ");
							// System.out.println(part2Split);
							finalSplit1 = part2Split.split(" ");
							// System.out.println("LENGTH OF FINAL SPLIT "+
							// finalSplit1.length);
							for (int i = 0; i < finalSplit1.length; i++) {
								// System.out.println("FOR LOOP");
								// System.out.println(finalSplit1[i].trim());
							}
							columnName1 = finalSplit1[0];
							op = operatorFound1;
							value1 = finalSplit1[2].split(" ");
							System.out.println("COL_NAME (SET) =  " + columnName1);
							System.out.println("OPERATOR (SET) =  " + op);
							//System.out.println(value1.length);
							if (value1.length == 0) {
								System.out.println("THE QUERY SHOULD NOT WORK");
								break;
							} else {
								for (int i = 0; i < value1.length; i++) {
									// System.out.println("VALUE= "+ value1[i]);
									val1 = value1[i];
									System.out.println("value = " + val1);
								}
							}
							break;

						case ">=":
							System.out.println("this is greater than equals part");
							part2Split = part2Split.replaceAll(">=", " >= ");
							// System.out.println(part2Split);
							finalSplit1 = part2Split.split(" ");
							// System.out.println("LENGTH OF FINAL SPLIT "+
							// finalSplit1.length);
							for (int i = 0; i < finalSplit1.length; i++) {
								// System.out.println("FOR LOOP");
								// System.out.println(finalSplit1[i].trim());
							}
							columnName1 = finalSplit1[0];
							op = operatorFound1;
							value1 = finalSplit1[2].split(" ");
							System.out.println("COL_NAME (SET) =  " + columnName1);
							System.out.println("OPERATOR (SET) =  " + op);
							//System.out.println(value1.length);
							if (value1.length == 0) {
								System.out.println("THE QUERY SHOULD NOT WORK");
								break;
							} else {
								for (int i = 0; i < value1.length; i++) {
									// System.out.println("VALUE= "+ value1[i]);
									val1 = value1[i];
									System.out.println("value = " + val1);
								}
							}
							break;

						case "<=":
							System.out.println("this is lesser than equals part");
							part2Split = part2Split.replaceAll("<=", " <= ");
							// System.out.println(part2Split);
							finalSplit1 = part2Split.split(" ");
							// System.out.println("LENGTH OF FINAL SPLIT "+
							// finalSplit1.length);
							for (int i = 0; i < finalSplit1.length; i++) {
								// System.out.println("FOR LOOP");
								System.out.println(finalSplit1[i].trim());
							}
							columnName1 = finalSplit1[0];
							op = operatorFound1;
							value1 = finalSplit1[2].split(" ");
							System.out.println("COL_NAME (SET) = " + columnName1);
							System.out.println("OPERATOR (SET) =  " + op);
							//System.out.println(value1.length);
							if (value1.length == 0) {
								System.out.println("THE QUERY SHOULD NOT WORK");
								break;
							} else {
								for (int i = 0; i < value1.length; i++) {
									// System.out.println("VALUE= "+ value1[i]);
									val1 = value1[i];
									System.out.println("value = " + val1);
								}
							}
							break;

						case "<>":
							System.out.println("this is NOT equals part");
							part2Split = part2Split.replaceAll("<>", " <> ");
							// System.out.println(part2Split);
							finalSplit1 = part2Split.split(" ");
							// System.out.println("LENGTH OF FINAL SPLIT "+
							// finalSplit1.length);
							for (int i = 0; i < finalSplit1.length; i++) {
								// System.out.println("FOR LOOP");
								// System.out.println(finalSplit1[i].trim());
							}
							columnName1 = finalSplit1[0];
							op = operatorFound1;
							value1 = finalSplit1[2].split(" ");
							System.out.println("COL_NAME (SET) =  " + columnName1);
							System.out.println("OPERATOR (SET) =  " + op);
							//System.out.println(value1.length);
							if (value1.length == 0) {
								System.out.println("THE QUERY SHOULD NOT WORK");
								break;
							} else {
								for (int i = 0; i < value1.length; i++) {
									// System.out.println("VALUE= "+ value1[i]);
									val1 = value1[i];
									System.out.println("value = " + val1);
								}
							}
							break;

						default:
							System.out.println("OPERATOR DOES NOT MATCH");
						}
					}
					
					
					
					
					
					
						// CHECK FOR THE WHERE PART
						if(check2==true)
						{
							switch (operatorFound2){
							case "=":
								System.out.println("this is equals part");
								part3Split = part3Split.replaceAll("=", " = ");
								// System.out.println(part2Split);
								finalSplit2 = part3Split.split(" ");
								// System.out.println("LENGTH OF FINAL SPLIT "+
								// finalSplit1.length);
								for (int i = 0; i < finalSplit2.length; i++) {
									 //System.out.println("FOR LOOP");
									 //System.out.println(finalSplit2[i].trim());
								}
								columnName2 = finalSplit2[0];
								op2 = operatorFound2;
								value2 = finalSplit2[2].split(";");
								System.out.println("COL_NAME (WHERE) =  " + columnName2);
								System.out.println("OPERATOR (WHERE) =  " + op2);
								//System.out.println(value1.length);
								if (value2.length == 0) {
									System.out.println("THE QUERY SHOULD NOT WORK");
									break;
								} else {
									for (int i = 0; i < value2.length; i++) {
										// System.out.println("VALUE= "+ value1[i]);
										val2 = value2[i];
										System.out.println("value = " + val2);
									}
								}
								break;
								
							case ">":
								System.out.println("this is greater than part");
								part3Split = part3Split.replaceAll(">", " > ");
								// System.out.println(part2Split);
								finalSplit2 = part3Split.split(" ");
								// System.out.println("LENGTH OF FINAL SPLIT "+
								// finalSplit1.length);
								for (int i = 0; i < finalSplit2.length; i++) {
									 //System.out.println("FOR LOOP");
									 //System.out.println(finalSplit2[i].trim());
								}
								columnName2 = finalSplit2[0];
								op2 = operatorFound2;
								value2 = finalSplit2[2].split(";");
								System.out.println("COL_NAME (WHERE) =  " + columnName2);
								System.out.println("OPERATOR (WHERE) =  " + op2);
								//System.out.println(value1.length);
								if (value2.length == 0) {
									System.out.println("THE QUERY SHOULD NOT WORK");
									break;
								} else {
									for (int i = 0; i < value2.length; i++) {
										// System.out.println("VALUE= "+ value1[i]);
										val2 = value2[i];
										System.out.println("value = " + val2);
									}
								}
								break;
							case "<":
								System.out.println("this is lesser than part");
								part3Split = part3Split.replaceAll("<", " < ");
								// System.out.println(part2Split);
								finalSplit2 = part3Split.split(" ");
								// System.out.println("LENGTH OF FINAL SPLIT "+
								// finalSplit1.length);
								for (int i = 0; i < finalSplit2.length; i++) {
									 //System.out.println("FOR LOOP");
									 //System.out.println(finalSplit2[i].trim());
								}
								columnName2 = finalSplit2[0];
								op2 = operatorFound2;
								value2 = finalSplit2[2].split(";");
								System.out.println("COL_NAME (WHERE) =  " + columnName2);
								System.out.println("OPERATOR (WHERE) =  " + op2);
								//System.out.println(value1.length);
								if (value2.length == 0) {
									System.out.println("THE QUERY SHOULD NOT WORK");
									break;
								} else {
									for (int i = 0; i < value2.length; i++) {
										// System.out.println("VALUE= "+ value1[i]);
										val2 = value2[i];
										System.out.println("value = " + val2);
									}
								}
								break;
								
							case ">=":
								System.out.println("this is greater than equals part");
								part3Split = part3Split.replaceAll(">=", " >= ");
								// System.out.println(part2Split);
								finalSplit2 = part3Split.split(" ");
								// System.out.println("LENGTH OF FINAL SPLIT "+
								// finalSplit1.length);
								for (int i = 0; i < finalSplit2.length; i++) {
									 //System.out.println("FOR LOOP");
									 //System.out.println(finalSplit2[i].trim());
								}
								columnName2 = finalSplit2[0];
								op2 = operatorFound2;
								value2 = finalSplit2[2].split(";");
								System.out.println("COL_NAME (WHERE) =  " + columnName2);
								System.out.println("OPERATOR (WHERE) =  " + op2);
								//System.out.println(value1.length);
								if (value2.length == 0) {
									System.out.println("THE QUERY SHOULD NOT WORK");
									break;
								} else {
									for (int i = 0; i < value2.length; i++) {
										// System.out.println("VALUE= "+ value1[i]);
										val2 = value2[i];
										System.out.println("value = " + val2);
									}
								}
								break;

							case "<=":
								System.out.println("this is lesser than equals part");
								part3Split = part3Split.replaceAll("<=", " <= ");
								// System.out.println(part2Split);
								finalSplit2 = part3Split.split(" ");
								// System.out.println("LENGTH OF FINAL SPLIT "+
								// finalSplit1.length);
								for (int i = 0; i < finalSplit2.length; i++) {
									 //System.out.println("FOR LOOP");
									 //System.out.println(finalSplit2[i].trim());
								}
								columnName2 = finalSplit2[0];
								op2 = operatorFound2;
								value2 = finalSplit2[2].split(";");
								System.out.println("COL_NAME (WHERE) =  " + columnName2);
								System.out.println("OPERATOR (WHERE) =  " + op2);
								//System.out.println(value1.length);
								if (value2.length == 0) {
									System.out.println("THE QUERY SHOULD NOT WORK");
									break;
								} else {
									for (int i = 0; i < value2.length; i++) {
										// System.out.println("VALUE= "+ value1[i]);
										val2 = value2[i];
										System.out.println("value = " + val2);
									}
								}
								break;
							case "<>":
								System.out.println("this is NOT equals part");
								part3Split = part3Split.replaceAll("<>", " <> ");
								// System.out.println(part2Split);
								finalSplit2 = part3Split.split(" ");
								// System.out.println("LENGTH OF FINAL SPLIT "+
								// finalSplit1.length);
								for (int i = 0; i < finalSplit2.length; i++) {
									 //System.out.println("FOR LOOP");
									 //System.out.println(finalSplit2[i].trim());
								}
								columnName2 = finalSplit2[0];
								op2 = operatorFound2;
								value2 = finalSplit2[2].split(";");
								System.out.println("COL_NAME (WHERE) =  " + columnName2);
								System.out.println("OPERATOR (WHERE) =  " + op2);
								//System.out.println(value1.length);
								if (value2.length == 0) {
									System.out.println("THE QUERY SHOULD NOT WORK");
									break;
								} else {
									for (int i = 0; i < value2.length; i++) {
										// System.out.println("VALUE= "+ value1[i]);
										val2 = value2[i];
										System.out.println("value = " + val2);
									}
								}
								break;
								
								default:
									System.out.println("OPERATOR DOES NOT MATCH");



							}
						}
					

				}
			}

			else {
				System.out.println("match syntax... error...");
			}



	}


}