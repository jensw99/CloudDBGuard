package databases;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.NavigableMap;
import java.util.Set;

import misc.Misc;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import enums.ColumnType;



/**
 * A class for accessing HBase Results
 * 
 * @author Tim Waage
 */

public class ResultHBase extends Result{
		
	// reference to a native hbase result set
	private ResultScanner hbaseResults = null;
	
	// the rows of this result
	private HashMap<byte[], NavigableMap<byte[],NavigableMap<byte[],byte[]>>> rows = new HashMap<byte[], NavigableMap<byte[],NavigableMap<byte[],byte[]>>>();
	
	
	
	/**
	 * Constructor
	 * 
	 * @param _request request object that lead to this result
	 * @param _hbaseResults reference to a native hbase result set
	 */
	public ResultHBase(Request _request, ResultScanner _hbaseResults, long _runtime) {
		
		super(_request, _runtime);
				
		hbaseResults = _hbaseResults;		
		
		try {
			for (org.apache.hadoop.hbase.client.Result result = hbaseResults.next(); (result != null); result = hbaseResults.next()) {
			
				if(result.size() > 0) {
					rows.put(result.getRow(), result.getNoVersionMap());
				}
			}
		}
		catch(Exception e) {}
	}
	
	
	
	
	
	/**
	 * returns a list of string sets, made of string sets found in seperate rows
	 * @param column that contains string sets
	 * @return a list of string sets, made of string sets found in seperate rows
	 */
	public ArrayList<Set<String>> getStringSetsFrom(String column) {
		
		ArrayList<Set<String>> results = new ArrayList<Set<String>>();
			
		try {
			for (org.apache.hadoop.hbase.client.Result result = hbaseResults.next(); (result != null); result = hbaseResults.next()) {
			
				if(result.size() > 0) {
				
					NavigableMap<byte[],NavigableMap<byte[],byte[]>> row = result.getNoVersionMap();
					NavigableMap<byte[],byte[]> byteRowValues = row.get(column.getBytes());
					
					Set<String> stringRowValues = new HashSet<String>();
					for(byte[] key : byteRowValues.keySet()) stringRowValues.add(new String(byteRowValues.get(key)));
					results.add(stringRowValues);
					
				}	
				else return null;
			}
		}
		catch(IOException e) {
		}
		
		return results;	
	}
	
	
	
	/**
	 * returns a list of long sets, made of long sets found in seperate rows
	 * @param column that contains long sets
	 * @return a list of long sets, made of long sets found in seperate rows
	 */
	public ArrayList<Set<Long>> getIntSetsFrom(String column) {
		
		ArrayList<Set<Long>> results = new ArrayList<Set<Long>>();
		
		try {
			for (org.apache.hadoop.hbase.client.Result result = hbaseResults.next(); (result != null); result = hbaseResults.next()) {
			
				if(result.size() > 0) {
				
					NavigableMap<byte[],NavigableMap<byte[],byte[]>> row = result.getNoVersionMap();
					NavigableMap<byte[],byte[]> byteRowValues = row.get(column.getBytes());
					
					Set<Long> longRowValues = new HashSet<Long>();
					for(byte[] key : byteRowValues.keySet()) longRowValues.add(Misc.bytesToLong(byteRowValues.get(key)));
					results.add(longRowValues);
					
				}	
				else return null;
			}
		}
		catch(IOException e) {
		}

		return results;
	}
	
	
	
	/**
	 * returns a list of byte[] sets, made of byte[] sets found in seperate rows
	 * @param column that contains byte[] sets
	 * @return a list of byte[] sets, made of byte[] sets found in seperate rows
	 */
	public ArrayList<Set<byte[]>> getByteSetsFrom(String column) {
		
		HashMap<byte[], Set<byte[]>> tmp = getKeyByteSetsFrom(column);
		
		ArrayList<Set<byte[]>> res = new ArrayList<Set<byte[]>>();
		for(Set<byte[]> s : tmp.values()) res.add(s);
		
		return res;
		
	}
	
	
	
	/**
	 * Returns a Hashmap<rowkey/primary key, string column value>
	 * @param stringColumn that contains string values
	 * @return a Hashmap<rowkey/primary key, string column value> or null, 
	 * rowkey is always returned as byte[]
	 */
	public HashMap<byte[], String> getKeyStringsFrom(String stringColumn) {
		
		HashMap<byte[], String> results = new HashMap<byte[], String>();
		
		if(!stringColumn.equals(request.getId().getTable().getRowkeyColumnName())) {
		
			for(byte[] rowkey : rows.keySet()) {
			
				NavigableMap<byte[],NavigableMap<byte[],byte[]>> row = rows.get(rowkey);
				NavigableMap<byte[],byte[]> rowValues = row.get(stringColumn.getBytes());
				
				for(byte[] key : rowValues.keySet()) results.put(rowkey, Bytes.toString(rowValues.get(key)));
			}
		}
		else {
			for(byte[] rowkey : rows.keySet()) results.put(rowkey, Bytes.toString(rowkey));
		}
		
		return results;
	}
	
	
	/**
	 * Returns a Hashmap<rowkey/primary key, string set column value>
	 * @param stringSetColumn that contains string sets
	 * @return a Hashmap<rowkey/primary key, string set value> or null, 
	 * rowkey is always returned as byte[]
	 */
	public HashMap<byte[], Set<String>> getKeyStringSetsFrom(String stringSetColumn) {
		
		HashMap<byte[], Set<String>> results = new HashMap<byte[], Set<String>>();
		
		for(byte[] rowkey : rows.keySet()) {
			
			NavigableMap<byte[],NavigableMap<byte[],byte[]>> row = rows.get(rowkey);
			NavigableMap<byte[],byte[]> byteRowValues = row.get(stringSetColumn.getBytes());
			
			Set<String> stringSetRowValues = new HashSet<String>();
			if(byteRowValues != null) { //maybe there's no element in the set
				for(byte[] key : byteRowValues.keySet()) stringSetRowValues.add(new String(byteRowValues.get(key)));
			}
			results.put(rowkey, stringSetRowValues);								
		}
	
		return results;		
	}
	
	
	/**
	 * Returns a Hashmap<rowkey/primary key, integer column value>
	 * @param column that contains the OPE encrypted values
	 * @return a Hashmap<rowkey/primary key, integer column value> or null, 
	 * if the cassandraResultSet was empty in the first place, rowkey is always returned as byte[]
	 */
	public HashMap<byte[], Long> getKeyIntsFrom(String intColumn) {
		
		HashMap<byte[], Long> results = new HashMap<byte[], Long>();
		
		if(!intColumn.equals(request.getId().getTable().getRowkeyColumnName())) {
		
			for(byte[] rowkey : rows.keySet()) {
			
				NavigableMap<byte[],NavigableMap<byte[],byte[]>> row = rows.get(rowkey);
				NavigableMap<byte[],byte[]> rowValues = row.get(intColumn.getBytes());
				
				for(byte[] key : rowValues.keySet()) results.put(rowkey, Misc.bytesToLong(rowValues.get(key)));
			
			}
		}
		else {
			for(byte[] rowkey : rows.keySet()) results.put(rowkey, Bytes.toLong(rowkey));
		}
		return results;
	}
			
	
	
	
	/**
	 * Returns a Hashmap<rowkey/primary key, long set column value>
	 * @param longSetColumn that contains longg sets
	 * @return a Hashmap<rowkey/primary key, long set value> or null, 
	 * rowkey is always returned as byte[]
	 */
	public HashMap<byte[], Set<Long>> getKeyIntSetsFrom(String longSetColumn) {
		
		HashMap<byte[], Set<Long>> results = new HashMap<byte[], Set<Long>>();
	
		for(byte[] rowkey : rows.keySet()) {
			
			NavigableMap<byte[],NavigableMap<byte[],byte[]>> row = rows.get(rowkey);
			NavigableMap<byte[],byte[]> byteRowValues = row.get(longSetColumn.getBytes());
			
			Set<Long> longSetRowValues = new HashSet<Long>();
			if(byteRowValues != null) { //maybe there's no element in the set
				for(byte[] key : byteRowValues.keySet()) longSetRowValues.add(Misc.bytesToLong(byteRowValues.get(key)));
			}
			results.put(rowkey, longSetRowValues);								
		}
		
		return results;	
	}
	
	
	
	/**
	 * Returns a Hashmap<rowkey/primary key, byte[] column value>
	 * @param column that contains the byte[] values
	 * @return a Hashmap<rowkey/primary key, byte[] column value> or null, 
	 * if the cassandraResultSet was empty in the first place, rowkey is always returned as byte[]
	 */
	public HashMap<byte[], byte[]> getKeyBytesFrom(String byteColumn) {
		
		HashMap<byte[], byte[]> results = new HashMap<byte[], byte[]>();
		
		for(byte[] rowkey : rows.keySet()) {
			
			NavigableMap<byte[],NavigableMap<byte[],byte[]>> row = rows.get(rowkey);
			NavigableMap<byte[],byte[]> rowValues = row.get(byteColumn.getBytes());
				
			for(byte[] key : rowValues.keySet()) results.put(rowkey, rowValues.get(key));
			
		}
		
		return results;
	
	}
	
	
	
	/**
	 * Returns a Hashmap<rowkey/primary key, byte[] set column value>
	 * @param byteSetColumn that contains byte[] sets
	 * @return a Hashmap<rowkey/primary key, byte set value> or null, 
	 * rowkey is always returned as byte[]
	 */
	public HashMap<byte[], Set<byte[]>> getKeyByteSetsFrom(String byteSetColumn) {
		
		HashMap<byte[], Set<byte[]>> results = new HashMap<byte[], Set<byte[]>>();
		
		for(byte[] rowkey : rows.keySet()) {
			
			NavigableMap<byte[],NavigableMap<byte[],byte[]>> row = rows.get(rowkey);
			NavigableMap<byte[],byte[]> byteRowValues = row.get(byteSetColumn.getBytes());
			
			Set<byte[]> byteSetRowValues = new HashSet<byte[]>();
			if(byteRowValues != null) { //maybe there's no element in the set
				for(byte[] key : byteRowValues.keySet()) byteSetRowValues.add(byteRowValues.get(key));
			}
			results.put(rowkey, byteSetRowValues);								
		}
		
		return results;
		
	}
	
	
	
	/**
	 * returns a map of separate Strings, made of the Strings of two string columns
	 * @param column1 the first column to be read, use row key if null
	 * @param column2 the second column to be read
	 * @return a map of separate Strings, made of the Strings of two string columns or null, if the cassandraResultSet was empty in the first place
	 */
	public HashMap<String,String> getStringStringsFor(String column1, String column2) {
		
		HashMap<String, String> results = new HashMap<String, String>();
		
		try {
			for (org.apache.hadoop.hbase.client.Result result = hbaseResults.next(); (result != null); result = hbaseResults.next()) {
				
				if(result.size() > 0) {
					
					NavigableMap<byte[],NavigableMap<byte[],byte[]>> row = result.getNoVersionMap();					
						
					// if one of the columns is null, use the roykey
					if(column1 == null)
					results.put(new String(result.getRow()),
							    new String(row.get(column2.getBytes()).get(row.get(column2.getBytes()).firstKey())));
					else if(column2 == null)
						results.put(new String(row.get(column1.getBytes()).get(row.get(column1.getBytes()).firstKey())),
								    new String(result.getRow()));
					else
						results.put(new String(row.get(column1.getBytes()).get(row.get(column1.getBytes()).firstKey())),
								    new String(row.get(column2.getBytes()).get(row.get(column2.getBytes()).firstKey())));
					
				}				
			}
		}
		catch(IOException e) {
			
		}
			
		return results;
	}
	
	
	
	/**
	 * returns a map <String, set of byte arrays> of the given columns
	 * @param column1 the first column to be read
	 * @param column2 the second column to be read
	 * @return a map <String, set of byte arrays> of the given columns or null, if the cassandraResultSet was empty in the first place
	 */
	public HashMap<String, Set<ByteBuffer>> getStringByteBuffersFor(String stringColumn, String byteBufferColumn) {
		
		HashMap<String, Set<ByteBuffer>> results = new HashMap<String, Set<ByteBuffer>>();
		
		try {
			for (org.apache.hadoop.hbase.client.Result result = hbaseResults.next(); (result != null); result = hbaseResults.next()) {
				
				if(result.size() > 0) {
					
					NavigableMap<byte[],NavigableMap<byte[],byte[]>> row = result.getNoVersionMap();					
						
					String string = null;
					try {
						string = new String(row.get(stringColumn.getBytes()).get(row.get(stringColumn.getBytes()).firstKey()));
					}
					catch (Exception e){
						// when this happens the string column was the rowkey in HBase
						string = new String(result.getRow());
					}
					
					Set<ByteBuffer> byteBuffers = new HashSet<ByteBuffer>();
					
					HashSet<byte[]> tmp = new HashSet<byte[]>(row.get(Bytes.toBytes(byteBufferColumn)).values());
					
					for(byte[] b : tmp) {
													
						byteBuffers.add(ByteBuffer.wrap(b));
					}
					
					results.put(string, byteBuffers);
				
				}				
			}
		}
		catch(IOException e) {
				
		}
			
		// because always return null, when there are no results
		if(results.size() == 0) return null;
		else return results;
	}
	
	
	
	
	
	/**
	 * prints a resultset to the Console
	 * @param rowsToPrint
	 */
	public void print(int rowsToPrint) {
		
		int rowsPrinted = -1; // make the ehader does not count;
		int rowCounter = 0;
		boolean headerPrinted = false;
		
		System.out.println("HBase Results:");
			
		try {
				
			for (org.apache.hadoop.hbase.client.Result result = hbaseResults.next(); (result != null); result = hbaseResults.next()) {
			
				if(result.size() > 0) {
				
					NavigableMap<byte[],NavigableMap<byte[],byte[]>> row = result.getNoVersionMap();
					
					//get the available columns from the originating request
					if(rowsPrinted < rowsToPrint) {
						for(int i=0; i<request.getId().getColumns().size(); i++) {
							
							String columnName = request.getId().getColumns().get(i);
							
							if(!headerPrinted) {
								System.out.print(Misc.makeLength(columnName, 22));
							}
							else {
							
								NavigableMap<byte[],byte[]> rowValues = row.get(columnName.getBytes());
							
								if(request.getId().getTable().getColumnByName(columnName).getType() == ColumnType.STRING) System.out.print(Misc.makeLength(Misc.ByteArrayToCharString(rowValues.get("col".getBytes())), 20) + " | ");
								if(request.getId().getTable().getColumnByName(columnName).getType() == ColumnType.INTEGER) System.out.print(Misc.makeLength(String.valueOf(Misc.bytesToLong(rowValues.get("col".getBytes()))), 20) + " | ");
								if(request.getId().getTable().getColumnByName(columnName).getType() == ColumnType.BYTE) System.out.print(Misc.makeLength(Misc.ByteArrayToCharString(rowValues.get("col".getBytes())), 20) + " | ");
							}	
						}
						rowsPrinted++;
						System.out.println();
						if(!headerPrinted) headerPrinted = true;
					}
					rowCounter++;
				}
				else System.out.println("The result contains no rows.");
			}
			System.out.println("(printed " + rowsPrinted + " of " + rowCounter + " results)");
		}
		catch(IOException e) {
			
		}
		
	}



	@Override
	public boolean isEmpty() {
		
		return !hbaseResults.iterator().hasNext();
	}





	@Override
	public long getSize() {
		// TODO Auto-generated method stub
		return 0;
	}

}
