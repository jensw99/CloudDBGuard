package databases;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import misc.Misc;
import misc.Timer;
import enums.ColumnType;
import enums.RequestType;



/**
 * A class for accessing Cassandra Results
 * 
 * @author Tim Waage
 */
public class ResultCassandra extends Result{
	
	// reference to a native cassandra result set 
	private ResultSet cassandraResults = null;
	
	// results from all String columns
	private HashMap<String, HashMap<byte[], String>> stringColumns = new HashMap<String, HashMap<byte[], String>>();
		
	// results from all Integer columns
	private HashMap<String, HashMap<byte[], Long>> intColumns = new HashMap<String, HashMap<byte[], Long>>();
			
	// results from all Byte Blob columns
	private HashMap<String, HashMap<byte[], byte[]>> byteColumns = new HashMap<String, HashMap<byte[], byte[]>>();
		
	// results from all String columns
	private HashMap<String, HashMap<byte[], Set<String>>> stringSetColumns = new HashMap<String, HashMap<byte[], Set<String>>>();
			
	// results from all Integer columns
	private HashMap<String, HashMap<byte[], Set<Long>>> intSetColumns = new HashMap<String, HashMap<byte[], Set<Long>>>();
				
	// results from all Byte Blob columns
	private HashMap<String, HashMap<byte[], Set<byte[]>>> byteSetColumns = new HashMap<String, HashMap<byte[], Set<byte[]>>>();
		
	// the number of rows in this result
	private long size;
	
	/**
	 * Constructor
	 * 
	 * @param _request request object that lead to this result
	 * @param _cassandraResults reference to a native cassandra result set
	 */
	public ResultCassandra(Request _request, ResultSet _cassandraResults, long _runtime) {
		
		super(_request, _runtime);
				
		cassandraResults = _cassandraResults;	
			
		// parsing only necessary if it was a read request
		if((_request.getType() == RequestType.READ)||
		   (_request.getType() == RequestType.READ_WITHOUT_IV)||
		   (_request.getType() == RequestType.READ)) parse();
		
	}
	
	
	
	/**
	 * parses the databases native result object
	 */
	private void parse() {
		
		
		
		size = 0;
		
		if(!cassandraResults.isExhausted()){
			
			ColumnDefinitions definitions = cassandraResults.getColumnDefinitions();
			
			for(int i=0; i<definitions.size(); i++) {
				
				if(definitions.getType(i) == DataType.blob()) byteColumns.put(definitions.getName(i), new HashMap<byte[], byte[]>());
				else if(definitions.getType(i) == DataType.varchar()) stringColumns.put(definitions.getName(i), new HashMap<byte[], String>());
				else if(definitions.getType(i) == DataType.bigint()) intColumns.put(definitions.getName(i), new HashMap<byte[], Long>());
				else if(definitions.getType(i).toString().equals(DataType.set(DataType.blob()).toString())) byteSetColumns.put(definitions.getName(i), new HashMap<byte[], Set<byte[]>>());
				else if(definitions.getType(i).toString().equals(DataType.set(DataType.varchar()).toString())) stringSetColumns.put(definitions.getName(i), new HashMap<byte[], Set<String>>());
				else if(definitions.getType(i).toString().equals(DataType.set(DataType.bigint()).toString())) intSetColumns.put(definitions.getName(i), new HashMap<byte[], Set<Long>>());
				
				
			}
			
			Iterator<Row> it = cassandraResults.iterator();
			
			ColumnType rowkeyType = request.getId().getTable().getRowkeyColumn().getType();
			String rowkeyColumnName = request.getId().getTable().getRowkeyColumnName();
			boolean rowkeyColumnEncrypted = request.getId().getTable().getRowkeyColumn().isEncrypted();
			
			
			// iterate through the rows
			while(it.hasNext()) {
				
				size++;
				
				Row row = it.next();
				
				// get the rowkey
				byte[] rowkey = null;
				
				if((rowkeyType == ColumnType.BYTE)||(rowkeyColumnEncrypted)) {
					byte[] tmp = new byte[row.getBytes(rowkeyColumnName).remaining()];
					row.getBytes(rowkeyColumnName).get(tmp);
					rowkey = tmp;
				}
				else if((rowkeyType == ColumnType.STRING)&&(!rowkeyColumnEncrypted)) rowkey = row.getString(rowkeyColumnName).getBytes();
				else if((rowkeyType == ColumnType.INTEGER)&&(!rowkeyColumnEncrypted)) rowkey = Misc.longToBytes(row.getLong(rowkeyColumnName));
				
				
				for(int i=0; i<definitions.size(); i++) {
					
					if(definitions.getType(i) == DataType.blob()) {
						byte[] tmp = new byte[row.getBytes(definitions.getName(i)).remaining()];
						row.getBytes(definitions.getName(i)).get(tmp);
						byteColumns.get(definitions.getName(i)).put(rowkey, tmp);					
					}
					else if(definitions.getType(i).toString().equals(DataType.set(DataType.blob()).toString())) byteSetColumns.get(definitions.getName(i)).put(rowkey, Misc.byteBufferHashSet2ByteHashSet(row.getSet(definitions.getName(i), ByteBuffer.class)));
					else if(definitions.getType(i) == DataType.varchar()) stringColumns.get(definitions.getName(i)).put(rowkey, row.getString(definitions.getName(i)));
					else if(definitions.getType(i) == DataType.bigint()) intColumns.get(definitions.getName(i)).put(rowkey, row.getLong(definitions.getName(i)));
					else if(definitions.getType(i).toString().equals(DataType.set(DataType.varchar()).toString())) stringSetColumns.get(definitions.getName(i)).put(rowkey, row.getSet(definitions.getName(i),  String.class));
					else if(definitions.getType(i).toString().equals(DataType.set(DataType.bigint()).toString())) intSetColumns.get(definitions.getName(i)).put(rowkey, row.getSet(definitions.getName(i),  Long.class));
					
											
				}
			}
		}
		
		
	}
	
	
	
	/**
	 * returns a list of string sets, made of string sets found in seperate rows
	 * @param column that contains string sets
	 * @return a list of string sets, made of string sets found in seperate rows
	 */
	public ArrayList<Set<String>> getStringSetsFrom(String column) {
		
		if(!stringSetColumns.containsKey(column)) return null;
		
		ArrayList<Set<String>> results = new ArrayList<Set<String>>();
		
		for(Set<String> ss : stringSetColumns.get(column).values()) results.add(ss);
		
		return results;	
	}
	
	
	
	/**
	 * returns a list of long sets, made of long sets found in seperate rows
	 * @param column that contains long sets
	 * @return a list of long sets, made of long sets found in seperate rows
	 */
	public ArrayList<Set<Long>> getIntSetsFrom(String column) {
		
		ArrayList<Set<Long>> results = new ArrayList<Set<Long>>();
				
		if(!cassandraResults.isExhausted()){
			Iterator<Row> it = cassandraResults.iterator();
			while(it.hasNext()) {
				results.add(it.next().getSet(column, Long.class));					
			}
			return results;
		}
		else return null;
	}
	
	
	
	/**
	 * returns a list of byte[] sets, made of byte[] sets found in seperate rows
	 * @param column that contains byte[] sets
	 * @return a list of byte[] sets, made of byte[] sets found in seperate rows
	 */
	public ArrayList<Set<byte[]>> getByteSetsFrom(String column) {
		
		ArrayList<Set<byte[]>> results = new ArrayList<Set<byte[]>>();
		
		if(byteSetColumns.get(column) != null) {
			for(Set<byte[]> sb : byteSetColumns.get(column).values()) results.add(sb);
			return results;
		}
		
		return null;
		
	}
	
	
	
	/**
	 * Returns a Hashmap<rowkey/primary key, string column value>
	 * @param stringColumn that contains string values
	 * @return a Hashmap<rowkey/primary key, string column value> or null, 
	 * rowkey is always returned as byte[]
	 */
	public HashMap<byte[], String> getKeyStringsFrom(String stringColumn) {
		
		return stringColumns.get(stringColumn.toLowerCase());		
	}
		
	

	/**
	 * Returns a Hashmap<rowkey/primary key, string set column value>
	 * @param stringSetColumn that contains string sets
	 * @return a Hashmap<rowkey/primary key, string set value> or null, 
	 * rowkey is always returned as byte[]
	 */
	public HashMap<byte[], Set<String>> getKeyStringSetsFrom(String stringSetColumn) {
		
		return stringSetColumns.get(stringSetColumn.toLowerCase());
	}
	
	
	/**
	 * Returns a Hashmap<rowkey/primary key, integer column value>
	 * @param column that contains the OPE encrypted values
	 * @return a Hashmap<rowkey/primary key, integer column value> or null, 
	 * if the cassandraResultSet was empty in the first place, rowkey is always returned as byte[]
	 */
	public HashMap<byte[], Long> getKeyIntsFrom(String intColumn) {
		
		return intColumns.get(intColumn.toLowerCase());
	}
	
	
	
	/**
	 * Returns a Hashmap<rowkey/primary key, long set column value>
	 * @param longSetColumn that contains longg sets
	 * @return a Hashmap<rowkey/primary key, long set value> or null, 
	 * rowkey is always returned as byte[]
	 */
	public HashMap<byte[], Set<Long>> getKeyIntSetsFrom(String longSetColumn) {
		
		return intSetColumns.get(longSetColumn.toLowerCase());
	}
	
	
	
	/**
	 * Returns a Hashmap<rowkey/primary key, byte[] column value>
	 * @param column that contains the byte[] values
	 * @return a Hashmap<rowkey/primary key, byte[] column value> or null, 
	 * if the cassandraResultSet was empty in the first place, rowkey is always returned as byte[]
	 */
	public HashMap<byte[], byte[]> getKeyBytesFrom(String byteColumn) {
		
		return byteColumns.get(byteColumn.toLowerCase());
	}
	
	
	
	
	
	/**
	 * Returns a Hashmap<rowkey/primary key, byte[] set column value>
	 * @param byteSetColumn that contains byte[] sets
	 * @return a Hashmap<rowkey/primary key, byte set value> or null, 
	 * rowkey is always returned as byte[]
	 */
	public HashMap<byte[], Set<byte[]>> getKeyByteSetsFrom(String byteSetColumn) {
		
		return byteSetColumns.get(byteSetColumn.toLowerCase());
	}
	
	
	
	/**
	 * returns a map of separate Strings, made of the Strings of two string columns
	 * @param column1 the first column to be read, use row key if null
	 * @param column2 the second column to be read
	 * @return a map of separate Strings, made of the Strings of two string columns or null, if the cassandraResultSet was empty in the first place
	 */
	public HashMap<String,String> getStringStringsFor(String column1, String column2) {
		
		HashMap<String, String> results = new HashMap<String, String>();
		
		if(column1 == null) column1 = id.getTable().getRowkeyColumnName();
			
		if(!cassandraResults.isExhausted()){
			Iterator<Row> it = cassandraResults.iterator();
			while(it.hasNext()) {
				Row tmp = it.next();
				results.put(tmp.getString(column1), tmp.getString(column2));
			}
			return results;
		}
		else return null;
		
	}
	
	
	
	/**
	 * returns a map <String, set of byte arrays> of the given columns
	 * @param column1 the first column to be read
	 * @param column2 the second column to be read
	 * @return a map <String, set of byte arrays> of the given columns or null, if the cassandraResultSet was empty in the first place
	 */
	public HashMap<String, Set<ByteBuffer>> getStringByteBuffersFor(String stringColumn, String byteBufferColumn) {
		
		HashMap<String, Set<ByteBuffer>> results = new HashMap<String, Set<ByteBuffer>>();
		
		if(!cassandraResults.isExhausted()){
			Iterator<Row> it = cassandraResults.iterator();
				
			// iterate through the rows
			while(it.hasNext()) {
				Row tmp = it.next();
				
				Set<ByteBuffer> byteBuffers = new HashSet<ByteBuffer>();
				Iterator<ByteBuffer> it2 = tmp.getSet(byteBufferColumn, ByteBuffer.class).iterator();
				
				// iterate through the set
				while(it2.hasNext()) {
					ByteBuffer b = it2.next();
					byte[] x = new byte[b.remaining()];
					b.get(x).array();
					byteBuffers.add(ByteBuffer.wrap(x));
				}
								
				results.put(tmp.getString(stringColumn), byteBuffers);
									
			}
			return results;
		}
		else return null;
	}
	
	

	/**
	 * prints a resultset to the Console
	 * @param rowsToPrint
	 */
	public void print(int rowsToPrint) {
		
		int rowsPrinted = 0;
		int rowCounter = 0;
		
		System.out.println("Cassandra Results:");
			
		if(!cassandraResults.isExhausted()){
				
			ColumnDefinitions definitions = cassandraResults.getColumnDefinitions();
			
			for(int i=0; i<definitions.size(); i++) System.out.print(Misc.makeLength(definitions.getName(i), 20) + "   ");
			System.out.println();
			
			Iterator<Row> it = cassandraResults.iterator();
			
			// iterate through the rows
			while(it.hasNext()) {
				
				Row row = it.next();
				
				if(rowsPrinted < rowsToPrint) {
					for(int i=0; i<definitions.size(); i++) {
					
						if(definitions.getType(i) == DataType.varchar()) System.out.print(Misc.makeLength(row.getString(definitions.getName(i)), 20) + " | ");
						if(definitions.getType(i) == DataType.bigint()) System.out.print(Misc.makeLength(String.valueOf(row.getLong(definitions.getName(i))), 20) + " | ");
						if(definitions.getType(i) == DataType.blob()) {
							
							byte[] tmp = new byte[row.getBytes(definitions.getName(i)).remaining()];
							row.getBytes(definitions.getName(i)).get(tmp);
							
							System.out.print(Misc.makeLength(Misc.ByteArrayToString(tmp), 20) + " | ");
						}
						
					}
					System.out.println();
					rowsPrinted++;
				}										
				rowCounter++;
			}
			
			System.out.println("(printed " + rowsPrinted + " of " + rowCounter + " results)");	
		}
		else System.out.println("The result contains no rows.");
	}



	@Override
	public boolean isEmpty() {
	
		return cassandraResults.isExhausted();
	}



	@Override
	public long getSize() {
		
		return size;
	}
	
}
