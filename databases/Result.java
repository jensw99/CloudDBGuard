package databases;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import enums.DatabaseType;



/**
 * A class for accessing results in a unified way
 * @author Tim Waage
 *
 */
public abstract class Result {
	
	// path to the table the original query was executed against
	protected DBLocation id;
	
	// Request that lead to this result
	protected Request request = null;
	
	protected long runtime;
	
	
	
	/**
	 * Constructor
	 * @param _type the type this result object represents, tells from which database the results were coming
	 * @param _cassandraResults reference to a native cassandra result set, should be not null if the type is ResultType.CASSANDRA
	 * @param _hbaseResults reference to a native hbase result set, should be not null if the type is ResultType.HBASE
	 */
	public Result(Request _request, long _runtime) {
		
		request = _request;
		id = request.getId();		
		runtime = _runtime;
	}
	
	
	
	/**
	 * Get request that lead to this result
	 * @return request that lead to this result
	 */
	public Request getRequest() {
		
		return request;
	}
	
	
	/**
	 * Gets the runtime of the query, the lead to this result
	 * @return the runtime of the query, the lead to this result
	 */
	public long getRuntime() {
		
		return runtime;
	}
	
	
	/**
	 * returns a list of string sets, made of string sets found in seperate rows
	 * @param column that contains string sets
	 * @return a list of string sets, made of string sets found in seperate rows
	 */
	public abstract ArrayList<Set<String>> getStringSetsFrom(String column); 
	
	
	
	/**
	 * returns a list of long sets, made of long sets found in seperate rows
	 * @param column that contains long sets
	 * @return a list of long sets, made of long sets found in seperate rows
	 */
	public abstract ArrayList<Set<Long>> getIntSetsFrom(String column); 
	
	
	
	/**
	 * returns a list of byte[] sets, made of byte[] sets found in seperate rows
	 * @param column that contains byte[] sets
	 * @return a list of byte[] sets, made of byte[] sets found in seperate rows
	 */
	public abstract ArrayList<Set<byte[]>> getByteSetsFrom(String column);
	
	
	
	/**
	 * Returns a Hashmap<rowkey/primary key, string column value>
	 * @param stringColumn that contains string values
	 * @return a Hashmap<rowkey/primary key, string column value> or null, 
	 * rowkey is always returned as byte[]
	 */
	public abstract HashMap<byte[], String> getKeyStringsFrom(String stringColumn); 
	
	
	
	/**
	 * Returns a Hashmap<rowkey/primary key, string set column value>
	 * @param stringSetColumn that contains string sets
	 * @return a Hashmap<rowkey/primary key, string set value> or null, 
	 * rowkey is always returned as byte[]
	 */
	public abstract HashMap<byte[], Set<String>> getKeyStringSetsFrom(String stringSetColumn);
	
	
	
	/**
	 * Returns a Hashmap<rowkey/primary key, integer column value>
	 * @param column that contains the OPE encrypted values
	 * @return a Hashmap<rowkey/primary key, integer column value> or null, 
	 * if the cassandraResultSet was empty in the first place, rowkey is always returned as byte[]
	 */
	public abstract HashMap<byte[], Long> getKeyIntsFrom(String intColumn);
	
	
	
	/**
	 * Returns a Hashmap<rowkey/primary key, long set column value>
	 * @param longSetColumn that contains longg sets
	 * @return a Hashmap<rowkey/primary key, long set value> or null, 
	 * rowkey is always returned as byte[]
	 */
	public abstract HashMap<byte[], Set<Long>> getKeyIntSetsFrom(String longSetColumn);
	
	
	
	/**
	 * Returns a Hashmap<rowkey/primary key, byte[] column value>
	 * @param column that contains the byte[] values
	 * @return a Hashmap<rowkey/primary key, byte[] column value> or null, 
	 * if the cassandraResultSet was empty in the first place, rowkey is always returned as byte[]
	 */
	public abstract HashMap<byte[], byte[]> getKeyBytesFrom(String byteColumn);
	
	
	
	/**
	 * Returns a Hashmap<rowkey/primary key, byte[] set column value>
	 * @param byteSetColumn that contains byte[] sets
	 * @return a Hashmap<rowkey/primary key, byte set value> or null, 
	 * rowkey is always returned as byte[]
	 */
	public abstract HashMap<byte[], Set<byte[]>> getKeyByteSetsFrom(String byteSetColumn);
	
	
	
	/**
	 * returns a map of separate Strings, made of the Strings of two string columns
	 * @param column1 the first column to be read, use row key if null
	 * @param column2 the second column to be read
	 * @return a map of separate Strings, made of the Strings of two string columns or null, if the cassandraResultSet was empty in the first place
	 */
	public abstract HashMap<String,String> getStringStringsFor(String column1, String column2);
	
	
	
	/**
	 * returns a map <String, set of byte arrays> of the given columns
	 * @param column1 the first column to be read
	 * @param column2 the second column to be read
	 * @return a map <String, set of byte arrays> of the given columns or null, if the cassandraResultSet was empty in the first place
	 */
	public abstract HashMap<String, Set<ByteBuffer>> getStringByteBuffersFor(String stringColumn, String byteBufferColumn);

	
	
	/**
	 * tells is a resultset is empty
	 * @return true, if there are 0 results in the resultset
	 */
	public abstract boolean isEmpty();
	
	
	
	/**
	 * prints a resultset to the Console
	 * @param rowsToPrint
	 */
	public abstract void print(int rowsToPrint);
	
	
	
	/**
	 * Gets the size of the Result in rows
	 * @return the number of rows contained in this result
	 */
	public abstract long getSize();

}
