package databases;

import java.nio.ByteBuffer;
import java.util.HashSet;

import com.datastax.oss.driver.api.core.cql.BoundStatement;

import java.util.HashMap;

import misc.Misc;
import enums.RequestType;

/**
 * Class representing a request to the database
 * 
 * @author Tim Waage
 * 
 * USAGE:
 * 
 * type: CREATE_TABLE
 * id: location (keyspace, table, null, null)
 * args: for every column (column name, "null"|"set"|"list"|"primarykey") in the corresponding args variable
 *                                        0     1     2      3
 *                                       "xxx".getBytes
 * 										
 * type: INSERT
 * id: location (keyspace, table, null, null)
 * args: for every column (column name, value)
 * 
 * type: UPDATE_VALUE (new)
 * id: location (keyspace, table, row condition, null)
 * args: new value in xxxArgs<column name,value>, all other xxxArgs have to be null/empty
 * 
 * type: UPDATE_SET
 * id: location (keyspace, table, row condition, null)
 * args: one single xxxArgs (columnname, new value), all other xxxArgs = null!
 * 	
 * type: READ (keyspace, table, row condition, column)
 * id: location 
 * Select column(s) from keyspace.table where row condition is met
 * (use column names as they appear in the database, rowkey column is added automatically, don't put it in!)
 * 
 * type: READ_WITH_SET_CONDITION (keyspace, table, row condition, column)
 * id: location 
 * Select column(s) from keyspace.table where row condition is met,
 * Aims for queries involving the IN operator in CQL: SELECT * FROM ... WHERE x IN {a, b, c}
 * rowconditions have to be x=a, x=b, x=c
 * 
 * type: DROP_TABLE
 * id: location (keyspace, table, null, null)
 * Drops the table keyspace.table, if exists
 * 
 * type: DROP_KEYSPACE
 * id: location (keyspace, null, null, null)
 * Drops the keyspace, if exists
 * 
 * type: DELETE_ROW
 * id: location (keyspace, table, row condition, columns)
 * Deletes the specified columns where the row condition (id=value) is met
 *
 */
public class Request {
	
	// request type (e.g. read, write), see class RequestType
	private RequestType type;
	
	// location this request aims for
	private DBLocation id;
	
	// parameters (e.g. what to read, where to write to)
	private HashMap<String, String> stringArgs;
	private HashMap<String, Long> intArgs;
	private HashMap<String, byte[]> byteArgs;
	private HashMap<String, String> timestampStringArgs; // for timestamps passed as Strings, TODO: do this for Ints!
	
	
	// for inserting sets in one step
	private HashMap<String, HashSet<String>> stringSets;
	private HashMap<String, HashSet<Long>> intSets;
	private HashMap<String, HashSet<ByteBuffer>> byteSets;
	private HashMap<String, HashSet<String>> timestampStringSets;
	
	
	// Bound Statement for Cassandra Query Optimization
	private BoundStatement boundStatement = null;
	
	
	
	/**
	 * Constructor
	 * @param _type request type (e.g. read, write), see class RequestType
	 * @param _id location this request aims for
	 * @param _rowKey rowkey to identify which column is a rowkey (e.g. necessary in HBase inserts)
	 */
	public Request(RequestType _type, DBLocation _id) {
		
		type = _type;
		id = _id;
		stringArgs = new HashMap<String, String>();
		intArgs = new HashMap<String, Long>();
		byteArgs = new HashMap<String, byte[]>();
		timestampStringArgs = new HashMap<String, String>();
		
		stringSets = new HashMap<String, HashSet<String>>();
		intSets = new HashMap<String, HashSet<Long>>();
		byteSets = new HashMap<String, HashSet<ByteBuffer>>();
		timestampStringSets = new HashMap<String, HashSet<String>>();
	}
	
	
	
	/**
	 * gets the request type, see class RequestType
	 * @return the request type, see class RequestType
	 */
	public RequestType getType() {
		return type;
	}

	
	
	/**
	 * gets the location this request aims for
	 * @return the location this request aims for
	 */
	public DBLocation getId() {
		return id;
	}
	
	
	
	/**
	 * gets the string arguments of this request
	 * @return the string arguments of this request
	 */
	public HashMap<String, String> getStringArgs() {
		return stringArgs;
	}
	
	
	
	/**
	 * sets the string arguments of the request
	 * @param _stringArgs
	 */
	public void setStringArgs(HashMap<String, String> _stringArgs) {
		
		stringArgs = _stringArgs;
	}
	
	
	
	/**
	 * gets the int/Integer arguments of this request
	 * @return the int/Integer arguments of this request
	 */
	public HashMap<String, Long> getIntArgs() {
		return intArgs;
	}
	
	
	
	/**
	 * gets the byte arguments of this request
	 * @return the byte arguments of this request
	 */
	public HashMap<String, byte[]> getByteArgs() {
		return byteArgs;
	}
	
	
	
	/**
	 * gets the timestamp string arguments of this request
	 * @return the timestamp string arguments of this request
	 */
	public HashMap<String, String> getTimestampStringArgs() {
		return timestampStringArgs;
	}
	
	
	
	/**
	 * gets the string set arguments of this request
	 * @return the string set arguments of this request
	 */
	public HashMap<String, HashSet<String>> getStringSets() {
		return stringSets;
	}
	
	
	
	/**
	 * gets the int/Integer set arguments of this request
	 * @return the int/Integer set arguments of this request
	 */
	public HashMap<String, HashSet<Long>> getIntSets() {
		return intSets;
	}
	
	
	
	/**
	 * gets the byte set arguments of this request
	 * @return the byte set arguments of this request
	 */
	public HashMap<String, HashSet<ByteBuffer>> getByteSets() {
		return byteSets;
	}
	
	
	
	/**
	 * gets the timestamp string set arguments of this request
	 * @return the timestamp string set arguments of this request
	 */
	public HashMap<String, HashSet<String>> getTimestampStringSets() {
		return timestampStringSets;
	}
	
	
	/**
	 * sets a boud statement for cassandra query optimization
	 * @param _boundStatement
	 */
	public void setBoundStatement(BoundStatement _boundStatement) {
		
		boundStatement = _boundStatement;
	}
	
	public BoundStatement getBoundStatement() {
		
		return boundStatement;
	}
	
	
	
	/**
	 * Prints a string representation of the request to the console (mostly for debug purposes)
	 */
	public String toString() {
		
		String result = "Type: " + type + "\n";
		result += "ID: " + id + "\n";
		result += "key column: " + id.getTable() + "\n";
		result += "StringArgs:\n";
		for(String s : stringArgs.keySet()) result += s + " -> " + stringArgs.get(s) + "\n";
		result += "IntArgs:\n";
		for(String s : intArgs.keySet()) result += s + " -> " + intArgs.get(s) + "\n";
		result += "ByteArgs:\n";
		for(String s : byteArgs.keySet()) result += s + " -> " + Misc.ByteArrayToHexString(byteArgs.get(s)) + "\n";
		
		return result;
	}
	
}
