package databases;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.bouncycastle.util.Arrays;

import misc.Misc;
import misc.Timer;
import crypto.DETScheme;
import crypto.RNDScheme;
import crypto.SE_RowIdentifierSet;
import enums.ColumnType;


/**
 * Class that provides query results in decrypted form
 *
 * @author Tim Waage
 */
public class DecryptedResults {
	
	// columns that appear in the final result (the "SELECT" columns from the original query)
	private HashSet<ColumnState> columns;
	
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
	
	// a set of all rowkeys of this resultset
	private Set<byte[]> rowkeysForDecryption = null;
	
	// tells how many resultsets were already added to contribute to this final decrypted resultset
	private int resultsetsAdded = 0;
	
	// Timer for measuring decryption steps
	private Timer decryptTimer = new Timer();
	
	// the time needed for decrypting all parts of the final result
	private long decryptionTime = 0;
	
		
	
	
	/**
	 * Constructor
	 * @param _columns columns that appear in the final result (the "SELECT" columns from the original query)
	 */
	public DecryptedResults(HashSet<ColumnState> _columns) {
		
		columns = _columns;
		
	}
	
	
	
	/**
	 * returns the time needed for decryption
	 * @return the time needed for decryption
	 */
	public long getDecryptionTime() {
		
		return decryptionTime;
	}
	
	
	
	/**
	 * Adds and decrypts a resultset from a single database query
	 * @param _result the result set to be added
	 */
	public void addResult(Result _result) {
		
		// get original request to find out what columns are available
		Request r = _result.getRequest();
		TableState table = r.getId().getTable();
		
		HashMap<byte[], byte[]> IVs = _result.getKeyBytesFrom(table.getIVcolumnName());
		
		// it the result is empty, abort
		if(IVs == null) return;
		if(IVs.isEmpty()) return;
		
		// if this is the first resultset we simply add all its rows for decryption
		if(resultsetsAdded == 0) rowkeysForDecryption = IVs.keySet(); 
		// otherwise we only focus on rows, that all resultssets have in common
		else {
			Set<byte[]> newRowkeysForDecryption = new HashSet<byte[]>();
			
			for(byte[] old : rowkeysForDecryption)
				for( byte[] neww: IVs.keySet())
					if(Arrays.areEqual(old,  neww)) newRowkeysForDecryption.add(neww);
			
			rowkeysForDecryption = newRowkeysForDecryption;
		}
		
		// filter out rows that do not pass SE, if necessary...
		ArrayList<RowCondition> SEConditions = new ArrayList<RowCondition>();
		for(RowCondition rc : r.getId().getRowConditions()) if(rc.getComparator().equals("#")) SEConditions.add(rc);
		
		for(RowCondition rc : SEConditions) {
			
			
			ColumnState column = r.getId().getTable().getColumnByPlainName(rc.getColumnName());
			
			ArrayList<String> tmpSEColumn = new ArrayList<String>();
			if(column.getSEScheme().getName().equals("SUISE")) 
				tmpSEColumn.add(column.getPlainName()); // Plain name for SUISE index
			else if(column.getSEScheme().getName().equals("SWP2")) 
				tmpSEColumn.add(column.getCSEname()); // SE name for direct reading in SWP
						
			SE_RowIdentifierSet seResults = column.getSEScheme().search(rc.getStringTerm(), new DBLocation(column.getTable().getKeyspace(), column.getTable(), null, tmpSEColumn));
			
			Set<byte[]> seResultsAsBytes = null;
			
			if(seResults.getType() == ColumnType.STRING) seResultsAsBytes = Misc.StringHashSet2ByteHashSet(seResults.getStringSet()); 
			if(seResults.getType() == ColumnType.INTEGER) seResultsAsBytes = Misc.LongHashSet2ByteHashSet(seResults.getIntSet());
			if(seResults.getType() == ColumnType.BYTE) seResultsAsBytes = seResults.getByteSet();
			
			Set<byte[]> rowkeysForDecryptionNew = new HashSet<byte[]>();
			
			//for(byte[] rowkey : rowkeysForDecryption) if(seResultsAsBytes.contains(rowkey)) rowkeysForDecryptionNew.add(rowkey);
			for(byte[] rowkey : rowkeysForDecryption)
				for(byte[] seResult : seResultsAsBytes)
					if(Arrays.areEqual(rowkey, seResult)) {
					rowkeysForDecryptionNew.add(rowkey);
					}
						
			rowkeysForDecryption = rowkeysForDecryptionNew;
		}
		
		//...then decrypt the rest
		
		// initialize thread pool
		ExecutorService executor = Executors.newCachedThreadPool();
					
		// future result lists
		Hashtable<String, Future<HashMap<byte[], String>>> futureStringColumnResults = new Hashtable<String, Future<HashMap<byte[], String>>>();
		Hashtable<String, Future<HashMap<byte[], Set<String>>>> futureStringSetColumnResults = new Hashtable<String, Future<HashMap<byte[], Set<String>>>>();
		Hashtable<String, Future<HashMap<byte[], Long>>> futureIntegerColumnResults = new Hashtable<String, Future<HashMap<byte[], Long>>>();
		Hashtable<String, Future<HashMap<byte[], Set<Long>>>> futureIntegerSetColumnResults = new Hashtable<String, Future<HashMap<byte[], Set<Long>>>>();
		Hashtable<String, Future<HashMap<byte[], byte[]>>> futureByteColumnResults = new Hashtable<String, Future<HashMap<byte[], byte[]>>>();
		Hashtable<String, Future<HashMap<byte[], Set<byte[]>>>> futureByteSetColumnResults = new Hashtable<String, Future<HashMap<byte[], Set<byte[]>>>>();				
		
		decryptTimer.start();
		
		// for every column: 
		for(String columnName : r.getId().getColumns()) {
			
			// find the ColumnState to know about column name and type
			ColumnState column = table.getColumnByPlainName(columnName);
			if(column == null) column = table.getColumnByCipherName(columnName);
			
			// decrypt only selected columns
			boolean isSelectColumn = false;
			for(ColumnState cs : columns) {
				if(columnName.equals(cs.getPlainName())) isSelectColumn = true;
				if(columnName.equals(cs.getCDETname())) isSelectColumn = true;
				if(columnName.equals(cs.getCRNDname())) isSelectColumn = true;
			}
			
			
			if((column != null)&&(isSelectColumn)) { // column could still be null if IV column or rowkey column in HBase
			
				// add HashMap<rowkey, value> to typ-map, e.g.: Stringmaps<columnName Map<rowkey, decrypted value>>
				
				// column was encrypted
				if(column.isEncrypted()) {
				
					// STRING COLUMN: use DET column for encryption in string columns
					if(column.getType() == ColumnType.STRING) {
						
						StringColumnDecrypter scd = new StringColumnDecrypter(_result.getKeyBytesFrom(column.getCDETname()), rowkeysForDecryption, IVs, column);
						futureStringColumnResults.put(column.getPlainName(), executor.submit(scd));				
					}
					
					// STRING_SET COLUMN: use RND column for encryption in string_set columns
					if(column.getType() == ColumnType.STRING_SET) {
						
						StringSetColumnDecrypter sscd = new StringSetColumnDecrypter(_result.getKeyByteSetsFrom(column.getCRNDname()), rowkeysForDecryption, IVs, column);
						futureStringSetColumnResults.put(column.getPlainName(), executor.submit(sscd));			
					}
					
					// INTEGER COLUMN: use DET column for encryption in int columns
					if(column.getType() == ColumnType.INTEGER) {
						
						IntegerColumnDecrypter icd = new IntegerColumnDecrypter(_result.getKeyBytesFrom(column.getCDETname()), rowkeysForDecryption, IVs, column);					
						futureIntegerColumnResults.put(column.getPlainName(), executor.submit(icd));										
					}
					
					// INTEGER_SET COLUMN: use RND column for encryption in integer_set columns
					if(column.getType() == ColumnType.INTEGER_SET) {
						
						IntegerSetColumnDecrypter iscd = new IntegerSetColumnDecrypter(_result.getKeyByteSetsFrom(column.getCRNDname()), rowkeysForDecryption, IVs, column);						
						futureIntegerSetColumnResults.put(column.getPlainName(), executor.submit(iscd));										
					}
					
					// BYTE COLUMN: use DET column for encryption in int columns
					if(column.getType() == ColumnType.BYTE) {
						
						ByteColumnDecrypter bcd = new ByteColumnDecrypter(_result.getKeyBytesFrom(column.getCDETname()), rowkeysForDecryption, IVs, column);					
						futureByteColumnResults.put(column.getPlainName(), executor.submit(bcd));										
					}
					
					// BYTE_SET COLUMN: use RND column for encryption in byte_set columns
					if(column.getType() == ColumnType.BYTE_SET) {
						
						ByteSetColumnDecrypter bscd = new ByteSetColumnDecrypter(_result.getKeyByteSetsFrom(column.getCRNDname()), rowkeysForDecryption, IVs, column);					
						futureByteSetColumnResults.put(column.getPlainName(), executor.submit(bscd));			
					}
				}
				
				// column was not encrypted in the first place
				else {
					if(column.getType() == ColumnType.STRING) stringColumns.put(columnName, _result.getKeyStringsFrom(columnName));
					if(column.getType() == ColumnType.INTEGER) intColumns.put(columnName, _result.getKeyIntsFrom(columnName));
					if(column.getType() == ColumnType.BYTE) byteColumns.put(columnName, _result.getKeyBytesFrom(columnName));
					if(column.getType() == ColumnType.STRING_SET) stringSetColumns.put(columnName, _result.getKeyStringSetsFrom(columnName));
					if(column.getType() == ColumnType.INTEGER_SET) intSetColumns.put(columnName, _result.getKeyIntSetsFrom(columnName));
					if(column.getType() == ColumnType.BYTE_SET) byteSetColumns.put(columnName, _result.getKeyByteSetsFrom(columnName));
				}
			}
		}
		
		// add decrypted columns in the end
		try {
			for(String columnName : futureStringColumnResults.keySet()) stringColumns.put(columnName, futureStringColumnResults.get(columnName).get());
			for(String columnName : futureStringSetColumnResults.keySet()) stringSetColumns.put(columnName, futureStringSetColumnResults.get(columnName).get());
			for(String columnName : futureIntegerColumnResults.keySet()) intColumns.put(columnName, futureIntegerColumnResults.get(columnName).get());
			for(String columnName : futureIntegerSetColumnResults.keySet()) intSetColumns.put(columnName, futureIntegerSetColumnResults.get(columnName).get());
			for(String columnName : futureByteColumnResults.keySet()) byteColumns.put(columnName, futureByteColumnResults.get(columnName).get());
			for(String columnName : futureByteSetColumnResults.keySet()) byteSetColumns.put(columnName, futureByteSetColumnResults.get(columnName).get());
					
		} catch (Exception e) {
								
		}
		
		decryptTimer.stop();
		decryptionTime += decryptTimer.getRuntime();
		
		resultsetsAdded++;
		
	}
	
	
	
	/**
	 * Prints the current content of the resultset
	 * @param rowsToPrint
	 */
	public void print(int rowsToPrint) {
		
		if(rowkeysForDecryption == null) {
			System.out.println("The result set of this query is empty, nothing to print here...");
			return;
		}
		
		int rowCounter = 0;
		
		// get all column names
		ArrayList<String> columnNames = new ArrayList<String>();
		
		columnNames.addAll(stringColumns.keySet());
		columnNames.addAll(intColumns.keySet());
		columnNames.addAll(byteColumns.keySet());
		columnNames.addAll(stringSetColumns.keySet());
		columnNames.addAll(intSetColumns.keySet());
		columnNames.addAll(byteSetColumns.keySet());
		
		for(int i=0; i<columnNames.size(); i++) System.out.print(Misc.makeLength(columnNames.get(i), 23));
		System.out.println();
		
		for(byte[] key : rowkeysForDecryption) {
			
			if(rowCounter < rowsToPrint) { 
				for(int i=0; i<columnNames.size(); i++) {
				
					if(stringColumns.keySet().contains(columnNames.get(i))) System.out.print(Misc.makeLength(getStringValue(key, columnNames.get(i)), 20) + " | ");
					if(intColumns.keySet().contains(columnNames.get(i))) System.out.print(Misc.makeLength(String.valueOf(getIntValue(key, columnNames.get(i))), 20) + " | ");
					if(byteColumns.keySet().contains(columnNames.get(i))) System.out.print(Misc.makeLength(Misc.ByteArrayToCharString(getByteValue(key, columnNames.get(i))), 20) + " | ");
					if(stringSetColumns.keySet().contains(columnNames.get(i))) System.out.print(Misc.makeLength("<" + getStringSetValue(key, columnNames.get(i)).size() + " element(s)>", 20) + " | ");
					if(intSetColumns.keySet().contains(columnNames.get(i))) System.out.print(Misc.makeLength("<" + getIntSetValue(key, columnNames.get(i)).size() + " element(s)>", 20) + " | ");
					if(byteSetColumns.keySet().contains(columnNames.get(i))) System.out.print(Misc.makeLength("<" + getByteSetValue(key, columnNames.get(i)).size() + " element(s)>", 20) + " | ");
								
				}
				System.out.println();
				rowCounter++;
			}
			
		}
		
		System.out.println("(printed " + rowCounter + " of " + rowkeysForDecryption.size() + " results)");
		
	}
	
	
	
	/**
	 * Returns the number of rows in this decrypted result set
	 * @return the number of rows in this decrypted result set
	 */
	public int getSize() {
		if(rowkeysForDecryption == null) return 0;
		else return rowkeysForDecryption.size();
	}
	
	
	
	/**
	 * Returns the String value of the resultset table at position <_key, column>
	 * @param _key the rowkey of the desired value
	 * @param column the column of the desired value
	 * @return the String value of the resultset table at position <_key, column>
	 */
	public String getStringValue(byte[] _key, String column) {
		
		String result = null;
		
		HashMap<byte[], String> columnData = stringColumns.get(column);
		for(byte[] key : columnData.keySet()) if(Arrays.areEqual(key, _key)) return columnData.get(key);
		
		return result;
	}
	
	
	
	/**
	 * Returns the numerical value of the resultset table at position <_key, column>
	 * @param _key the rowkey of the desired value
	 * @param column the column of the desired value
	 * @return the numerical value of the resultset table at position <_key, column>
	 */
	public Long getIntValue(byte[] _key, String column) {
		
		Long result = null;
		
		HashMap<byte[], Long> columnData = intColumns.get(column);
		for(byte[] key : columnData.keySet()) if(Arrays.areEqual(key, _key)) return columnData.get(key);
		
		return result;
	}
	
	
	
	/**
	 * Returns the byte array value of the resultset table at position <_key, column>
	 * @param _key the rowkey of the desired value
	 * @param column the column of the desired value
	 * @return the byte array value of the resultset table at position <_key, column>
	 */
	public byte[] getByteValue(byte[] _key, String column) {
		
		byte[] result = null;
		
		HashMap<byte[], byte[]> columnData = byteColumns.get(column);
		for(byte[] key : columnData.keySet()) if(Arrays.areEqual(key, _key)) return columnData.get(key);
		
		return result;
	}
	
	
	
	/**
	 * Returns the set of String values of the resultset table at position <_key, column>
	 * @param _key the rowkey of the desired value
	 * @param column the column of the desired value
	 * @return the set of String values of the resultset table at position <_key, column>
	 */
	public Set<String> getStringSetValue(byte[] _key, String column) {
		
		Set<String> result = null;
		
		HashMap<byte[], Set<String>> columnData = stringSetColumns.get(column);
		for(byte[] key : columnData.keySet()) if(Arrays.areEqual(key, _key)) return columnData.get(key);
		
		return result;
	}
	
	
	
	/**
	 * Returns the set of numerical values of the resultset table at position <_key, column>
	 * @param _key the rowkey of the desired value
	 * @param column the column of the desired value
	 * @return the set of numerical value of the resultset table at position <_key, column>
	 */
	public Set<Long> getIntSetValue(byte[] _key, String column) {
		
		Set<Long> result = null;
		
		HashMap<byte[], Set<Long>> columnData = intSetColumns.get(column);
		for(byte[] key : columnData.keySet()) if(Arrays.areEqual(key, _key)) return columnData.get(key);
		
		return result;
	}
	
	
	
	/**
	 * Returns the set of byte array values of the resultset table at position <_key, column>
	 * @param _key the rowkey of the desired value
	 * @param column the column of the desired value
	 * @return the set of byte array values of the resultset table at position <_key, column>
	 */
	public Set<byte[]> getByteSetValue(byte[] _key, String column) {
		
		Set<byte[]> result = null;
		
		HashMap<byte[], Set<byte[]>> columnData = byteSetColumns.get(column);
		for(byte[] key : columnData.keySet()) if(Arrays.areEqual(key, _key)) return columnData.get(key);
		
		return result;
	}

}
