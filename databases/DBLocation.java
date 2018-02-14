package databases;

import java.util.ArrayList;

/**
 * class for describing locations in a database
 * @author Tim Waage
 *
 */
public class DBLocation {
	
	// the locations keyspace
	private KeyspaceState keyspace;
	
	// the locations table
	private TableState table = null;
	
	// specifies filters to select rcertain rows
	private ArrayList<RowCondition> rowConditions;
	
	// specifies Filters to select certain columns
	private ArrayList<String> columns;
	
	
	
	/**
	 * Constructor
	 * @param _keyspace the locations keyspace
	 * @param _table the locations table
	 * @param _rows condition to select certain rows of...
	 * @param _columns ...certain columns
	 */
	public DBLocation(KeyspaceState _keyspace, TableState _table, ArrayList<RowCondition> _rows, ArrayList<String> _columns) {
		keyspace = _keyspace;
		if(_table != null) table = _table;
		rowConditions = _rows;
		columns = _columns;
	}
	
	
	
	/**
	 * gets the ids keyspace
	 * @return the ids keyspace
	 */
	public KeyspaceState getKeyspace() {
		return keyspace;
	}
	
	
	
	/**
	 * get the ids table name
	 * @return the ids table name
	 */
	public TableState getTable() {
		return table;
	}

	
	
	/**
	 * returns the ciphertext name of the table of this DBLocation
	 * @return the ciphertext name of the table of this DBLocation
	 */
	public String getCipherTableName() {
		
		return table.getCipherName();
	}
	
	
	
	/**
	 * gets the condition for specific rows of the table
	 * @return the condition for specific rows of the table
	 */
	public ArrayList<RowCondition> getRowConditions() {
		return rowConditions;
	}
	
	
	
	/**
	 * gets the ids column(s)
	 * @return the ids column(s)
	 */
	public ArrayList<String> getColumns() {
		
		return columns;
	}
	
	
	
	/**
	 * Adds a row condition to this DBLocation
	 * @param r the new RowCondition object
	 */
	public void addRowCondition(RowCondition r) {
		
		rowConditions.add(r);
	}
	
	
	
	/**
	 * Add a column to this DBLocation
	 * @param _column the name of the column to be added
	 */
	public void addColumn(String _column) {
		
		columns.add(_column);
	}
	
	
	
	/**
	 * get a string representation of the id object, that can be used in file system names
	 * @return a string representation of the id object, that can be used in file system names
	 */
	public String getIdAsPath() {
		
		return keyspace.getPlainName() + "_" + table.getPlainName() + "_" + columns.get(0);
		
	}
	
}
