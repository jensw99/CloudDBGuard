package databases;

import java.io.Serializable;

import org.apache.hadoop.hbase.util.Bytes;

import misc.Misc;
import enums.ColumnType;

/**
 * Class specifying filters for selecting rows
 * 
 * @author Tim Waage
 */
public class RowCondition implements Serializable {
	
	private static final long serialVersionUID = 9050143671889465346L;
	
	// the column restricting the row selection
	private String columnName;
	
	// the term expression can be of three types, only one is allowed per RowCondition
	private String stringTerm;
	private long longTerm;
	private byte[] byteTerm;
	
	// the comparator of the restricting expression
	private String comparator;
	
	// the term of the restricting expression
	private ColumnType type;
	
	
	
	/**
	 * Constructor
	 * @param _columnName the first argument, usually a column name, NULL if the primary key column should be used later
	 * @param _comparator the comparator
	 * @param _stringTerm the condition's term if the column is of type string
	 * @param _longTerm the condition's term if the column is of type long
	 * @param _byteTerm the condition's term if the column is of type byte
	 * @param _term the second argument, usually a value
	 */
	public RowCondition(String _columnName, String _comparator, String _stringTerm, long _longTerm, byte[] _byteTerm, ColumnType _type) {
		
		columnName = _columnName;
		stringTerm = _stringTerm;
		longTerm = _longTerm;
		byteTerm = _byteTerm;
		comparator = _comparator;
		type = _type;
	}
	
	
	
	/**
	 * returns the row condition as String for the use in CQL queries
	 */
	public String getConditionAsString() {
	
		if(this.type == ColumnType.STRING) return columnName + comparator + "'" + stringTerm + "'";
		else if(this.type == ColumnType.INTEGER)return columnName + comparator + longTerm;
		else return columnName + comparator + Misc.bytesToCQLHexString(byteTerm); // byte
	}
	
	// just to be safe
	public String toString() {
		return getConditionAsString();
	}
	
	
	
	/**
	 * gets the condition's column name
	 * @return the condition's column name
	 */
	public String getColumnName() {
		return columnName;
	}
	
	
	
	/**
	 * Sets the column name, can be necessary to specify the primary key here during request processing
	 * and for changing plain row conditions to cipher row conditions
	 * @param rowkey
	 */
	public void setColumnName(String _columnName) {
		columnName = _columnName;
	}
	
	
	
	/**
	 * gets the condition's string term
	 * @return the condition's string term
	 */
	public String getStringTerm() {
		return stringTerm;
	}
	
	
	
	/**
	 * Sets the string term, necessarry to replace a plain string term with its cipher string term
	 * @param _stringTerm the row condition's string term
	 */
	public void setStringTerm(String _stringTerm) {
		
		stringTerm = _stringTerm;
	}
	
	
	
	/**
	 * gets the condition's long term
	 * @return the condition's long term
	 */
	public long getLongTerm() {
		return longTerm;
	}
	
	
	
	/**
	 * Sets the long term, necessarry to replace a plain long term with its cipher long term
	 * @param _longTerm the row condition's long term
	 */
	public void setLongTerm(long _longTerm) {
		
		longTerm = _longTerm;
	}
	
	
	
	/**
	 * gets the condition's byte[] term
	 * @return the condition's byte[] term
	 */
	public byte[] getByteTerm() {
		return byteTerm;
	}
	
	
	
	/**
	 * Sets the byte term, necessarry to replace a plain byte term with its cipher byte term
	 * @param _byteTerm the row condition's byte term
	 */
	public void setByteTerm(byte[] _byteTerm) {
		
		byteTerm = _byteTerm;
	}
	
	
	
	/**
	 * returns the term as byte array, independent from the column's type
	 * @return the term as byte array, independent from the column's type
	 */
	public byte[] getTermAsByteArray() {
		
		if(this.type == ColumnType.STRING) return stringTerm.getBytes();
		if(this.type == ColumnType.INTEGER) return Bytes.toBytes(longTerm);
		return byteTerm;		
		
	}
	
	
	
	/**
	 * gets the condition's comparator
	 * @return the condition's comparator
	 */
	public String getComparator() {
		return comparator;
	}
	
	
	
	/**
	 * gets the condition's type
	 * @return the condition's type
	 */
	public ColumnType getType() {
		
		return type;
	}
	
	
	
	/**
	 * Sets row condition's type, can be necessarry for converting plain RC's to cipher RC's
	 * @param _type the new type
	 */
	public void setType(ColumnType _type) {
		
		type = _type;
	}
	
	
	
}
