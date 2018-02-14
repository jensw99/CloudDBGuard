package crypto;

import java.util.HashSet;
import java.util.Set;

import enums.ColumnType;

/**
 * Class for storing the results of a SE search. SE search results are always sets of identifiers. These identifiers
 * are always rowkeys. Rowkeys can be of type String, Int or Byte.
 * 
 * @author Tim Waage
 */
public class SE_RowIdentifierSet {
	
	// row key type
	private ColumnType type;
	
	// hashsets for storing sets of row keys
	private Set<String> stringSet = new HashSet<String>();;
	private Set<Long> intSet = new HashSet<Long>();
	private Set<byte[]> byteSet = new HashSet<byte[]>();
	
	
	
	/**
	 * Constructor
	 * @param _type the row key type
	 */
	public SE_RowIdentifierSet(ColumnType _type) {
		
		type = _type;
	}
	
	
	
	/**
	 * gets the type of the row key set
	 * @return the type of the row key set
	 */
	public ColumnType getType() {
		
		return type;
	}
	
	
	/**
	 * sets the type of the row key set (apparently only needed in Papamanthou)
	 * @param type
	 */
	public void setType(ColumnType type){
		this.type = type;
	}
	
	
	/**
	 * gets the row key set in case it is of type string
	 * @return the row key set
	 */
	public Set<String> getStringSet() {
		
		return stringSet;
	}
	
	
	
	/**
	 * gets the row key set in case it is of type long
	 * @return the row key set
	 */
	public Set<Long> getIntSet() {
		
		return intSet;
	}

	
	
	/**
	 * gets the row key set in case it is of type byte array
	 * @return the row key set
	 */
	public Set<byte[]> getByteSet() {
		
		return byteSet;
	}
	
	
	
	/**
	 * sets the row key set
	 * @param _stringSet the row key set to set for this instance
	 */
	public void setStringSet(Set<String> _stringSet) {
		
		stringSet = _stringSet;
	}
	
	
	
	/**
	 * sets the row key set
	 * @param _intSet the row key set to set for this instance
	 */
	public void setIntSet(Set<Long> _intSet) {
		
		intSet = _intSet;
	}

	
	
	/**
	 * sets the row key set
	 * @param _byteSet the row key set to set for this instance
	 */
	public void setByteSet(Set<byte[]> _byteSet) {
	
		byteSet = _byteSet;
	}
	
	
	
	/**
	 * returns the numer of elements in this row key set
	 * @return the numer of elements in this row key set
	 */
	public int getSize() {
		
		if((type == ColumnType.STRING)&&(stringSet != null)) return stringSet.size();
		if((type == ColumnType.INTEGER)&&(intSet != null)) return intSet.size();
		if((type == ColumnType.BYTE)&&(byteSet != null)) return byteSet.size();
		
		return -1;
	}
}
