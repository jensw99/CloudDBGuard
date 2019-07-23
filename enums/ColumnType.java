package enums;

/**
 * class representing different data types of columns
 * 
 * @author Tim Waage
 */
public enum ColumnType {

	STRING(1),		// String
    INTEGER(2),		// Integer
    BYTE(3),		// Byte Blob
	TIMESTAMP(4),	// Timestamp
	STRING_SET(5),  // String set
	INTEGER_SET(6), // Integer set
	BYTE_SET(7);    // Byte set

	// the value representing the type constant
    private final int value;

    /**
     * Constructor
     * @param value the value representing the type constant
     */
    private ColumnType(int value) {
        this.value = value;
    }

    
    
    /**
     * gets the value representing the type constant
     * @return value the value representing the type constant
     */
    public int getValue() {
        return this.value;
    }
    
    
    
    /**
     * returns the corresponding SET type given the input type t 
     * @param t the "normal" ColumnType
     * @return the corresponding SET type given the input type t 
     */
    public ColumnType getSetType(ColumnType t) {
    	
    	if(t == ColumnType.STRING) return ColumnType.STRING_SET;
    	if(t == ColumnType.INTEGER) return ColumnType.INTEGER_SET;
    	if(t == ColumnType.BYTE) return ColumnType.BYTE_SET;
    	
    	return null;
    }
	
}
