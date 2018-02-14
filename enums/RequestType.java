package enums;

/**
 * class representing different types of requests to the database
 * 
 * @author Tim Waage
 */
public enum RequestType {

	READ(1),			// read something
    INSERT(2),			// insert a row
    CREATE_TABLE(3),	// create a table
	UPDATE_SET(4), 		// adding a value to a set
	UPDATE_VALUE(5), 	// update a single value 
	DROP_TABLE(6),		// drop a table
	DROP_KEYSPACE(7),	// drop a keyspace
	CREATE_KEYSPACE(8), // create a keyspace
	READ_WITHOUT_IV(9), // read something without including the IV column (needed for indexes, that do not hav an IV column)
	DELETE(10),			// delete columns of rows
	READ_WITH_SET_CONDITION (11); // read like with the IN operator in CQL 
	
	

	// the value representing the type constant
    private final int value;

    
    
    /**
     * Constructor
     * @param value the value representing the type constant
     */
    private RequestType(int value) {
        this.value = value;
    }

    
    
    /**
     * gets the value representing the type constant
     * @return value the value representing the type constant
     */
    public int getValue() {
        return this.value;
    }
	
}
