package enums;

/**
 * Enum Type representing different Databases
 * 
 * @author Tim Waage
 */
public enum DatabaseType {

	CASSANDRA(1),	// Apache Cassandra
    HBASE(2);		// Apache HBase
	
	// the value representing the type constant
    private final int value;

    
    
    /**
     * Constructor
     * @param value the value representing the type constant
     */
    private DatabaseType(int value) {
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
