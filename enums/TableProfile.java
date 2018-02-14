package enums;

/**
 * class representing different data types of columns
 * 
 * @author Tim Waage
 */
public enum TableProfile {

	FAST(1),				// use fast encryption schemes
    ALLROUND(2),			// use good allrounder
    STORAGEEFFICIENT(3),	// use storage-efficient encryption schemes
    OPTIMIZED_READING(4),
    OPTIMIZED_WRITING(5);
	
	// the value representing the type constant
    private final int value;

    
    
    /**
     * Constructor
     * @param value the value representing the type constant
     */
    private TableProfile(int value) {
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
     * Converts Strings to ColumnProfiles
     * @param s a String describing a ColumnProfile
     * @return the ColumnProfile that was represented by the String
     */
    public static TableProfile StringToColumnProfile(String s) {
    	if(s.equals("FAST")) return TableProfile.FAST;
    	else if(s.equals("STORAGEEFFICIENT")) return TableProfile.STORAGEEFFICIENT;
    	else return TableProfile.ALLROUND;
    }
	
}
