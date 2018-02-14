package enums;

/**
 * Enum Type representing different schemes for searchable encryption
 * 
 * @author Tim Waage
 */
public enum SESchemeType {

	SWP(1),		// Song
    SWP2(2),	// Song with fixed parameters for m and n
    SUISE(3); 	// Hahn & Kerschbaum
	
	// the value representing the type constant
    private final int value;

    
    
    /**
     * Constructor
     * @param value the value representing the type constant
     */
    private SESchemeType(int value) {
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
