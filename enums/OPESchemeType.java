package enums;

/**
 * Enum Type representing different schemes for order preserving encryption
 * 
 * @author Tim Waage
 */
public enum OPESchemeType {

	MOPE(1),	// Boldyreva 2011
    KS(2),		// Kerschbaum & Schröpfer 2014
    RSS(3); 	// Wozniak 2013
	
	// the value representing the type constant
    private final int value;

    
    
    /**
     * Constructor
     * @param value the value representing the type constant
     */
    private OPESchemeType(int value) {
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
