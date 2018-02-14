package enums;

/**
 * Enum Type representing different schemes for deterministic encryption
 * 
 * @author Tim Waage
 */
public enum DETSchemeType {

    AES(1); 	// AES CBC encryption
	
	// the value representing the type constant
    private final int value;

    
    
    /**
     * Constructor
     * @param value the value representing the type constant
     */
    private DETSchemeType(int value) {
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
