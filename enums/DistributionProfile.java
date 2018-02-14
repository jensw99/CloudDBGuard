package enums;

/**
 * class representing the ways how data gets distributed across the available databases
 * 
 * @author Tim Waage
 */
public enum DistributionProfile {

	RANDOM(1),				// distribute columns randomly
    ROUNDROBIN(2),			// distribute columns round robin
    CUSTOM(3);				// customized distribution of columns across the available DBS 
	
	// the value representing the type constant
    private final int value;

    
    
    /**
     * Constructor
     * @param value the value representing the type constant
     */
    private DistributionProfile(int value) {
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
