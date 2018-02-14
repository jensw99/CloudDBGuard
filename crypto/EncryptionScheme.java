package crypto;

import interfaces.SaveableInXMLElement;

import java.util.HashMap;
import java.util.LinkedHashMap;

import databases.DBClient;

/**
 * Abstract class, specifying the minimum required methods necessary for an encryption scheme
 * @author Tim Waage
 */
public abstract class EncryptionScheme implements SaveableInXMLElement {

	// identifier for the algorithm
	protected String name;
	
	// the keystore this scheme will use to retrieve keys
	protected KeyStoreManager ks;
	
	// the database the encryption scheme will be outputting to or searching in
	protected DBClient db;
	
		
	
	/**
	 * Constructor, this makes sure every encryption scheme is associated to a keystore and has a database connection
	 * @param _name the name of the scheme
	 * @param _ks the keystore this scheme is using
	 * @param _db the database          
	 */
	public EncryptionScheme(String _name, KeyStoreManager _ks, DBClient _db) {
		
		name = _name;
		ks = _ks;
		db = _db;
	}
	
	
		
	/**
	 * gets the identifier of the encryption scheme
	 * @return the identifier of the encryption schem
	 */
	public String getName() {
		return name;
	}

	

}
