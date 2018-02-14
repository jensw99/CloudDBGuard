package crypto;

import java.util.HashSet;
import java.util.Set;

import databases.DBClient;
import databases.DBLocation;
import databases.Result;

/**
 * Interface for searchable Encryption Schemes
 * @author tim
 *
 */
public abstract class SEScheme extends EncryptionScheme {
	
	
	// counter for the words encrypted by this scheme
	protected long wordcount = 0;
	
	
	
	/**
	 * Constructor
	 * @param _name an identifying name for the scheme
	 * @param _ks the JCEKS keystore this scheme is using
	 * @param _db the target database this scheme is operating on
	 */
	public SEScheme(String _name, KeyStoreManager _ks, DBClient _db) {
		
		super(_name, _ks, _db);
		
	}

	
	
	/**
	 * Main method of the searchable encryption scheme for encrypting strings
	 * @param input a string to be encrypted
	 * @param id an ID object telling where exactly the new data is written to in the database, needed to build the index
	 * @return the String that is going to be written into the database
	 */
	public abstract String encrypt(String input, DBLocation id);
	
	
	
	/**
	 * Encrypts a set of values by encrypting every value separately
	 * @param input the input set
	 * @param id the destination within the database
	 * @return the encrypted input set
	 */
	public HashSet<String> encryptSet(HashSet<String> input, DBLocation id) {
		
		HashSet<String> result = new HashSet<String>();		
		for (String s : input) result.add(encrypt(s, id));
		return result;
	};
	
	
	
	/**
	 * Searchable encryption schemes have to provide a search method
	 * @param keyword the word that is searched for
	 * @param id an ID object telling where to look for the keyword	 
	 * @return the results corresponding to the keyword (a set of identifiers
	 */
	public abstract SE_RowIdentifierSet search (String keyword, DBLocation id); 
	
	
	
	/**
	 * tells the SE scheme that the connection will now be closed, allowing finalizing operations
	 */
	public abstract void close();


	/**
	 * gets called when the tablecolumn this scheme is associated with is deleted
	 */
	public abstract void delete(); 
	
	
	
	/**
	 * returns the number of words encrypted by this scheme
	 * @return the number of words encrypted by this scheme
	 */
	public long getWordcount() {
		
		return wordcount;
	}

}
