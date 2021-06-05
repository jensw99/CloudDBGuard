package crypto;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import databases.DBClient;
import databases.DBLocation;
import databases.Result;

/**
 * Interface for deterministic Encryption Schemes
 * @author Tim Waage
 */
public abstract class DETScheme extends EncryptionScheme {
	
	
	
	/**
	 * Constructor
	 * @param _name an identifying name for the scheme
	 * @param _ks the JCEKS keystore this scheme is using
	 * @param _db the target database this scheme is operating on
	 */
	public DETScheme(String _name, KeyStoreManager _ks, DBClient _db) {
		
		super(_name, _ks, _db);
		
	}

	

	/**
	 * Encryption for Byte Arrays
	 * @param input the encryption input
	 * @return the ciphertext
	 */
	public abstract byte[] encrypt (byte[] input); 
	
	
	
	/**
	 * Encrypts a set of values by encrypting every value separately
	 * @param input the input set
	 * @param id the destination within the database
	 * @return the encrypted input set
	 */
	public HashSet<byte[]> encryptByteSet(HashSet<byte[]> input) {
		
		HashSet<byte[]> result = new HashSet<byte[]>();		
		for (byte[] b : input) result.add(encrypt(b));

		return result;
	};
	
	
	/**
	 * Decrypts a set of values by decrypting every value separately
	 * @param input the input set
	 * @return the decrypted input set
	 */
	public HashSet<byte[]> decryptByteSet(Set<byte[]> input) {
		
		HashSet<byte[]> result = new HashSet<byte[]>();		
		for (byte[] b : input) result.add(encrypt(b));

		return result;
	}
	
	
	/**
	 * Decryption for Byte Arrays
	 * @param keyword encrypted data item
	 * @return the decrypted data item
	 */
	public abstract byte[] decrypt (byte[] input); 
	
	
	/**
	 * Encryption for Stings
	 * @param keyword plaintext 
	 * @return the ciphertext
	 */
	public abstract String encrypt (String input); 
	
	
	
	/**
	 * Encrypts a set of values by encrypting every value separately
	 * @param input the input set
	 * @param id the destination within the database
	 * @return the encrypted input set
	 */
	public HashSet<String> encryptStringSet(HashSet<String> input, DBLocation id) {
		
		HashSet<String> result = new HashSet<String>();		
		for (String s : input) result.add(encrypt(s));
		return result;
	};
	
	
	
	/**
	 * Decryption for Strings
	 * @param keyword encrypted data item
	 * @return the decrypted data item
	 */
	public abstract String decrypt (String input); 

}
