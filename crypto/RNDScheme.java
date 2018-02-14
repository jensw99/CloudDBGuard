package crypto;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import databases.DBClient;
import databases.DBLocation;

/**
 * Interface for random Encryption Schemes
 * @author Tim Waage
 */
public abstract class RNDScheme extends EncryptionScheme {
	
	
	
	/**
	 * Constructor
	 * @param _name an identifying name for the scheme
	 * @param _ks the JCEKS keystore this scheme is using
	 * @param _db the target database this scheme is operating on
	 */
	public RNDScheme(String _name, KeyStoreManager _ks, DBClient _db) {
		
		super(_name, _ks, _db);
		
	}


	
	/**
	 * Encryption for Byte Arrays
	 * @param keyword plaintext 
	 * @return the ciphertext
	 */
	public abstract byte[] encrypt (byte[] input, byte[] iv); 
	
	
	
	/**
	 * Decryption for Byte Arrays
	 * @param keyword encrypted data item
	 * @return the decrypted data item
	 */
	public abstract byte[] decrypt (byte[] input, byte[] iv); 
	
	
	
	/**
	 * Encryption for Stings
	 * @param keyword plaintext 
	 * @return the ciphertext
	 */
	public abstract String encrypt (String input, byte[] iv); 
	
	
	
	/**
	 * Decryption for Strings
	 * @param keyword encrypted data item
	 * @return the decrypted data item
	 */
	public abstract String decrypt (String input, byte[] iv); 
	
	
	
	/**
	 * Encrypts a set of values by encrypting every value separately
	 * @param input the input set
	 * @param id the destination within the database
	 * @return the encrypted input set
	 */
	public HashSet<String> encryptStringSet(Set<String> input, DBLocation id, byte[] _iv) {
		
		HashSet<String> result = new HashSet<String>();		
		for (String s : input) result.add(encrypt(s, _iv));
		return result;
	};
	
	
	
	/**
	 * Encrypts a set of values by encrypting every value separately
	 * @param input the input set
	 * @param id the destination within the database
	 * @return the encrypted input set
	 */
	public HashSet<byte[]> encryptByteSet(HashSet<byte[]> input, byte[] _iv) {
		
		HashSet<byte[]> result = new HashSet<byte[]>();		
		for (byte[] b : input) {
						
			result.add(encrypt(b, _iv));
		}
		return result;
	};
	
	
	
	/**
	 * Decrypts a set of values by decrypting every value separately
	 * @param input the input set
	 * @param id the destination within the database
	 * @return the decrypted input set
	 */
	public HashSet<byte[]> decryptByteSet(Set<byte[]> input, byte[] _iv) {
		
		HashSet<byte[]> result = new HashSet<byte[]>();		
		for (byte[] b : input) {
			
			//result.add(ByteBuffer.wrap(decrypt(b, _iv)));
			result.add(decrypt(b, _iv));
		}
		return result;
	};

}
