package crypto;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;

import misc.Misc;
import databases.DBClient;
import databases.DBLocation;

/**
 * Interface for OPE Encryption Schemes
 * @author Tim Waage
 *
 */
public abstract class OPEScheme extends EncryptionScheme {
	
	
	
	/**
	 * Constructor
	 * @param _name an identifying name for the scheme
	 * @param _ks the JCEKS keystore this scheme is using
	 * @param _db the target database this scheme is operating on
	 */
	public OPEScheme(String _name, KeyStoreManager _ks, DBClient _db) {
		
		super(_name, _ks, _db);
		
	}



	/**
	 * encrypts a value
	 * @param input in plaintext input
	 * @param id the path to the place within the database the encrypted value is suppoesed to be written to
	 * @return the ciphertext value
	 */
	public abstract long encrypt(long input, DBLocation id);
	
	
	
	/**
	 * Encrypts a set of values by encrypting every value separately
	 * @param input the input set
	 * @param id the destination within the database
	 * @return the encrypted input set
	 */
	public HashSet<Long> encryptSet(HashSet<Long> input, DBLocation id) {
		
		HashSet<Long> result = new HashSet<Long>();		
		for (Long l : input) result.add(encrypt(l, id));
		return result;
	};
	
	
	
	/**
	 * decryption
	 * @param keyword the word that is searched for
	 * @return
	 */
	public abstract long decrypt(long input, DBLocation id); 
	
	
	
	/**
	 * OPE encryption for strings
	 * @param s the input string
	 * @param id the path to the place within the database the encrypted value is suppoesed to be written to
	 * @return the ciphertext output
	 */
	public byte[] encryptString(String s, DBLocation id) {
		
		byte[] result = new byte[96];
		
		// split the string into chunks		
		byte[][] chunks = new byte[8][4];
		for(int i=0; i<s.length() && i<32; i++) chunks[i/4][i%4] = (byte)s.charAt(i);
				
		// OPE encrypt (only the necessary) chunks 
		int numberOfNecessaryChunks = (s.length()/4) + 1;
		if(numberOfNecessaryChunks > 8) numberOfNecessaryChunks = 8;
		
		for(int i=0; i<numberOfNecessaryChunks; i++) { 
			
			long crypto_in = chunkToLong(chunks[i]);			
			long crypto_out = encrypt(crypto_in, id);
			
			byte[] tmp = Misc.longToBytes(crypto_out);
						
			// and put them into the result array
			for(int j=0; j<8; j++) result[(i*8)+j] = tmp[j];
		}
		
		// pad if necessary
		if(s.length() <= 32) {
			byte[] additionalRandomBytes = new PRG().generateRandomBytes((8 - numberOfNecessaryChunks) * 8);
			for(int i=(numberOfNecessaryChunks * 8); i<64; i++) result[i] = additionalRandomBytes[i - (numberOfNecessaryChunks * 8)]; 
		}
			
		// append the SHA256 hash
		MessageDigest d;
		
		try {
			d = MessageDigest.getInstance("SHA-256");
			byte[] tmp;
			tmp = d.digest(s.getBytes());
			for(int i=0; i<32; i++) result[i+64] = tmp[i];
			
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	
	
	/**
	 * Helper function for encryptString
	 * @param chunk one of the 8 chunks
	 * @return the longvalue represented by that chunk
	 */
	private long chunkToLong(byte[] chunk) {
				
		long result = (long)((chunk[0] * Math.pow(128, 3)) + (chunk[1] * Math.pow(128, 2)) + (chunk[2] * 128) + chunk[3]);
		return result;
	}
	
	
	
	/**
	 * For tidying up after everything is done, e.g. save clientside data
	 */
	public abstract void close();

}
