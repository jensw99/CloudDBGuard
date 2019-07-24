package crypto;

import org.jdom2.Element;

import databases.DBClient;

/**
 * This class realizes AES as needed for the DET layer
 *
 * @author Tim Waage
 *
 */
public class DET_AES extends DETScheme {
	
	private byte[] key; // AES key
	
	private PRF prf; // underlying pseudo-random function
	
	
	
	/**
	 * Constructor
	 * @param _ks the JCEKS keystore this scheme is using
	 * @param _db the target database this scheme is operating on
	 * @param _iv the initialization vector to be used
	 * @param _key the cryptographic key to be used
	 */
	public DET_AES(KeyStoreManager _ks, DBClient _db, byte[] _iv, byte[] _key) {
		
		super("AES", _ks, _db);
		
		key = _key;	
		prf = new PRF(_iv);
	}
	
	
	
	/**
	 * Encrypts a byte array
	 * @param input the encryption input
	 * @return the ciphertext
	 */
	public byte[] encrypt(byte[] input) {
		
		return prf.encryptByte_AES_CBC(input, key);
	}

	
	
	/**
	 * Decrypts a byte array
	 * @param input the decryption input
	 * @return the plaintext
	 */
	public byte[] decrypt(byte[] input) {
		
		return prf.decryptByte_AES_CBC(input, key);
	}

	
	
	/**
	 * Encrypts a string
	 * @param input the encryption input
	 * @return the ciphertext
	 */
	public String encrypt(String input) {
		
		return prf.encryptString_AES_CBC(input, key);
	}

	
	
	/**
	 * Decrypts a string
	 * @param input the decryption input
	 * @return the plaintext
	 */
	public String decrypt(String input) {
		
		return prf.decryptString_AES_CBC(input, key);
	}


	
	@Override
	public Element getThisAsXMLElement() {
		
		Element schemeRoot = new Element("det");
		
		Element schemeIdentifier = new Element("identifier");
		schemeIdentifier.addContent(name);
		schemeRoot.addContent(schemeIdentifier);
		
		return schemeRoot;
	}


	
	@Override
	public void initializeFromXMLElement(Element data) {
		// TODO Auto-generated method stub
		
	}

}
