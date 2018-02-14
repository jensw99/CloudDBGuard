package crypto;

import org.jdom2.Element;

import databases.DBClient;

public class RND_AES extends RNDScheme {

	// AES key (column key)
	private byte[] key; 
	
	
	/**
	 * Constructor
	 * @param _ks the JCEKS keystore this scheme is using
	 * @param _db the target database this scheme is operating on
	 * @param _key the cryptographic key to be used
	 */
	public RND_AES(KeyStoreManager _ks, DBClient _db, byte[] _key) {
		
		super("AES", _ks, _db);
		
		key = _key;	
		
	}
	
	
	
	/**
	 * Encrypts a byte array
	 * @param input the encryption input
	 * @param the initialization vector to be used
	 * @return the ciphertext
	 */
	public byte[] encrypt(byte[] input, byte[] _iv) {
		
		return new PRF(_iv).encryptByte_AES_CBC(input, key);
	}

	
	
	/**
	 * Decrypts a byte array
	 * @param input the decryption input
	 * @param the initialization vector to be used
	 * @return the plaintext
	 */
	public byte[] decrypt(byte[] input, byte[] _iv) {
		
		return new PRF(_iv).decryptByte_AES_CBC(input, key);
	}

	
	
	/**
	 * Encrypts a string
	 * @param input the encryption input
	 * @param the initialization vector to be used
	 * @return the ciphertext
	 */
	public String encrypt(String input, byte[] _iv) {
		
		return new PRF(_iv).encryptString_AES_CBC(input, key);
	}

	
	
	/**
	 * Decrypts a string
	 * @param input the decryption input
	 * @param the initialization vector to be used
	 * @return the plaintext
	 */
	public String decrypt(String input, byte[] _iv) {
		
		return new PRF(_iv).decryptString_AES_CBC(input, key);
	}


	
	@Override
	public Element getThisAsXMLElement() {
		
		Element schemeRoot = new Element("rnd");
		
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
