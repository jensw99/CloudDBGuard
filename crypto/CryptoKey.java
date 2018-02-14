package crypto;

import javax.crypto.SecretKey;

/**
 * Class representing a cryptographic key. Needed for using J(CE)KS keystore files.
 * @author Tim Waage
 *
 */
public class CryptoKey implements SecretKey {

	private static final long serialVersionUID = 1L;
	
	// the key itself
	private byte[] key;
	
	
	
	/**
	 * Constructor
	 * @param _key the cryptographic key represented by this instance
	 */
	public CryptoKey(byte[] _key) {
		
		key = _key;
	}
	
	
	
	@Override
	public String getAlgorithm() {
		
		return null;
	}

	
	
	@Override
	public byte[] getEncoded() {
		
		return key;
	}

	
	
	@Override
	public String getFormat() {
		
		return null;
	}

}
