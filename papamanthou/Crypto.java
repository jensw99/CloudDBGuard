package papamanthou;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.Mac;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class Crypto {
	public static final int bitLength = 256;
	public static final String encryptionAlgo = "AES/CBC/PKCS5PADDING";
	public static final String generalEncryptionAlgo = "AES";
	public static final String hashAlgo = "SHA-256";
	public static final String hmacAlgo = "HmacSHA256";
	public static final int initVectorLength = 128 / Byte.SIZE;
	public static final int size = bitLength / Byte.SIZE;

	private Keys keys;
	private List<Cipher> prfCipher;
	private Cipher cipher;

	private Mac mac;
	private SecureRandom random;
	private MessageDigest digest;

	static {
		removeCryptographyRestrictions();
	}
	
	
	private static void removeCryptographyRestrictions() {
	    if (!isRestrictedCryptography()) {
//	        logger.fine("Cryptography restrictions removal not needed");
	        return;
	    }
	    try {
	        /*
	         * Do the following, but with reflection to bypass access checks:
	         *
	         * JceSecurity.isRestricted = false;
	         * JceSecurity.defaultPolicy.perms.clear();
	         * JceSecurity.defaultPolicy.add(CryptoAllPermission.INSTANCE);
	         */
	        final Class<?> jceSecurity = Class.forName("javax.crypto.JceSecurity");
	        final Class<?> cryptoPermissions = Class.forName("javax.crypto.CryptoPermissions");
	        final Class<?> cryptoAllPermission = Class.forName("javax.crypto.CryptoAllPermission");

	        final Field isRestrictedField = jceSecurity.getDeclaredField("isRestricted");
	        isRestrictedField.setAccessible(true);
	        final Field modifiersField = Field.class.getDeclaredField("modifiers");
	        modifiersField.setAccessible(true);
	        modifiersField.setInt(isRestrictedField, isRestrictedField.getModifiers() & ~Modifier.FINAL);
	        isRestrictedField.set(null, false);

	        final Field defaultPolicyField = jceSecurity.getDeclaredField("defaultPolicy");
	        defaultPolicyField.setAccessible(true);
	        final PermissionCollection defaultPolicy = (PermissionCollection) defaultPolicyField.get(null);

	        final Field perms = cryptoPermissions.getDeclaredField("perms");
	        perms.setAccessible(true);
	        ((Map<?, ?>) perms.get(defaultPolicy)).clear();

	        final Field instance = cryptoAllPermission.getDeclaredField("INSTANCE");
	        instance.setAccessible(true);
	        defaultPolicy.add((Permission) instance.get(null));

//	        logger.fine("Successfully removed cryptography restrictions");
	    } catch (final Exception e) {
//	        logger.log(Level.WARNING, "Failed to remove cryptography restrictions", e);
	    }
	}

	private static boolean isRestrictedCryptography() {
	    // This simply matches the Oracle JRE, but not OpenJDK.
	    return "Java(TM) SE Runtime Environment".equals(System.getProperty("java.runtime.name"));
	}

	public Crypto() {
		try {
			digest = MessageDigest.getInstance(hashAlgo);
		} catch (NoSuchAlgorithmException e) {
			System.out.println("Hash-Algorithm not available");
			e.printStackTrace();
		}
		try {
			mac = Mac.getInstance(hmacAlgo);
		} catch (NoSuchAlgorithmException e) {
			System.out.println("HMac-Algorithm not available");
			e.printStackTrace();
		}

		random = new SecureRandom();
		cipher = getCipher(new byte[size], new byte[initVectorLength], Cipher.DECRYPT_MODE);
	}

	private byte[] crypt(byte[] input, Cipher cipher) {
		try {
			return cipher.doFinal(input);
		} catch (IllegalBlockSizeException | BadPaddingException e) {
			System.out.println("Error while en- or decrypting");
			System.out.println("Input was: " + input.toString());
			System.out.println("Input length was: " + input.length);
			System.out.println("Init Vector was: " + cipher.getIV().toString());
			System.out.println("Init vector length was: " + cipher.getIV().length);
			e.printStackTrace();
			return null;
		}
	}

	public byte[] prf(byte[] input, int level) {
		return crypt(input, prfCipher.get(level));
	}

	public byte[] decrypt(byte[] input, byte[] iv) {
		cipher = getCipher(keys.getEsk(), iv, Cipher.DECRYPT_MODE, cipher);
		return crypt(input, cipher);
	}

	public byte[] encrypt(byte[] input) {
		keys.setEskIv(getRandomIV()); 
		cipher = getCipher(keys.getEsk(), keys.getEskIv(), Cipher.ENCRYPT_MODE, cipher);
		return crypt(input, cipher);
	}

	public byte[] getLastEskInitVector() {
		return keys.getEskIv();
	}

	public byte[] encrypt(String input) {
		return encrypt(input.getBytes(Utils.utf8));
	}

	public String decryptString(byte[] input, byte[] iv) {
		return new String(decrypt(input, iv), Utils.utf8);
	}

	public void newLevelKey(int level) {
		
		byte[] newkey = getRandomKey();
		Cipher newCipher = getCipher(newkey, keys.getInitVector(), Cipher.ENCRYPT_MODE);
		if (level < prfCipher.size()) {
			prfCipher.set(level, newCipher);
			keys.addKey(newkey, level);
		} else {
			prfCipher.add(newCipher);
			keys.addKey(newkey);
		}
		
	}

	public void setKeys(Keys keys) {
		this.keys = keys;
		prfCipher = new ArrayList<Cipher>();
		for (int i = 0; i < keys.numberOfKeys(); i++) {
			prfCipher.add(getCipher(keys.getKey(i), keys.getInitVector(), Cipher.ENCRYPT_MODE));
		}

	}

	private Cipher getCipher(byte[] key, byte[] iv, int mode) {
		Cipher cipher = null;
		try {
			cipher = Cipher.getInstance(encryptionAlgo);
		} catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
			System.out.println("Encryption Algorithm not available");
			e.printStackTrace();
		}
		return getCipher(key, iv, mode, cipher);

	}

	private Cipher getCipher(byte[] key, byte[] iv, int mode, Cipher cipher) {
		SecretKeySpec encKey = new SecretKeySpec(key, generalEncryptionAlgo);

		try {
			cipher.init(mode, encKey, new IvParameterSpec(iv));
		} catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
			System.out.println("Error while setting up en- or decryption");
			System.out.println("Length of iv: " + iv.length);
			e.printStackTrace();
		}
		return cipher;
	}

	public Keys getKeys() {
		return keys;
	}

	public void generateKeys(int noOfLevels) {
		byte[] esk = getRandomKey();
		byte[] iv = getRandomIV();
		keys = new Keys(esk, iv);
		for (int i = 0; i < noOfLevels; i++) {
			keys.addKey(getRandomKey());
		}
		setKeys(keys);
	}

	public byte[] getRandomIV() {
		byte[] result = new byte[initVectorLength];
		random.nextBytes(result);
		return result;
	}

	public byte[] getRandomKey() {
		byte[] result = new byte[size];
		random.nextBytes(result);
		return result;
	}

	public byte[] hash(String text) {
		return digest.digest(text.getBytes(Utils.utf8));
	}

	public byte[] keyedHash(String text, byte[] key) {
		SecretKeySpec macKey = new SecretKeySpec(key, hmacAlgo);
		try {
			mac.init(macKey);
		} catch (InvalidKeyException e) {
			System.out.println("Invalid key generated");
			e.printStackTrace();
		}
		return mac.doFinal(text.getBytes(Utils.utf8));
	}

}
