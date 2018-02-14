package crypto;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.bouncycastle.crypto.macs.SipHash;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import misc.Misc;

/**
 * Implements the pseudorandom functions, in Song00 and SUISE referred to as F
 * @author Tim Waage
 */
public class PRF {
	
	// initialization vectors	
	private IvParameterSpec iv16;
	private IvParameterSpec iv8;
	
	private static Mac md5mac;
	private static Mac sha1mac;
	
	
	/**
	 * Constructor
	 */
	public PRF() {
		
		iv16 = new IvParameterSpec(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
		iv8 = new IvParameterSpec(new byte[]{0, 0, 0, 0, 0, 0, 0, 0});
		
		try {
			md5mac = Mac.getInstance("HmacMD5");
			sha1mac = Mac.getInstance("HmacSHA1");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Constructor with given IV
	 * @param iv
	 */
	public PRF(byte[] iv) {
		
		if(iv.length == 16) iv16 = new IvParameterSpec(iv);		
		else iv16 = new IvParameterSpec(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
		
		iv8 = new IvParameterSpec(new byte[]{0, 0, 0, 0, 0, 0, 0, 0});
	}
	
	
	
	/**
	 * encrypts a byte array, plain-text and cipher-text will have the same length
	 * but only the first outputLength bytes are returned
	 * (e.g. used to compute F_ki(s_i) in SWP scheme)
	 * @param input the input as byte[] (most likely the plaintext)
	 * @param encryptionKey the encryption key
	 * @return ciphertext (e.g. F_ki(S_i) in the SWP scheme)
	 */
	public byte[] encrypt_AES_CFB (byte[] input, byte[] encryptionKey, int outputLength) {
		
		//using the last bytes of AES
		try {
			Cipher cipher = Cipher.getInstance("AES/CFB/NoPadding" , "BC");
			SecretKeySpec key = new SecretKeySpec(encryptionKey, "AES");
			cipher.init(Cipher.ENCRYPT_MODE, key, iv16);
		
			byte[] enc = cipher.doFinal(input);
						
			//if outputLength is already correct
			if(enc.length == outputLength) return enc;
			//shorten 
			else{ 
				byte[] result = new byte[outputLength];
				System.arraycopy(enc, 0, result, 0, outputLength);
				return result;
			}
			
		}
		catch(Exception e) {
			e.printStackTrace();
			return null;
		}		
	}
		
	
	
	/**
	 * Encrypts a byte array using AES
	 * @param input the plaintext input
	 * @param encryptionKey the encryption key
	 * @return the ciphertext output
	 */
	public byte[] encryptByte_AES_CBC (byte[] input, byte[] encryptionKey) {
		
		SecretKeySpec skeySpec;
		
		//when encrypting documents with very long keywords in SUISE, the key for computing r_w_i can get very long
		//but AES allow 32 byte key length at max
		if(encryptionKey.length > 32) {
			byte[] tmp = new byte[32];
			System.arraycopy(encryptionKey, 0, tmp, 0, 32);
			skeySpec = new SecretKeySpec(tmp, "AES");
		}
		else skeySpec = new SecretKeySpec(encryptionKey, "AES");
		 		
		try {
			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING", "BC");
			cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv16);
		
			return cipher.doFinal(input);			
		}
		catch(Exception e) {
			e.printStackTrace();
			return null;
		}		
	}
	
	
	
	/**
	 * Encrypts a byte array using Blowfish
	 * @param input the plaintext input
	 * @param encryptionKey the encryption key
	 * @return the ciphertext output
	 */
	public byte[] encryptByte_Blowfish_CBC (byte[] input, byte[] encryptionKey) {
		
		SecretKeySpec skeySpec;
		
		if(encryptionKey.length > 56) {
			byte[] tmp = new byte[56];
			System.arraycopy(encryptionKey, 0, tmp, 0, 56);
			skeySpec = new SecretKeySpec(tmp, "Blowfish");
		}
		else skeySpec = new SecretKeySpec(encryptionKey, "Blowfish");
		 		
		try {
			Cipher cipher = Cipher.getInstance("Blowfish/CBC/PKCS5PADDING" , "BC");
			cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv8);
		
			return cipher.doFinal(input);			
		}
		catch(Exception e) {
			e.printStackTrace();
			return null;
		}		
	}
	
	
	
	/**
	 * Encrypts a string using AES
	 * @param input the plaintext input
	 * @param encryptionKey the encryption key
	 * @return the ciphertext output
	 */
	public String encryptString_AES_CBC (String input, byte[] encryptionKey) {
		
		byte[] encryptedBytes = encryptByte_AES_CBC(input.getBytes(), encryptionKey);
		
		return Base64.getEncoder().encodeToString(encryptedBytes);		
	}
	
	
	
	/**
	 * decrypts a byte array using AES
	 * @param input the ciphertext input
	 * @param encryptionKey the encryption key
	 * @return the plaintext output
	 */
	public byte[] decryptByte_AES_CBC (byte[] input, byte[] encryptionKey) {
				
		SecretKeySpec skeySpec;
		
		//when encrypting documents with very long keywords in SUISE, the key for computing r_w_i can get very long
		//but AES allow 32 byte key length at max
		if(encryptionKey.length > 32) {
			byte[] tmp = new byte[32];
			System.arraycopy(encryptionKey, 0, tmp, 0, 32);
			skeySpec = new SecretKeySpec(tmp, "AES");
		}
		else skeySpec = new SecretKeySpec(encryptionKey, "AES");
		 
		
		try {
			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING" , "BC");
			cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv16);
		
			return cipher.doFinal(input);			
		}
		catch(Exception e) {
			System.out.println("Error decrypting: " + Misc.ByteArrayToString(input));
			e.printStackTrace();
			return null;
		}		
	}
	
	
	
	/**
	 * decrypts a byte array using Blowfish
	 * @param input the ciphertext input
	 * @param encryptionKey the encryption key
	 * @return the plaintext output
	 */
	public byte[] decryptByte_Blowfish_CBC (byte[] input, byte[] encryptionKey) {
		
		SecretKeySpec skeySpec;
		
		//when encrypting documents with very long keywords in SUISE, the key for computing r_w_i can get very long
		//but AES allow 32 byte key length at max
		if(encryptionKey.length > 56) {
			byte[] tmp = new byte[56];
			System.arraycopy(encryptionKey, 0, tmp, 0, 56);
			skeySpec = new SecretKeySpec(tmp, "Blowfish");
		}
		else skeySpec = new SecretKeySpec(encryptionKey, "Blowfish");
		 
		
		try {
			Cipher cipher = Cipher.getInstance("Blowfish/CBC/PKCS5PADDING" , "BC");
			cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv8);
		
			return cipher.doFinal(input);			
		}
		catch(Exception e) {
			e.printStackTrace();
			return null;
		}		
	}
	
	
	
	/**
	 * decrypts a string using AES
	 * @param input the ciphertext input
	 * @param encryptionKey the encryption key
	 * @return the plaintext output
	 */
	public String decryptString_AES_CBC (String input, byte[] encryptionKey) {
		
		return new String(decryptByte_AES_CBC(Base64.getDecoder().decode(input), encryptionKey));
	}
	
	
	
	/**
	 * Computes HMAC SHA1 Signatures.
	 * @param input the input
	 * @param encryptionKey
	 * @return the SHA1 output
	 */
	public static byte[] compute_SHA1(byte[] input, byte[] encryptionKey) {

		byte[] result = null; 	
			
		try {		
			SecretKeySpec signingKey = new SecretKeySpec(encryptionKey, "HmacSHA1");			
			sha1mac.init(signingKey);			
			return sha1mac.doFinal(input);
				
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error computing the HMAC SHA-1");
		}
			
		return null;
	}
	
	
	
	/**
	 * Computes HMAC MD5 Signatures.
	 * @param input the input
	 * @param encryptionKey
	 * @return the MD5 output
	 */
	public static byte[] compute_MD5(byte[] input, byte[] encryptionKey) {
				
		try {		
			SecretKeySpec signingKey = new SecretKeySpec(encryptionKey, "HmacMD5");
			md5mac.init(signingKey);				
			return md5mac.doFinal(input);
					
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error computing the HMAC MD5");
		}
				
		return null;
	}
	
	
	/**
	 * Computes SipHash Signatures.
	 * @param input the input
	 * @param encryptionKey
	 * @return the SipHash output
	 */
	public static byte[] compute_SipHash(byte[] input, byte[] encryptionKey) {

		byte[] result = null; 	
				
		try {		
			
			SipHash mac = new SipHash();
			mac.init(new KeyParameter(encryptionKey));
			mac.update(input, 0, input.length - 1);
			result = Misc.longToBytes(mac.doFinal());
					
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error computing the HMAC MD5");
		}
				
		return result;
	}
}