package crypto;

import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

	

public class OPE_Boldyreva_AES {
	public static final int blocksize = 16;
	
	private byte[] key;
	
	public OPE_Boldyreva_AES(byte[] keyarg) {
		if(!(keyarg.length == 16)&&!(keyarg.length == 24)&&!(keyarg.length == 32)) 
			System.out.println("Unzulässige AES Key Länge!");
		
		key = keyarg;
	}
	
	public byte[] encrypt (byte[] input) {
		
		SecretKeySpec secretKeySpec = new SecretKeySpec(key, "AES"); 
		byte[] encrypted;
		
		try {
			// Verschluesseln
			Cipher cipher = Cipher.getInstance("AES");
			cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec);
			encrypted = cipher.doFinal(input);
			 
			// bytes zu Base64-String konvertieren (dient der Lesbarkeit)		
		}
		catch(Exception e) {
			e.printStackTrace();
			return null;
		}		
		return encrypted;
	}
	
	public byte[] decrypt (byte[] input) {
		
		SecretKeySpec secretKeySpec = new SecretKeySpec(key, "AES"); 
		
		byte[] crypted = Base64.getDecoder().decode(input);
		
		byte[] decrypted;
		try {
			// Verschluesseln
			Cipher cipher = Cipher.getInstance("AES");
			cipher.init(Cipher.DECRYPT_MODE, secretKeySpec);
			decrypted = cipher.doFinal(crypted);
			 
		}
		catch(Exception e) {
			e.printStackTrace();
			return null;
		}		
		return decrypted;
	}
	
}
