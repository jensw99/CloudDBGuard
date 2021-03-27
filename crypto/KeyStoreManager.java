package crypto;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableEntryException;
import java.security.cert.CertificateException;
import java.util.Base64;
import java.util.HashMap;

import javax.crypto.SecretKey;

import misc.FileSystemHelper;
import misc.Misc;

/**
 * Class for managing multiple keys in a JCEKS keystore
 * @author tim
 *
 */
public final class KeyStoreManager {

	// path of the keystore
	private String path;
	
	// password to access the keystore
	private char[] password;
	
	// JCEKS's protection parameter
	KeyStore.ProtectionParameter protParam;
	
	// pseudorandom generator for requesting random passwords
	private PRG keyGen = new PRG();
	
	// the JCEKS keystore
	private KeyStore keychain = null;
	
	
	
	/**
	 * Constructor
	 * @param _path path of the keystore
	 * @param _password password to access the keystore
	 */
	public KeyStoreManager(String _path, String _password) {
		
		// Adding our classes to the JCEKS serial filter
		String serialFilter = "java.lang.Enum;java.security.KeyRep;java.security.KeyRep$Type;javax.crypto.spec.SecretKeySpec;crypto.**;!*";
		System.setProperty("jceks.key.serialFilter", serialFilter);
		
		path = _path;
		password = _password.toCharArray();
		protParam = new KeyStore.PasswordProtection(password);
		
		File file = new File(_path);
		
		try {
			keychain = KeyStore.getInstance("JCEKS");
		} catch (KeyStoreException e) {
			System.out.println("Unable to create JCEKS keystore!");
			e.printStackTrace();
		}
		
		
		// load keychain, if exists
		if(file.exists() && !file.isDirectory()) {
				
			FileInputStream fis = null;
			try {
	       
				fis = new FileInputStream(_path);			
				keychain.load(fis, password);
			} 
			catch(Exception e) {
				System.out.println("Unable to load keychain from " + _path);
			}
			finally {
				if (fis != null) {
					try {
						fis.close();
					} catch (IOException e) {
						System.out.println("Unable to close file input stream when initializing the keychain.");
						e.printStackTrace();
					}
				}
			}
		}
		
		// else
		else {

			
			try {
				keychain.load(null, _password.toCharArray());
			} catch (Exception e) {
				System.out.println("Unable to initialize empty keystore");
				e.printStackTrace();
			}

			saveKeystore();
		}
	}

	
	
	/**
	 * checks if there already is a certain key in the store
	 * @param k the key to check for
	 * @return whether the key already exists or not
	 */
	public boolean exists(String k) {
			
		try {
			return keychain.isKeyEntry(k);
		} catch (KeyStoreException e) {
			System.out.println("Unable to check for key " + k);
			e.printStackTrace();
		}
		
		return false;
	}
	
	
	
	/**
	 * returns the file path of this keystore
	 * @return the file path of this keystore
	 */
	public String getPath() {
		
		return path;
	}
	
	
	
	/**
	 * saves a key in the keystore using a certain label for it, then saves the keystore back to disk
	 * @param label the key's label/alias
	 * @param key the key to be stored
	 */
	private void saveKey(String label, byte[] key) {
		
		KeyStore.SecretKeyEntry skEntry = new KeyStore.SecretKeyEntry(new CryptoKey(key));
	    try {
			keychain.setEntry(label, skEntry, protParam);
		} catch (KeyStoreException e) {
			System.out.println("Unable to set secret key entry for key " + label);
			e.printStackTrace();
		}
		
	    saveKeystore();
	}
	
	
	
	/**
	 * writes the entire keystore to a file, specified in the path variable
	 */
	public void saveKeystore() {
		
		FileOutputStream fos = null;
	    try {
	        fos = new FileOutputStream(path);
	        keychain.store(fos, password);
		} catch (Exception e) {
			System.out.println("Unable to save keychain to " + path);
			e.printStackTrace();
		} finally {
	        if (fos != null) {
	            try {
					fos.close();
				} catch (IOException e) {
					System.out.println("Unable to close file output stream when saving the keychain");
					e.printStackTrace();
				}
	        }
	    }
	}
	
	
	
	/**
	 * returns a specifically labeled key from the store, create one if not exists yet
	 * @param label label/alias of the requested key
	 * @param length expected key length
	 * @return the requested key
	 */
	public byte[] getKeyFor(String label, int length) {
		
		// if key already exists
		if(exists(label)) {
			// if it has the expected length return it
			KeyStore.SecretKeyEntry keyEntry;
			
			try {
				keyEntry = (KeyStore.SecretKeyEntry)keychain.getEntry(label, protParam);
				byte[] key = keyEntry.getSecretKey().getEncoded();
				if(key.length == length) return key;
				// if not, return nothing
				else {
					System.out.println("Key for \"" + label + "\" does not have the expected length of " + length + " bytes!");
					return null;
				}
			} catch (Exception e) {
				System.out.println("Unable to get a key labeled \"" + label + "\"");
				e.printStackTrace();
				return null;
			}
			
			
			
		}
		// if not
		else {
			// create key
			byte[] result = keyGen.generateRandomBytes(length);
			// save it
			saveKey(label, result);
			
			// return it
			return result;
		}
	}
		
}