package databases;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Callable;

import misc.Misc;

/**
 * Class for decrypting an Integer column
 *
 * @author Tim  Waage
 */
public class IntegerColumnDecrypter implements Callable<HashMap<byte[], Long>> {

	// the encrypted values as read from the database
	HashMap<byte[], byte[]> encryptedValues = null;
	
	// the rowkeys of the rows to encrypt
	Set<byte[]> rowkeysForDecryption = null;
	
	// the necessary IVs
	HashMap<byte[], byte[]> IVs = null;
	
	// the column's state
	ColumnState column = null;
	
	
	/**
	 * Constructor
	 * @param _encryptedValues the encrypted values as read from the database
	 * @param _rowkeysForDecryption the rowkeys of the rows to encrypt
	 * @param _IVs the necessary IVs
	 * @param _column the column's state
	 */
	public IntegerColumnDecrypter (HashMap<byte[], byte[]> _encryptedValues, Set<byte[]> _rowkeysForDecryption, HashMap<byte[], byte[]> _IVs, ColumnState _column) {
		
		encryptedValues = _encryptedValues;
		rowkeysForDecryption = _rowkeysForDecryption;
		column = _column;
		IVs = _IVs;
		
	}

	
	
	/**
	 * Returns the column's plaintext name
	 * @return the column's plaintext name
	 */
	public String getPlainColumnName() {
		
		return column.getPlainName();
	}
	
	
	
	@Override
	/**
	 * Encrypts the column
	 */
	public HashMap<byte[], Long> call() throws Exception {
		
		// create new HashMap for decrypted Values
		HashMap<byte[], Long> decryptedValues = new HashMap<byte[], Long>();
	
		if(column.isRNDoverDETStrippedOff()) // no RND layer over DET layer
			for(byte[] key : rowkeysForDecryption) 
				decryptedValues.put(key, Misc.bytesToLong(column.getDETScheme().decrypt(encryptedValues.get(key))));
		else { //RND layer still existing
			for(byte[] key : rowkeysForDecryption) 								
				decryptedValues.put(
						key, 
						Misc.bytesToLong(
								column.getDETScheme().decrypt( 
										column.getRNDScheme().decrypt(encryptedValues.get(key), IVs.get(key))
								)
						)
				);
			
		}
		
		return decryptedValues;
				
	};

	
}
