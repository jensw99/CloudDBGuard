package databases;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Callable;

import misc.Misc;

/**
 * Class for decrypting an Integer set column
 *
 * @author Tim  Waage
 */
public class IntegerSetColumnDecrypter implements Callable<HashMap<byte[], Set<Long>>> {

	// the encrypted values as read from the database
	HashMap<byte[], Set<byte[]>> encryptedValues = null;
	
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
	public IntegerSetColumnDecrypter (HashMap<byte[], Set<byte[]>> _encryptedValues, Set<byte[]> _rowkeysForDecryption, HashMap<byte[], byte[]> _IVs, ColumnState _column) {
		
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
	public HashMap<byte[], Set<Long>> call() throws Exception {
		
		// create new HashMap for decrypted Values
		HashMap<byte[], Set<Long>> decryptedValues = new HashMap<byte[], Set<Long>>();
					
		for(byte[] key : rowkeysForDecryption) 
			decryptedValues.put(key, Misc.byteArraySetToLongSet(column.getDETScheme().decryptByteSet(encryptedValues.get(key))));	
	
		return decryptedValues;
				
	};

	
}
