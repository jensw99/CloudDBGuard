package crypto;

import databases.DBClient;



/**
 * This class defines the minimum requirements for an index of an encryption scheme (if it has one).
 * @author Tim Waage
 */
public class Index {
	
	// the database the index is stored in
	protected DBClient db;
	
	
	
	/**
	 * Constructor, makes sure the index has a database to read/write to
	 * @param _db the database, where the index is stored
	 */
	public Index(DBClient _db) {
		db = _db;
		
	}
}
