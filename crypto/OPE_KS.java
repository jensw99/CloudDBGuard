package crypto;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeMap;
import org.jdom2.Element;

import enums.ColumnType;
import databases.DBClient;
import databases.DBLocation;
import databases.Request;
import enums.RequestType;
import databases.RowCondition;
import misc.FileSystemHelper;
import misc.Misc;



/**
 * An Implamentation of the Kerschbaum OPE scheme
 * 
 * @author Tim Waage
 */
public class OPE_KS extends OPEScheme {
	
	// path to dictionaries
	private String meatadataPath = "/home/tim/TimDB/";
	
	// underlying dictionary
	private TreeMap<Long, Long> mainDict = null;	
	
	// arrays for reordering keys
	long[] sortedKeys = null;
	long[] optimalOrderedKeys = null;
	int pos;
	
	private long domainMinimum = 0;
	private long domainMaximum;
	
	// adjust target range here
	private long rangeMinimum = 0;
	private long rangeMaximum;

	// (only needed when returning the scheme as HashMap) 
	private int pbits;
	private int cbits;
	
	// identifier of the currently open dictionary
	String dictID = "";
	
	// temp for debugging
	private int updateCounter = 0;
	
	
	
	/**
	 * Constructor
	 * @param _ks the JCEKS keystore this scheme is using
	 * @param _db the target database this scheme is operating on
	 * @param _pbits length of the plaintexts in bit
	 * @param _cbits length of the ciphertexts in bit
	 */
	public OPE_KS(KeyStoreManager _ks, DBClient _db, int _pbits, int _cbits) {
		
		super("KS", _ks, _db);
		
		pbits = _pbits;
		cbits = _cbits;
		
		domainMaximum = (long)Math.pow(2, _pbits);
		rangeMaximum = (long)Math.pow(2, _cbits);
					
	}

	
	
	/**
	 * generates a new dictionary
	 * @param id the database location to which this scheme instance belongs
	 */
	private void loadDictionary(DBLocation id) {
		
		// check, if we have to load another dictionary
		if(!id.getIdAsPath().equals(dictID)) {
		
			// save the other dictionary
			if(!dictID.equals("")) close();
			
			// if no dictionary exists, create one and save it
			File file = new File(meatadataPath + " - " + id.getIdAsPath());
			// TODO !!! for benchmark purposes always create a new dict, comment that back in for non benchmark usage
			if(!file.exists()) { 
							
				mainDict = new TreeMap<Long, Long>();
		
				//TODO: consider this problem in diss/paper!
				mainDict.put(domainMinimum, rangeMinimum); // makes the domain minimal smaller than the range, so
				mainDict.put(domainMaximum, rangeMaximum); // min/max value don't get mapped to the same 'ciphertext'
			}
			else mainDict = FileSystemHelper.readTreeMapFromFile(meatadataPath + " - " + id.getIdAsPath());
			
			// set current dictID as the just opened id path
			dictID = id.getIdAsPath();
		}	
	}
	
	
	
	/**
	 * finds the optimal order for inserting the keys (which is a pre-order traversal of the tree)
	 * @param start interval start value
	 * @param end interval end value
	 */
	private void reorder(int start, int end) {
		
		pos++;
		
		if(start == end) optimalOrderedKeys[pos] = sortedKeys[start];
		else {
			
			int middle = start + (int)Math.floor((end - start)/2.0);
	
			optimalOrderedKeys[pos] = sortedKeys[middle];
		
			if(middle > start) reorder(start, middle - 1);
			if(middle < end) reorder(middle + 1, end);
		}
	}
	
	
	/**
	 * sorts the existing keys for an optimal re-insertion to the dictionary, uses the reorder
	 * method to a recursive pre-order traversal
	 * @param id the database location to which this scheme instance belongs
	 */
	private void update(DBLocation id) {

		updateCounter++;
		
		// System.out.println(mainDict.keySet().size());
		
		// put keys into integer arrays
		sortedKeys = new long[mainDict.keySet().size()];
		optimalOrderedKeys = new long[mainDict.keySet().size()];
		
		Iterator<Long> it = mainDict.keySet().iterator();
		for(int i=0; i<sortedKeys.length; i++) sortedKeys[i] = it.next(); 
		
		// perform pre-order traversal
		pos = -1;
		reorder(0, mainDict.keySet().size() -1);
		
		// create a new dictionary
		TreeMap<Long, Long> newDict = new TreeMap<Long, Long>();
		
		newDict.put(Long.MIN_VALUE+1, rangeMinimum);
		newDict.put(Long.MAX_VALUE-1, rangeMaximum);
		
		// put in the keys in optimal order
		for(int i=0; i<optimalOrderedKeys.length; i++) addValue(newDict, optimalOrderedKeys[i], id);
		
		// update the database
		
		// 1. Read the entire Column
		Request readRequest = new Request(RequestType.READ, new DBLocation(id.getKeyspace(), id.getTable(), null, id.getColumns()));
		HashMap<byte[], byte[]> old_raw = db.processRequest(readRequest).getKeyBytesFrom(id.getColumns().get(0)); // OPE column is a byte column
		
		// 1.5 recover the Long values
		HashMap<byte[], Long> old = new HashMap<byte[], Long>();
		for(byte[] key : old_raw.keySet()) old.put(key, Misc.bytesToLong(old_raw.get(key)));
		
		// 2 create a inverted lookup table to find 
		HashMap<Long, Long> revMainDict = buildReverseMainDict();
		
		for(byte[] rowkey : old.keySet()) {
			
			// 3. update the old value with newDict(k, value)
			ColumnType rowkeyColumnType = id.getTable().getRowkeyColumn().getType();
			
			ArrayList<RowCondition> tmpRC = new ArrayList<RowCondition>();
			if(rowkeyColumnType == ColumnType.STRING) tmpRC.add(new RowCondition(id.getTable().getRowkeyColumnName(), "=", Misc.ByteArrayToCharString(rowkey), 0, null, rowkeyColumnType)); 
			if(rowkeyColumnType == ColumnType.INTEGER) tmpRC.add(new RowCondition(id.getTable().getRowkeyColumnName(), "=", null, Misc.bytesToLong(rowkey), null, rowkeyColumnType)); 
			if(rowkeyColumnType == ColumnType.BYTE) tmpRC.add(new RowCondition(id.getTable().getRowkeyColumnName(), "=", null, 0, rowkey, rowkeyColumnType)); 
			
			Request updateRequest = new Request(RequestType.UPDATE_VALUE, new DBLocation(id.getKeyspace(), id.getTable(), tmpRC, null));
			updateRequest.getByteArgs().put(id.getColumns().get(0), Misc.longToBytes(newDict.get(revMainDict.get(old.get(rowkey)))));
					
			db.processRequest(updateRequest);
		}
		
		// make the new dictionary the main dictionary
		mainDict = newDict;
	}
	
	
	
	/**
	 * encrypts a value
	 * @param input plaintest input value
	 * @param id the database location to which this scheme instance belongs
	 * @return the ciphertext
	 */
	public long encrypt(long input, DBLocation id) {
		
		// load dictionary
		loadDictionary(id);
		
		// add Value
		addValue(mainDict, input, id);
				
		return mainDict.get(input);
	}
	
	
	
	/**
	 * decrypts a value
	 * @param input plaintest input value
	 * @param id the database location to which this scheme instance belongs
	 * @return the plaintext
	 */
	public long decrypt(long input, DBLocation id) {
		
		loadDictionary(id);
		System.out.println(mainDict.size());
		for(long k : mainDict.keySet()) if(mainDict.get(k) == input) return k;
		
		return -1;
	}
	
	
	
	/**
	 * builds a reverse index, needed for the update process
	 * @return the reverde index
	 */
	private HashMap<Long, Long> buildReverseMainDict() {
		
		HashMap<Long, Long> result = new HashMap<Long, Long>();
		
		for(long key : mainDict.keySet()) result.put(mainDict.get(key), key);
		
		return result;
	}
	
	
	
	/**
	 * adds an integer to a dictionary and computes the correct cipher value
	 * @param dict the current index
	 * @param x_i the value to be added
	 * @param database location on which this schme instance is operating on
	 */
	private void addValue(TreeMap<Long, Long> dict, long x_i, DBLocation id) {
		
		// value already exists in the dictionary, we're finished (2.)
		if(dict.containsKey(x_i)) {
			//System.out.println("schon drin");
			return;
		}
	
		// iterate through the dictionary to find the right position for the new key (1.)
		NavigableSet<Long> navKeySet = dict.navigableKeySet();
		
		long y_j_k = dict.get(navKeySet.lower(x_i));
		long y_j_k1 = dict.get(navKeySet.higher(x_i));
		
	    // compute and insert new key and cipher value (4. & 5.)
	    if(y_j_k + 1 < y_j_k1) {
	    	dict.put(x_i, y_j_k + (long)Math.ceil((y_j_k1 - y_j_k)/2.0));
	    	
	    }
	    else {	 
	    	// re-balance tree if necessary (3.) and add new key afterwards 
	    	update(id); 
	    	encrypt(x_i, id);
	    }		
	}
	
	
	
	/**
	 * prints out the main dictionary (for debug purposes)
	 */
	public void printDict() {
		for(long key : mainDict.keySet()) System.out.println(key + " - " + mainDict.get(key));
		System.out.println("size: " + mainDict.keySet().size());
	}
	
	

	@Override
	public void close() {
		
		if(mainDict != null) { // which means the scheme was actually used
			try {
				FileSystemHelper.writeTreeMapToFile(mainDict, meatadataPath + " - " + dictID);
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Error while saving " + meatadataPath + " - " + dictID);
			}
		}
	}
	
	
	
	@Override
	public String toString() {
		
		return "Kerschbaum & Schröpfer 2014";
	}
	
	
	
	@Override
	public Element getThisAsXMLElement() {
		
		Element schemeRoot = new Element("ope");
		
		Element schemeIdentifier = new Element("identifier");
		schemeIdentifier.addContent(name);
		schemeRoot.addContent(schemeIdentifier);
		
		Element schemePBits = new Element("pbits");
		schemePBits.addContent(String.valueOf(pbits));
		schemeRoot.addContent(schemePBits);
		
		Element schemeCBits = new Element("cbits");
		schemeCBits.addContent(String.valueOf(cbits));
		schemeRoot.addContent(schemeCBits);
		
		return schemeRoot;
	}



	@Override
	public void initializeFromXMLElement(Element data) {
		// TODO Auto-generated method stub
		
	}
}