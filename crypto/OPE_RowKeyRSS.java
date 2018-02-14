package crypto;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.jdom2.Element;

import databases.DBClient;
import databases.DBLocation;
import misc.FileSystemHelper;



/**
 * Class for order-preserving encryption using the scheme of Wozniak et. al 2013
 * 
 * @author Tim Waage
 *
 */
public class OPE_RowKeyRSS extends OPEScheme {
	
	// TODO im paper anmerken: trotz lazy samplings werden unter umständen auch werte verschlüsselt/gemappt, die nicht benötigt werden
	
	// path to dictionaries
	private String meatadataPath = "/home/tim/TimDB/";
	
	// underlying dictionary
	private TreeMap<Long, Long> mainDict = null;	
	
	// for the randomness needed by the scheme
	private PRG g;
	
	// identifier of the currently open dictionary
	String dictID = "";
	
	private long M;
	private long N;
	
	// (only needed when returning the scheme as HashMap) 
	private int pbits;
	private int cbits;
		
	
	
	/**
	 * Constructor
	 * @param _id the schemes id string
	 * @param _ks the keystore this scheme is using
	 * @param _db the database this scheme is connecting to
	 * @param _pbits the length of the plain text space in bits
	 * @param _cbits the length of the cipher text space in bits
	 */
	public OPE_RowKeyRSS(KeyStoreManager _ks, DBClient _db, int _pbits, int _cbits) {
		
		super("RowKeyRSS", _ks, _db);
		
		pbits = _pbits;
		cbits = _cbits;
	
		M = (long)Math.pow(2, _pbits); // - 1 because the sign takes one bit as well
		N = (long)Math.pow(2, _cbits); // - 1 to make sure the ciphertexts are still short enough after step 4 
		
		
		g = new PRG();
		
		
	}

	
	
	/**
	 * loads the dictionary, generates a new one, if none exists yet
	 * @param database location on which this schme instance is operating on
	 */
	private void loadDictionary(DBLocation id) {
		
		// check, if we have to load another dictionary
		if(!id.getIdAsPath().equals(dictID)) {
		
			// save the other dictionary
			if(!dictID.equals("")) close();
			
			// if no dictionary exists, create one and save it
			File file = new File(meatadataPath + id.getIdAsPath());
			
			// TODO !!! for benchmark purposes always create a new dict, comment that back in for non benchmark usage
			if(!file.exists()) { 
				
				mainDict = new TreeMap<Long, Long>();
				long r_min = 0;
				long r_max = 0;
				
				// Step 1: Randomly decide whether to choose a lower bound (0) or an upper bound (1) first
				int q = g.generateRandomInt(0, 1);
				
				// Step 2
				if(q == 0) {
					r_min = g.generateRandomLong(1, N - M + 1);
					r_max = g.generateRandomLong(r_min + M - 1, N);
				}
				else {
					r_max = g.generateRandomLong(M, N);
					r_min = g.generateRandomLong(1, r_max - M + 1);
				}
				
				// Step 4: adjust the range, it does not matter if we do that in advance, since addition is commutative
				r_min += r_min - 1;
				r_max += r_min - 1;
				
				// Step 3 for d_min and d_max, "manual" sampling to set up the outer bounds
				// always consider -M for negative values as lower bound, DIFFERENCE to [Woz13], so mention it in the Paper!
				sample(mainDict,  0,                   0, M-2,                              r_min, r_max-1);
				sample(mainDict, M-1, mainDict.lastKey()+1, M-1, mainDict.get(mainDict.lastKey())+1, r_max);
				
			}
			else mainDict = FileSystemHelper.readTreeMapFromFile(meatadataPath + id.getIdAsPath());
			
			// set current dictID as the just opened id path
			dictID = id.getIdAsPath();
		}
		
	
	}
	
	
	
	/**
	 * encrypts a numerical value
	 * @param input the numerical value to encrypt
	 * @param id the path to the place within the database the encrypted value is suppoesed to be written to
	 * @return the encrypted value 
	 */
	public long encrypt(long input, DBLocation id) {
		
		// load dictionary
		loadDictionary(id);
		
		// add Value, if not already mapped
		if(!mainDict.containsKey(input)) {
			
			// iterate through the dictionary to find the right position for the new key (3.1)
			NavigableSet<Long> navKeySet = mainDict.navigableKeySet();
			
			//System.out.println("NavKeySet( + " + navKeySet.size() + "): " + navKeySet);
			
			long d_min = navKeySet.lower(input);
			long d_max = navKeySet.higher(input);
			
			long r_min = mainDict.get(d_min);
			long r_max = mainDict.get(d_max);
			
			sample(mainDict, input, d_min+1, d_max-1, r_min+1, r_max-1);
		}
				
		return mainDict.get(input);
	}
	
	
	
	/**
	 * decrypts a numerical value
	 * @param input the numerical value to decrypt
	 * @param id the path to the place within the database the encrypted value is read from
	 * @return the decrypted value 
	 */
	public long decrypt(long input, DBLocation id) {
		
		loadDictionary(id);
		
		for(long k : mainDict.keySet()) if(mainDict.get(k) == input) return k;
		
		return -1;
	}
	
	
	
	
	/**
	 * adds an integer to a dictionary and computes the correct cipher value
	 * @param dict 
	 * @param target
	 */
	private void sample(TreeMap<Long, Long> dict, long target, long d_min, long d_max, long r_min, long r_max) {
		
		long p = g.generateRandomLong(d_min, d_max);
		long c = g.generateRandomLong(r_min + p - d_min, r_max - (d_max - p));
	
		mainDict.put(p, c);
		
		
		//if(target < p) sample(mainDict, target, d_min, p-1, r_min, c-1);
		if(Long.compareUnsigned(target, p) < 0) sample(mainDict, target, d_min, p-1, r_min, c-1);
		//if(target > p) sample(mainDict, target, p+1, d_max, c+1, r_max);
		if(Long.compareUnsigned(target, p) > 0) sample(mainDict, target, p+1, d_max, c+1, r_max);
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
			//System.out.println("Saving Rowkey dictionary to: " + meatadataPath + dictID);
			try {
				FileSystemHelper.writeTreeMapToFile(mainDict, meatadataPath + dictID);
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Error while saving " + meatadataPath + dictID);
			}
		}
		
	}
	
	@Override
	public String toString() {
		
		return "Wozniak et al. 2013";
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