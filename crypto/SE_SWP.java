package crypto;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.codec.binary.Base64;
import org.jdom2.Element;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Vector;

import databases.Result;
import databases.DBClient;
import databases.DBLocation;
import databases.Request;
import enums.ColumnType;
import enums.RequestType;
import misc.Misc;



/**
 * Contains the logic required for encryting and searching using the SWP scheme
 * @author Tim Waage
 *
 */
public class SE_SWP extends SEScheme{

	private PRG g; // G as it appears in the paper
	private PRF f; // F as it appears in the paper
		
	private int n; // word length
	private int m; // length of F_k_i(S_i)
	
		
	
	/**
	 * Implements a certain encryption process
	 * @param _ks the JCEKS keystore this scheme is using
	 * @param _db the target database this scheme is operating on
	 * @param _n the fixed word length, in Sonn00 referred to as n
	 * @param _m the length of F_ki(S_i), in Song00 referred to as m
	 */
	public SE_SWP(KeyStoreManager _ks, DBClient _db, int _n, int _m) {
		
		super("SWP", _ks, _db);
		
		n = _n;
		m = _m;
		
		//setup pseudo random generator G for n-m-bit random byte arrays
		//no random seed, requirement of the final SWP scheme
		g = new PRG(12345678);
		f = new PRF();		
	}
	
	
	
	/**
	 * main method of the scheme, encrypts Strings
	 * 
	 * @param s the String to be encrypted
	 * @return the encrypted String
	 */
	public String encrypt(String s, DBLocation id) {
	
		// split up the string into words
		Vector<String> result = new Vector<String>();
		String[] split  = Misc.getWords(s); // split on spaces, tabs etc...
			
		// make all words have the length n, split and pad if necessary
		for(int i=0; i<split.length; i++) {	
			result.addAll(Misc.splitWord(split[i], this.n));
		}
		
		// finally encrypt them	
		return Base64.encodeBase64String(generateOutput(result));
	}
	
	
	
	/**
	 * generates the ciphertext of multiple n-bit words, should usually be called by this.encrypt()
	 * @param a vector of n bit words
	 * @return the ciphertext of multiple n-bit words
	 */
	private byte[] generateOutput(Vector<String> words) {
		
		byte[] result = new byte[words.size() * n];
		byte[] ciphertext;
		
		for(int i=0; i<words.size(); i++) {
			
			ciphertext = generateOutput(words.elementAt(i)); 
					
			System.arraycopy(ciphertext, 0, result, i*n, n);
		}
		
		return result;
	}
	
	
	
	/**
	 * Implements the scheme, called to encrypt a single word,
	 * should be called by this.generateOutput(Vector<String> words)
	 * @param w the plain-text word
	 * @return the generated encrypted word
	 */
	private byte[] generateOutput(String word) {
	
		byte[] ciphertext = new byte[n];
		byte[] w = this.getPreEncryptedSearchWord(word.getBytes()); //pre-encrypt the search word
		
		byte[] l = new byte[n-m];
		byte[] r = new byte[m];
		
		//split the pre-encrypted word in L_i and R_i
		for (int i=0; i<n-m; i++) l[i] = w[i];
		for (int i=n-m; i<n; i++) r[i-n+m] = w[i];
			
		//generate an n-m-bit S_i
		byte[] s = g.generateRandomBytes(n-m);
		
		//generate an m-bit F_ki(S_i)
		byte[] fs = computeFS(s, getControlledSearchKey(l)); 
				
		//combine t = s + fs
		byte[] t = new byte[s.length + fs.length];
		System.arraycopy(s, 0, t, 0, s.length);
		System.arraycopy(fs, 0, t, s.length, fs.length);
		
		//finally XOR w and t
		for(int i=0; i<n; i++) {
			ciphertext[i] = (byte)(w[i] ^ t[i]);
		}
	
		return ciphertext;
	}	
	
	
	
	/**
	 * Computes F(S) as it appears in the paper
	 * @param input the input
	 * @param encryptionKey the key
	 * @return F(S)
	 */
	public byte[] computeFS (byte[] input, byte[] encryptionKey) {
		
		//using the last bytes of AES
		try {
				
			byte[] enc = PRF.compute_MD5(input, encryptionKey);
					
			//if outputLength is already correct
			if(enc.length == m) return enc;
			//shorten 
			else{ 
				byte[] result = new byte[m];
				System.arraycopy(enc, 0, result, 0, m);
				return result;
			}
			
		}
		catch(Exception e) {
			e.printStackTrace();
			return null;
		}		
	}
	
	
	
	/**
	 * used for scheme II of the SWP algorithm:
	 * (deprecated: encrypts a byte array, cipher-text output has a length, that it 
	 * can be reused as key again) 
	 * @param input a word 
	 * @return f_k'(L_i) = k_i
	 */
	public byte[] getControlledSearchKey (byte[] input) {
		
		return PRF.compute_MD5(input, ks.getKeyFor("SWP_k1", 16));
		
	}
	
	
	
	/**
	 * used (by Bob as well) to retrieve the search word for hidden searching (scheme 3)
	 * @param word
	 * @return the pre-encrypted search word
	 */
	private byte[] getPreEncryptedSearchWord(byte[] word) {
		
		return f.encrypt_AES_CFB(word, ks.getKeyFor("SWP_k2", 24), n);
	}
	
	
	
	/**
	 * used by Bob to retrieve the Key k' for controlled searching from Alice
	 * @param w the word thats belongs to the key
	 * @return k' for controlled searching
	 */
	private byte[] getKeyForControlledSearching(byte[] w) {
		
		byte[] l = new byte[n-m];
		for (int i=0; i<n-m; i++) l[i] = w[i];
		return getControlledSearchKey(l);				
	}
	
			
	
	/**
	 * performs the search
	 * @param searchword
	 * @param id database path to be searched through, with only one column: the one that is to be searched
	 * @return a set of keys of rows in which the searchword was found
	 */
	public SE_RowIdentifierSet search (String searchword, DBLocation id) {
		
		ColumnType rowkeyType = id.getTable().getRowkeyColumn().getType();
		SE_RowIdentifierSet overallResults = new SE_RowIdentifierSet(rowkeyType);
				
		//compare the encryption word length with the actual length of the search word, act depending on the result:
		if(searchword.length() <= n) {
			
			if(searchword.length() < n) searchword = Misc.pkcs7pad(searchword, n);
									
			byte[] preEncryptedSearchWord = this.getPreEncryptedSearchWord(searchword.getBytes());
			byte[] key =  getKeyForControlledSearching(preEncryptedSearchWord);
				
			// read all rows of that column 
			Request readRequest = new Request(RequestType.READ, id);
			Result result = db.processRequest(readRequest);
			
			HashMap<byte[], String> rows = result.getKeyStringsFrom(id.getColumns().get(0));
									
			for(byte[] rowkey : rows.keySet()) {
				if(lookup(ByteBuffer.wrap(Base64.decodeBase64(rows.get(rowkey))), preEncryptedSearchWord, key)) {
					// vorm RowCondition re-coding:
					// overallResults.add(new Id(id.getKeyspace(), id.getTable(), rowkey, new String[]{column}).toString());
					if(rowkeyType == ColumnType.STRING) overallResults.getStringSet().add(Misc.ByteArrayToCharString(rowkey));
					if(rowkeyType == ColumnType.INTEGER) overallResults.getIntSet().add(Misc.bytesToLong(rowkey));
					if(rowkeyType == ColumnType.BYTE) overallResults.getByteSet().add(rowkey);
				}
			}			
		}
		
		else {
			//we need to look up all fragments of the split word
			//TODO!
		}	
		
		return overallResults;
	}
	
	
	
	/**
	 * looks up one single n-bit word in a given ByteBuffer
	 * @param input ByteBuffer to look in
	 * @param word the search word
	 * @param abortAfterFirstMatch true, if we don't care about how many matches occur, just whether or not there are matches at all
	 * @return a vector of ints, indicating the positions where matches occured
	 */
	protected boolean lookup (ByteBuffer input, byte[] preEncryptedSearchWord, byte[] key) {
		
		int wordCount = input.array().length / n;
		byte[] ciphertext = new byte[n];
		
		byte[] w = preEncryptedSearchWord;; //searchword
				
		for(int p=0; p<wordCount; p++) {	
		
			input.get(ciphertext, 0, n); 
		
			//reconstruct t
			byte[] t = new byte[n];				
		
			for(int i=0; i<n; i++) {
				t[i] = (byte)(ciphertext[i] ^ w[i]);
			}
			//check, whether t is of the form s+fs 
				
			//reconstruct s from t
			byte[] s = new byte[n-m];
			for(int i=0; i<n-m; i++) s[i] = t[i];
				
			//reconstruct fs from t
			byte[] fs = new byte[m];
			for(int i=n-m, k=0; i<n; i++, k++) fs[k] = t[i];
				
			//see, if fs and f_ki(s_i) (=fs2) match
			byte[] fs2 = computeFS(s, key);
			if(Arrays.equals(fs, fs2)) {
				return true;
			}				
		}
		
		return false;
	}



	@Override
	public HashSet<String> encryptSet(HashSet<String> input, DBLocation id) {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public Element getThisAsXMLElement() {
		
		Element schemeRoot = new Element("se");
		
		Element schemeIdentifier = new Element("identifier");
		schemeIdentifier.addContent(name);
		schemeRoot.addContent(schemeIdentifier);
		
		Element schemeN = new Element("n");
		schemeN.addContent(String.valueOf(n));
		schemeRoot.addContent(schemeN);
		
		Element schemeM = new Element("m");
		schemeM.addContent(String.valueOf(m));
		schemeRoot.addContent(schemeM);
		
		return schemeRoot;
	}



	@Override
	public void initializeFromXMLElement(Element data) {
		// TODO Auto-generated method stub
		
	}



	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}



	@Override
	public void delete() {
		// TODO Auto-generated method stub
		
	}

}