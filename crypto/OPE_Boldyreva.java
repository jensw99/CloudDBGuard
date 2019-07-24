package crypto;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

//import org.apache.commons.math3.util.FastMath;
import org.jdom2.Element;

import misc.FileSystemHelper;
import databases.DBClient;
import databases.DBLocation;



/**
 * 
 * @author Daniel Homann
 *
 */
public class OPE_Boldyreva extends OPEScheme {

	protected int pbits;
	protected int cbits;
	private byte[] aesk;
	
	// dictionary
	private HashMap<BigInteger, BigInteger> dgap_cache = null;
	
	// path to dictionaries
	private String meatadataPath = "/Users/michaelbrenner/CloudDBGuard/tim/TimDB/";
			
	// identifier of the currently open dictionary
	String dictID = "";
	
	
	/**
	 * Contructor
	 * @param _ks the keystore this scheme is using
	 * @param _db the database client this scheme is using
	 * @param plainbits the size of the plaintext space
	 * @param cipherbits the size of the ciphertext space
	 */
	public OPE_Boldyreva(KeyStoreManager _ks, DBClient _db, int plainbits, int cipherbits) {

		super("mOPE", _ks, _db);
		
		pbits = plainbits;
		cbits = cipherbits;
		aesk = aeskey(ks.getKeyFor("Boldy", 16).toString());
		
	}
	
	
	/**
	 * generates a new dictionary
	 */
	private void loadDictionary(DBLocation id) {
		
		// check, if we have to load another dictionary
		if(!id.getIdAsPath().equals(dictID)) {
		
			// save the other dictionary
			if(!dictID.equals("")) close();
			
			// if no dictionary exists, create one 
			File file = new File(meatadataPath + db + " - " + id.getIdAsPath());
			if(!file.exists()) { 				
				dgap_cache = new HashMap<BigInteger, BigInteger>();		
			}
			// else read the existing dictionary from file
			else dgap_cache = FileSystemHelper.readHashMapFromFile(meatadataPath + db + " - " + id.getIdAsPath());
			
			// set current dictID as the just opened id path
			dictID = id.getIdAsPath();
		}
		
	
	}
	
	
	static private byte[] aeskey(String key) {
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("SHA-256");
			md.update(key.getBytes("UTF-8")); // Change this to "UTF-16" if
												// needed
			byte[] v = md.digest();
			byte[] result = Arrays.copyOf(v, 16);
			return result;

		} catch (NoSuchAlgorithmException e) {
			System.out.println("Failed in instantiation of SHA256.");
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			System.out.println("Encoding of AES key not supported.");
			e.printStackTrace();
		}

		
		return null;
	}

	private OPE_Boldyreva_DomainRange lazy_sample(BigInteger d_lo, BigInteger d_hi, BigInteger r_lo, BigInteger r_hi,
			OPE_Boldyreva_blockrng prng, BigInteger text, boolean encrypt) {
		BigInteger ndomain = d_hi.subtract(d_lo).add(BigInteger.ONE);
		BigInteger nrange = r_hi.subtract(r_lo).add(BigInteger.ONE);
		// This should not happen
		if (nrange.compareTo(ndomain) < 0) {
			System.out.println("nrange<ndomain");
		}

		if (ndomain.compareTo(BigInteger.ONE) == 0)
			return new OPE_Boldyreva_DomainRange(d_lo, r_lo, r_hi);

		/*
		 * Deterministically reset the PRNG counter, regardless of
		 * whether we had to use it for HGD or not in previous round.
		 */

		byte[] v = null;
		try {
			Mac sha256_HMAC = Mac.getInstance("HmacSHA256");
			SecretKeySpec secret_key = new SecretKeySpec(ks.getKeyFor("Boldy", 16), "HmacSHA256");
			sha256_HMAC.init(secret_key);
			String message = d_lo.toString() + "/" + d_hi.toString() + "/" + r_lo.toString() + "/" + r_hi.toString();
			v = sha256_HMAC.doFinal(message.getBytes());
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		byte[] v1 = Arrays.copyOf(v, OPE_Boldyreva_AES.blocksize);
		prng.set_ctr(v1);

		BigInteger rgap = nrange.divide(BigInteger.valueOf(2));
		BigInteger dgap;
		

		BigInteger key = r_lo.add(rgap);
		if (dgap_cache.containsKey(key)) {	
			dgap = dgap_cache.get(key);
		} else {
			
			dgap = domain_gap(ndomain, nrange, nrange.divide(BigInteger.valueOf(2)), prng);
			dgap_cache.put(r_lo.add(rgap), dgap);
			
		}

		if (encrypt) {
			if (text.compareTo(d_lo.add(dgap)) < 0)
				return lazy_sample(d_lo, d_lo.add(dgap).subtract(BigInteger.ONE), r_lo,
						r_lo.add(rgap).subtract(BigInteger.ONE), prng, text, encrypt);
			else
				return lazy_sample(d_lo.add(dgap), d_hi, r_lo.add(rgap), r_hi, prng, text, encrypt);
		} else {
			if (text.compareTo(r_lo.add(rgap)) < 0)
				return lazy_sample(d_lo, d_lo.add(dgap).subtract(BigInteger.ONE), r_lo,
						r_lo.add(rgap).subtract(BigInteger.ONE), prng, text, encrypt);
			else
				return lazy_sample(d_lo.add(dgap), d_hi, r_lo.add(rgap), r_hi, prng, text, encrypt);
		}

	}

	private BigInteger domain_gap(BigInteger ndomain, BigInteger nrange, BigInteger rgap, OPE_Boldyreva_blockrng prng) {
				
		try {
		
			//HypergeometricSampler<BigInteger> sampler = new OPE_Boldyreva_BIGH2PEC();
			//return sampler.sample(rgap, ndomain, nrange.subtract(ndomain), prng);
			
			HypergeometricSampler<Long> sampler = new OPE_Boldyreva_BIGH2PEC_Long();
			return BigInteger.valueOf(sampler.sample(rgap.longValue(), ndomain.longValue(), nrange.subtract(ndomain).longValue(), prng));
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	

	public OPE_Boldyreva_DomainRange search(BigInteger text, boolean encrypt) {
		OPE_Boldyreva_blockrng r = new OPE_Boldyreva_blockrng(aesk);

		return lazy_sample(BigInteger.ZERO, BigInteger.valueOf(2).pow(pbits), BigInteger.ZERO, BigInteger.valueOf(2)
				.pow(cbits), r, text, encrypt);
	}

	/**
	 * Long-Version of the encrypt method to match the interface IOrderPreserving
	 */
	public long encrypt(long input, DBLocation id) {
		
		// load dictionary
		loadDictionary(id);
		
		BigInteger ptext = BigInteger.valueOf(input);		
		return encryptBigInteger(ptext).longValue();

	}
	
	
	/**
	 * the main encryption method of the boldyreva OPE scheme
	 * @param ptext the value to encrypt
	 * @return the encrypted value
	 */
	protected BigInteger encryptBigInteger(BigInteger ptext) {
		
		OPE_Boldyreva_DomainRange dr = search(ptext, true);
		byte[] v = aeskey(ptext.toString());

		OPE_Boldyreva_blockrng aesrand = new OPE_Boldyreva_blockrng(aesk);
		aesrand.set_ctr(v);

		BigInteger nrange = dr.r_hi.subtract(dr.r_lo).add(BigInteger.ONE);
		return dr.r_lo.add(aesrand.rand_zz_mod(nrange));
		
	}

	
	/**
	 * Long-Version of the decrypt method to match the interface IOrderPreserving
	 */
	public long decrypt(long input, DBLocation id) {
		
		loadDictionary(id);
		
		BigInteger ctext = BigInteger.valueOf(input);
				
		return decryptBigInteger(ctext).longValue();

	}
	
	protected BigInteger decryptBigInteger(BigInteger ctext) {
		
		OPE_Boldyreva_DomainRange dr = search(ctext, false);		
		return dr.d;
	}

	@Override
	public void close() {
		// for benchmark reasons don't save the index, uncomment for non benchmark use
		/*try {
			
			FileSystemHelper.writeHashMapToFile(dgap_cache, meatadataPath + db.getID() + " - " + dictID);
			
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error while saving " + meatadataPath + db.getID() + " - " + dictID);
		}*/
		
	}

	@Override
	public String toString() {
		
		return "Boldyreva 2009 (" + this.pbits + " painbits, " + this.cbits + " cipherbits)";
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
