package crypto;

import java.math.BigInteger;


public class OPE_Boldyreva_blockrng {

	private byte[] ctr = {0};
	private OPE_Boldyreva_AES cipher;
	
	public OPE_Boldyreva_blockrng(byte[] aeskey)
	{
		cipher = new OPE_Boldyreva_AES(aeskey);
	}
	
	public BigInteger rand_zz_mod(BigInteger max) {
        byte[] buf = rand_bytes(max.bitLength() / 8 + 1);
        return (new BigInteger(buf)).mod(max);
    }
	
	
	public byte[] rand_bytes(int nbytes) {
		byte[] buf = new byte[nbytes];
		
		for (int i = 0; i < nbytes; i += OPE_Boldyreva_AES.blocksize) {
			for (int j = 0; j < OPE_Boldyreva_AES.blocksize; j++) {
				ctr[j]++;
				if (ctr[j] != 0)
					break;
			}

			byte[] ct = cipher.encrypt(ctr);

			if (OPE_Boldyreva_AES.blocksize < nbytes - i) {
				System.arraycopy(ct, 0, buf, i, OPE_Boldyreva_AES.blocksize);
			} else {
				System.arraycopy(ct, 0, buf, i, nbytes - i);
			}
		}
		return buf;
	}

	void set_ctr(String v) {
		// throw_c(v.size() == BlockCipher::blocksize);
		ctr = v.getBytes();
	}
	
	void set_ctr(byte[] v) {
		// throw_c(v.size() == BlockCipher::blocksize);
		ctr = v;
	}

	/*
	 * // length of the pseudo random output
	 * private int l;
	 * 
	 * private SecureRandom sr;
	 *//**
	 * Constructor for random seed
	 * seed automatically chosen at random by not specifying it
	 * (see http://www.cigital.com/justice-league-blog/2009/08/14/proper-use-of-
	 * javas-securerandom/)
	 */
	/*
	 * public blockrng(int _l, byte[] s) {
	 * this(s);
	 * 
	 * l = _l;
	 * 
	 * }
	 * 
	 * public blockrng(byte[] s) {
	 * 
	 * try {
	 * sr = SecureRandom.getInstance("SHA1PRNG", "BC");
	 * sr.setSeed(s);
	 * }
	 * catch (Exception e) {
	 * System.out.println(
	 * "An error occured during the initialization of the pseudorandom generator"
	 * );
	 * }
	 * }
	 *//**
	 * Constructor for certain output length and fixed seed
	 * 
	 * @param w
	 *            length of the random generated output (in Bytes)
	 * @param s
	 *            seed to be used for initialization
	 */
	/*
	 * public blockrng(int _l, long s) {
	 * this(_l);
	 * sr.setSeed(s);
	 * }
	 * 
	 * 
	 * public void set_ctr(byte[] s)
	 * {
	 * ByteBuffer wrapped = ByteBuffer.wrap(s);
	 * long seed = wrapped.getLong();
	 * sr.setSeed(seed);
	 * }
	 *//**
	 * Generates w random bytes
	 * 
	 * @return w random byes
	 */
	/*
	 * public byte[] generateRandomBytes() {
	 * 
	 * byte[] result = new byte[l];
	 * sr.nextBytes(result);
	 * 
	 * return result;
	 * }
	 * 
	 * @Override
	 * public void rand_bytes(int nbytes, byte[] buf) {
	 * // TODO Auto-generated method stub
	 * 
	 * }
	 */
}
