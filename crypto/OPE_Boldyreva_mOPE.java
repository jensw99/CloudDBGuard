package crypto;

import java.math.BigInteger;

import databases.DBClient;

public class OPE_Boldyreva_mOPE extends OPE_Boldyreva {
	
	private BigInteger offset;
	
	public OPE_Boldyreva_mOPE(KeyStoreManager _ks, DBClient _db, int plainbits, int cipherbits, BigInteger offset)
	{
		super(_ks, _db, plainbits, cipherbits);
		this.offset = offset;
	}	
	
	@Override
	protected BigInteger encryptBigInteger(BigInteger ptext) {
		BigInteger modptext = ptext.subtract(offset).mod(BigInteger.valueOf(2).pow(pbits));
		return super.encryptBigInteger(modptext);		
	}

	@Override
	protected BigInteger decryptBigInteger(BigInteger ctext) {
		BigInteger modptext = super.decryptBigInteger(ctext);
		return modptext.add(offset).mod(BigInteger.valueOf(2).pow(pbits));
	}
	
	@Override
	public String toString() {		
		return "Boldyreva 2011 \"mOPE\" (" + this.pbits + " painbits, " + this.cbits + " cipherbits, " + this.offset + " offset)";
	}
	
}
