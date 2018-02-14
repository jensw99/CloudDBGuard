package crypto;
import java.math.BigInteger;

public class OPE_Boldyreva_DomainRange {
	
	BigInteger d;
	BigInteger r_lo;
	BigInteger r_hi;
	
	public OPE_Boldyreva_DomainRange(BigInteger d_arg, BigInteger r_lo_arg, BigInteger r_hi_arg) {
		
		d = d_arg;
		r_lo = r_lo_arg;
		r_hi = r_hi_arg;
	}
	
}
