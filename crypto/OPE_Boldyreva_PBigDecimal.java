package crypto;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;

import misc.org.nevec.rjm.BigDecimalMath;

public class OPE_Boldyreva_PBigDecimal extends BigDecimal{

	private static final long serialVersionUID = -2211377826258697071L;

	private static final RoundingMode rounding = RoundingMode.HALF_UP;
	private static final int startPrecision = 120;
	private static MathContext context = new MathContext(startPrecision, rounding);

	public static final OPE_Boldyreva_PBigDecimal ZERO = new OPE_Boldyreva_PBigDecimal(0);
	public static final OPE_Boldyreva_PBigDecimal ONE = new OPE_Boldyreva_PBigDecimal(1);

	public static void setPrecision(int precision) {
		if (precision > 0) {
			context = new MathContext(precision, rounding);
		}
	}

	public OPE_Boldyreva_PBigDecimal(BigInteger val) {
		super(val, context);
	}

	public OPE_Boldyreva_PBigDecimal(long val) {
		super(val, context);
	}

	public OPE_Boldyreva_PBigDecimal(double val) {
		super(val, context);
	}

	public OPE_Boldyreva_PBigDecimal(String s) {
		super(s, context);
	}

	public OPE_Boldyreva_PBigDecimal(java.math.BigDecimal bd) {
		this(bd.toString());
	}

	public OPE_Boldyreva_PBigDecimal add(OPE_Boldyreva_PBigDecimal augend) {
		return new OPE_Boldyreva_PBigDecimal(super.add(augend, context));
	}

	public OPE_Boldyreva_PBigDecimal divide(OPE_Boldyreva_PBigDecimal augend) {
		return new OPE_Boldyreva_PBigDecimal(super.divide(augend, context));
	}

	public OPE_Boldyreva_PBigDecimal multiply(OPE_Boldyreva_PBigDecimal augend) {
		return new OPE_Boldyreva_PBigDecimal(super.multiply(augend, context));
	}

	public OPE_Boldyreva_PBigDecimal subtract(OPE_Boldyreva_PBigDecimal augend) {
		return new OPE_Boldyreva_PBigDecimal(super.subtract(augend, context));
	}
	
	public OPE_Boldyreva_PBigDecimal pow(int n){
		return new OPE_Boldyreva_PBigDecimal(super.pow(n, context));
	}

	public OPE_Boldyreva_PBigDecimal negate() {
		return new OPE_Boldyreva_PBigDecimal(super.negate());
	}

	static public OPE_Boldyreva_PBigDecimal exp(OPE_Boldyreva_PBigDecimal x) {
		return new OPE_Boldyreva_PBigDecimal(BigDecimalMath.exp(x));
	}

	static public OPE_Boldyreva_PBigDecimal sqrt(OPE_Boldyreva_PBigDecimal x) {
		if (x.compareTo(BigDecimal.ZERO) < 0)
		{
			throw new ArithmeticException("Square root of numbers < 0 undefined");
		}
		
		return new OPE_Boldyreva_PBigDecimal(BigDecimalMath.sqrt(x, context));
	}

	static public OPE_Boldyreva_PBigDecimal log(OPE_Boldyreva_PBigDecimal x) {
		if (x.compareTo(BigDecimal.ZERO) <= 0)
		{
			throw new ArithmeticException("Logarithm of numbers <= 0 undefined");
		}
		return new OPE_Boldyreva_PBigDecimal(BigDecimalMath.log(x));
	}

	static public OPE_Boldyreva_PBigDecimal pow(OPE_Boldyreva_PBigDecimal x, OPE_Boldyreva_PBigDecimal y) {
		if (x.compareTo(BigDecimal.ZERO) < 0)
		{
			throw new ArithmeticException("Power of base < 0 in general undefined");
		}		
		return new OPE_Boldyreva_PBigDecimal(BigDecimalMath.pow(x, y));
	}

}
