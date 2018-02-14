//mostly copied from apache.math library, changed from int to long

package crypto;

import java.math.BigInteger;

import org.apache.commons.math3.exception.MathInternalError;
import org.apache.commons.math3.exception.OutOfRangeException;
import org.apache.commons.math3.exception.util.LocalizedFormats;
import org.apache.commons.math3.special.Gamma;
import org.apache.commons.math3.util.FastMath;
import org.apache.commons.math3.util.MathUtils;

public class OPE_Boldyreva_INVHGD implements HypergeometricSampler<Long> {
	long populationSize, numberOfSuccesses, sampleSize;

	@Override
	public Long sample(Long KK, Long NN1, Long NN2, OPE_Boldyreva_blockrng prng) throws Exception {
		this.populationSize = NN1 + NN2;
		this.numberOfSuccesses = NN1;
		this.sampleSize = KK;

		double p = RAND(prng);

		if (p < 0.0 || p > 1.0) {
			throw new OutOfRangeException(p, 0, 1);
		}

		long lower = FastMath.max(0, sampleSize + numberOfSuccesses - populationSize);
		if (p == 0.0) {
			return lower;
		}
		if (lower == Long.MIN_VALUE) {
			if (checkedCumulativeProbability(lower) >= p) {
				return lower;
			}
		} else {
			lower -= 1; // this ensures cumulativeProbability(lower) < p, which
						// is important for the solving step
		}

		long upper = FastMath.min(numberOfSuccesses, sampleSize);
		if (p == 1.0) {
			return upper;
		}

		// use the one-sided Chebyshev inequality to narrow the bracket
		// cf. AbstractRealDistribution.inverseCumulativeProbability(double)
		final double mu = getNumericalMean();
		final double sigma = FastMath.sqrt(getNumericalVariance());
		final boolean chebyshevApplies = !(Double.isInfinite(mu) || Double.isNaN(mu) || Double.isInfinite(sigma)
				|| Double.isNaN(sigma) || sigma == 0.0);
		if (chebyshevApplies) {
			
			double k = FastMath.sqrt((1.0 - p) / p);
			double tmp = mu - k * sigma;
			if (tmp > lower) {
				lower = ((long) FastMath.ceil(tmp)) - 1;
			}
			k = 1.0 / k;
			tmp = mu + k * sigma;
			if (tmp < upper) {
				upper = ((long) FastMath.ceil(tmp)) - 1;
			}
		}
		else System.out.println("!");

		
		return solveInverseCumulativeProbability(p, lower, upper);
	}

	private static double RAND(OPE_Boldyreva_blockrng prng) {
		long rzz = prng.rand_zz_mod(BigInteger.valueOf(Long.MAX_VALUE)).longValue();
		return 1.0 / rzz;

	}

	private double checkedCumulativeProbability(long argument) throws MathInternalError {
		double result = Double.NaN;
		result = cumulativeProbability(argument);
		if (Double.isNaN(result)) {
			throw new MathInternalError(LocalizedFormats.DISCRETE_CUMULATIVE_PROBABILITY_RETURNED_NAN, argument);
		}
		return result;
	}

	public double cumulativeProbability(long x) {
		double ret;

		long[] domain = getDomain(populationSize, numberOfSuccesses, sampleSize);
		
		if (x < domain[0]) {
			ret = 0.0;
		} else if (x >= domain[1]) {
			ret = 1.0;
		} else {
			// this is always 0
			ret = innerCumulativeProbability(domain[0], x, 1);
			
		}
		
		return ret;
	}

	private double innerCumulativeProbability(long x0, long x1, long dx) {
		double ret = probability(x0);
		while (x0 != x1) {
			x0 += dx;
			ret += probability(x0);
			
		}
		return ret;
	}

	public double probability(long x) {
		final double logProbability = logProbability(x);
		return logProbability == Double.NEGATIVE_INFINITY ? 0 : FastMath.exp(logProbability);
	}

	public double logProbability(long x) {
		double ret;

		long[] domain = getDomain(populationSize, numberOfSuccesses, sampleSize);
		if (x < domain[0] || x > domain[1]) {
			ret = Double.NEGATIVE_INFINITY;
		} else {
			double p = (double) sampleSize / (double) populationSize;
			double q = (double) (populationSize - sampleSize) / (double) populationSize;
			double p1 = logBinomialProbability(x, numberOfSuccesses, p, q);
			double p2 = logBinomialProbability(sampleSize - x, populationSize - numberOfSuccesses, p, q);
			double p3 = logBinomialProbability(sampleSize, populationSize, p, q);
			ret = p1 + p2 - p3;
		}
		
		return ret;
	}

	static double logBinomialProbability(long x, long n, double p, double q) {
		double ret;
		if (x == 0) {
			if (p < 0.1) {
				ret = -getDeviancePart(n, n * q) - n * p;
			} else {
				ret = n * FastMath.log(q);
			}
		} else if (x == n) {
			if (q < 0.1) {
				ret = -getDeviancePart(n, n * p) - n * q;
			} else {
				ret = n * FastMath.log(p);
			}
		} else {
			ret = getStirlingError(n) - getStirlingError(x) - getStirlingError(n - x) - getDeviancePart(x, n * p)
					- getDeviancePart(n - x, n * q);
			double f = (MathUtils.TWO_PI * x * (n - x)) / n;
			ret = -0.5 * FastMath.log(f) + ret;
		}
		return ret;
	}

	static double getStirlingError(double z) {
		double ret;
		if (z < 15.0) {
			double z2 = 2.0 * z;
			if (FastMath.floor(z2) == z2) {
				ret = EXACT_STIRLING_ERRORS[(int) z2];
			} else {
				ret = Gamma.logGamma(z + 1.0) - (z + 0.5) * FastMath.log(z) + z - HALF_LOG_2_PI;
			}
		} else {
			double z2 = z * z;
			ret = (0.083333333333333333333 - (0.00277777777777777777778 - (0.00079365079365079365079365
					- (0.000595238095238095238095238 - 0.0008417508417508417508417508 / z2) / z2) / z2) / z2) / z;
		}
		return ret;
	}

	static double getDeviancePart(double x, double mu) {
		double ret;
		if (FastMath.abs(x - mu) < 0.1 * (x + mu)) {
			double d = x - mu;
			double v = d / (x + mu);
			double s1 = v * d;
			double s = Double.NaN;
			double ej = 2.0 * x * v;
			v *= v;
			int j = 1;
			while (s1 != s) {
				s = s1;
				ej *= v;
				s1 = s + ej / ((j * 2) + 1);
				++j;
			}
			ret = s1;
		} else {
			ret = x * FastMath.log(x / mu) + mu - x;
		}
		return ret;
	}

	private static final double HALF_LOG_2_PI = 0.5 * FastMath.log(MathUtils.TWO_PI);

	/** exact Stirling expansion error for certain values. */
	private static final double[] EXACT_STIRLING_ERRORS = { 0.0, /* 0.0 */
			0.1534264097200273452913848, /* 0.5 */
			0.0810614667953272582196702, /* 1.0 */
			0.0548141210519176538961390, /* 1.5 */
			0.0413406959554092940938221, /* 2.0 */
			0.03316287351993628748511048, /* 2.5 */
			0.02767792568499833914878929, /* 3.0 */
			0.02374616365629749597132920, /* 3.5 */
			0.02079067210376509311152277, /* 4.0 */
			0.01848845053267318523077934, /* 4.5 */
			0.01664469118982119216319487, /* 5.0 */
			0.01513497322191737887351255, /* 5.5 */
			0.01387612882307074799874573, /* 6.0 */
			0.01281046524292022692424986, /* 6.5 */
			0.01189670994589177009505572, /* 7.0 */
			0.01110455975820691732662991, /* 7.5 */
			0.010411265261972096497478567, /* 8.0 */
			0.009799416126158803298389475, /* 8.5 */
			0.009255462182712732917728637, /* 9.0 */
			0.008768700134139385462952823, /* 9.5 */
			0.008330563433362871256469318, /* 10.0 */
			0.007934114564314020547248100, /* 10.5 */
			0.007573675487951840794972024, /* 11.0 */
			0.007244554301320383179543912, /* 11.5 */
			0.006942840107209529865664152, /* 12.0 */
			0.006665247032707682442354394, /* 12.5 */
			0.006408994188004207068439631, /* 13.0 */
			0.006171712263039457647532867, /* 13.5 */
			0.005951370112758847735624416, /* 14.0 */
			0.005746216513010115682023589, /* 14.5 */
			0.005554733551962801371038690 /* 15.0 */
	};

	private long getLowerDomain(long n, long m, long k) {
		return FastMath.max(0, m - (n - k));
	}

	private long getUpperDomain(long m, long k) {
		return FastMath.min(k, m);
	}

	private long[] getDomain(long n, long m, long k) {
		return new long[] { getLowerDomain(n, m, k), getUpperDomain(m, k) };
	}

	public double getNumericalMean() {
		return sampleSize * (numberOfSuccesses / (double) populationSize);
	}

	/**
	 * {@inheritDoc}
	 *
	 * For population size {@code N}, number of successes {@code m}, and sample
	 * size {@code n}, the variance is
	 * {@code [n * m * (N - n) * (N - m)] / [N^2 * (N - 1)]}.
	 */
	public double getNumericalVariance() {
		if (!numericalVarianceIsCalculated) {
			numericalVariance = calculateNumericalVariance();
			numericalVarianceIsCalculated = true;
		}
		return numericalVariance;
	}

	private double numericalVariance = Double.NaN;
	private boolean numericalVarianceIsCalculated = false;

	protected double calculateNumericalVariance() {
		final double N = populationSize;
		final double m = numberOfSuccesses;
		final double n = sampleSize;
		return (n * m * (N - n) * (N - m)) / (N * N * (N - 1));
	}

	protected long solveInverseCumulativeProbability(final double p, long lower, long upper) {
		while (lower + 1 < upper) {
			long xm = (lower + upper) / 2;
			if (xm < lower || xm > upper) {
				/*
				 * Overflow.
				 * There will never be an overflow in both calculation methods
				 * for xm at the same time
				 */
				xm = lower + (upper - lower) / 2;
			}

			double pm = checkedCumulativeProbability(xm);
			if (pm >= p) {
				upper = xm;
			} else {
				lower = xm;
			}
		}
		return upper;
	}

}
