package crypto;

import java.math.BigInteger;

public class OPE_Boldyreva_BIGH2PEC_Long implements HypergeometricSampler<Long>{

	static private int extraPrecision = 20;
	
	
	public Long sample(Long KK, Long NN1, Long NN2, OPE_Boldyreva_blockrng prng) throws Exception {

		int precision = (int) (Math.log(NN1 + NN2 + KK)/Math.log(2)) + extraPrecision;
		OPE_Boldyreva_PBigDecimal.setPrecision(precision);

		boolean REJECT = true;
		OPE_Boldyreva_PBigDecimal CON = new OPE_Boldyreva_PBigDecimal(57.56462733);
		OPE_Boldyreva_PBigDecimal DELTAL = new OPE_Boldyreva_PBigDecimal(0.0078);
		OPE_Boldyreva_PBigDecimal DELTAU = new OPE_Boldyreva_PBigDecimal(0.0034);
		OPE_Boldyreva_PBigDecimal SCALE = new OPE_Boldyreva_PBigDecimal(1.0e25);

		// CHECK PARAMETER VALIDITY
		if (NN1  < 0 || NN2 < 0
				|| KK < 0 || KK > NN1 + NN2) {
			throw new Exception("Invalid parameter values!");
		}

		long TN = NN1 + NN2;
		long N1 = 0;
		long N2 = 0;

		if (NN1 <= NN2) {
			N1 = NN1;
			N2 = NN2;
		} else {
			N1 = NN2;
			N2 = NN1;
		}

		long K = 0;
		if ((KK + KK) >= TN) {
			K = TN - KK;
		} else {

			K = KK;
		}

		// if result of calculation is negative, this code is not compatible
		// with original code
		// but this should never happen
		OPE_Boldyreva_PBigDecimal M = (new OPE_Boldyreva_PBigDecimal((K+1)*(N1+1)))
				.divide(new OPE_Boldyreva_PBigDecimal(TN+2));
		
		long MINJX = 0;
		if (K-N2 > 0) MINJX = K-N2;
		
		long MAXJX = N1;
		if(K < N1) MAXJX = K;
		
				// GENERATE RANDOM VARIATE
		OPE_Boldyreva_PBigDecimal IX;

		if (MINJX == MAXJX) {
			// DEGENERATE DISTRIBUTION...
			IX = new OPE_Boldyreva_PBigDecimal(MAXJX);

		} else {
			if (M.subtract(new OPE_Boldyreva_PBigDecimal(MINJX)).compareTo(new OPE_Boldyreva_PBigDecimal(10)) < 0) {
				// INVERSE TRANSFORMATION...
				OPE_Boldyreva_PBigDecimal W = OPE_Boldyreva_PBigDecimal.ZERO;
				if (K < N2) {
					W = OPE_Boldyreva_PBigDecimal.exp(CON.add(AFC(N2)).add(AFC(N1 + N2 - K)).subtract(AFC(N2 - K))
							.subtract(AFC(N1 + N2)));
				} else {
					W = OPE_Boldyreva_PBigDecimal.exp(CON.add(AFC(N1)).add(AFC(K)).subtract(AFC(K - N2))
							.subtract(AFC(N1 + N2)));
				}

				// ugly original code with gotos translated to java
				OPE_Boldyreva_PBigDecimal P = W;
				IX = new OPE_Boldyreva_PBigDecimal(MINJX);
				OPE_Boldyreva_PBigDecimal U = RAND(prng, precision).multiply(SCALE);

				while (U.compareTo(P) > 0) {

					U = U.subtract(P);
					P = P.multiply((new OPE_Boldyreva_PBigDecimal(N1)).subtract(IX)).multiply((new OPE_Boldyreva_PBigDecimal(K)).subtract(IX));
					IX = IX.add(OPE_Boldyreva_PBigDecimal.ONE);
					P = P.divide(IX).divide((new OPE_Boldyreva_PBigDecimal(N2 - K)).add(IX));

					if (IX.compareTo(new OPE_Boldyreva_PBigDecimal(MAXJX)) > 0) {
						P = W;
						IX = new OPE_Boldyreva_PBigDecimal(MINJX);
						U = RAND(prng, precision).multiply(SCALE);
					}
				}
			}

			else {
				// H2PE

				OPE_Boldyreva_PBigDecimal S = OPE_Boldyreva_PBigDecimal.sqrt(new OPE_Boldyreva_PBigDecimal((TN - K) * K * N1 * N2
						/ (TN - 1) / TN / TN));

				// REMARK: D IS DEFINED IN REFERENCE WITHOUT INT.
				// THE TRUNCATION CENTERS THE CELL BOUNDARIES AT 0.5
				OPE_Boldyreva_PBigDecimal D = new OPE_Boldyreva_PBigDecimal((S.multiply(new OPE_Boldyreva_PBigDecimal(1.5))).toBigInteger()).add(new OPE_Boldyreva_PBigDecimal(
						0.5));
				OPE_Boldyreva_PBigDecimal XL = M.subtract(D).add(new OPE_Boldyreva_PBigDecimal(0.5));
				OPE_Boldyreva_PBigDecimal XR = M.add(D).add(new OPE_Boldyreva_PBigDecimal(0.5));
				OPE_Boldyreva_PBigDecimal A = AFC(M).add(AFC((new OPE_Boldyreva_PBigDecimal(N1)).subtract(M)))
						.add(AFC((new OPE_Boldyreva_PBigDecimal(K)).subtract(M))).add(AFC(new OPE_Boldyreva_PBigDecimal(N2 - K)).add(M));
				//System.out.println(A.subtract(AFC(XL)).subtract(AFC((new OPE_Boldyreva_PBigDecimal(N1)).subtract(XL)))
				//		.subtract(AFC((new OPE_Boldyreva_PBigDecimal(K)).subtract(XL)))
				//		.subtract(AFC((new OPE_Boldyreva_PBigDecimal(N2.subtract(K))).add(XL))));
				OPE_Boldyreva_PBigDecimal KL = OPE_Boldyreva_PBigDecimal.exp(A.subtract(AFC(XL)).subtract(AFC((new OPE_Boldyreva_PBigDecimal(N1)).subtract(XL)))
						.subtract(AFC((new OPE_Boldyreva_PBigDecimal(K)).subtract(XL)))
						.subtract(AFC((new OPE_Boldyreva_PBigDecimal(N2 - K)).add(XL))));
				OPE_Boldyreva_PBigDecimal KR = OPE_Boldyreva_PBigDecimal.exp(A.subtract(AFC(XR.subtract(OPE_Boldyreva_PBigDecimal.ONE)))
						.subtract(AFC((new OPE_Boldyreva_PBigDecimal(N1)).subtract(XR).add(OPE_Boldyreva_PBigDecimal.ONE)))
						.subtract(AFC((new OPE_Boldyreva_PBigDecimal(K)).subtract(XR).add(OPE_Boldyreva_PBigDecimal.ONE)))
						.subtract(AFC((new OPE_Boldyreva_PBigDecimal(N2 - K)).add(XR).subtract(OPE_Boldyreva_PBigDecimal.ONE))));
				OPE_Boldyreva_PBigDecimal LAMDL = OPE_Boldyreva_PBigDecimal.log(
						XL.multiply(((new OPE_Boldyreva_PBigDecimal(N2 - K)).add(XL)))
								.divide(((new OPE_Boldyreva_PBigDecimal(N1)).subtract(XL).add(OPE_Boldyreva_PBigDecimal.ONE)))
								.divide(((new OPE_Boldyreva_PBigDecimal(K)).subtract(XL).add(OPE_Boldyreva_PBigDecimal.ONE)))).negate();

				OPE_Boldyreva_PBigDecimal LAMDR = OPE_Boldyreva_PBigDecimal.log(
						((new OPE_Boldyreva_PBigDecimal(N1)).subtract(XR).add(OPE_Boldyreva_PBigDecimal.ONE))
								.multiply(((new OPE_Boldyreva_PBigDecimal(K)).subtract(XR).add(OPE_Boldyreva_PBigDecimal.ONE))).divide(XR)
								.divide(((new OPE_Boldyreva_PBigDecimal(N2 - K)).add(XR)))).negate();

				OPE_Boldyreva_PBigDecimal P1 = D.add(D);
				OPE_Boldyreva_PBigDecimal P2 = P1.add(KL.divide(LAMDL));
				OPE_Boldyreva_PBigDecimal P3 = P2.add(KR.divide(LAMDR));

				// label 30
				do {

					boolean accept = false;
					OPE_Boldyreva_PBigDecimal V;

					do {
						OPE_Boldyreva_PBigDecimal U = RAND(prng, precision).multiply(P3);
						V = RAND(prng, precision);
						if (U.compareTo(P1) < 0) {
							// RECTANGULAR REGION...
							IX = XL.add(U);
							accept = true;
						} else {
							if (U.compareTo(P2) <= 0) {
								// LEFT TAIL...
								IX = XL.add(OPE_Boldyreva_PBigDecimal.log(V).divide(LAMDL));
								if (IX.compareTo(new OPE_Boldyreva_PBigDecimal(MINJX)) < 0) {
									continue;
								} else {
									accept = true;
									V = V.multiply(U.subtract(P1)).multiply(LAMDL);
								}
							} else {
								// RIGHT TAIL...
								IX = XR.subtract(OPE_Boldyreva_PBigDecimal.log(V).divide(LAMDR));
								if (IX.compareTo(new OPE_Boldyreva_PBigDecimal(MAXJX)) > 0) {
									continue;
								} else {
									accept = true;
									V = V.multiply(U.subtract(P2)).multiply(LAMDR);
								}
							}
						}
					} while (!accept);

					// ACCEPTANCE/REJECTION TEST...

					if (M.compareTo(new OPE_Boldyreva_PBigDecimal(100)) < 0 || IX.compareTo(new OPE_Boldyreva_PBigDecimal(50)) <= 0) {

						// EXPLICIT EVALUATION...

						OPE_Boldyreva_PBigDecimal F = OPE_Boldyreva_PBigDecimal.ONE;
						if (M.compareTo(IX) < 0) {
							// label 40
							// hgd.cc does only iterate until IX-1
							// but Fortran specification seems to be clear about
							// it
							for (OPE_Boldyreva_PBigDecimal I = M.add(OPE_Boldyreva_PBigDecimal.ONE); I.compareTo(IX) <= 0; I = I
									.add(OPE_Boldyreva_PBigDecimal.ONE)) {
								F = F.multiply((new OPE_Boldyreva_PBigDecimal(N1)).subtract(I).add(OPE_Boldyreva_PBigDecimal.ONE))
										.multiply((new OPE_Boldyreva_PBigDecimal(K)).subtract(I).add(OPE_Boldyreva_PBigDecimal.ONE))
										.divide((new OPE_Boldyreva_PBigDecimal(N2 - K)).add(I)).divide(I);
							}
						} else {
							if (M.compareTo(IX) > 0) {
								// label 50
								// hgd.cc does only iterate until M-1 but
								// Fortran specification seems to be clear
								// about it
								for (OPE_Boldyreva_PBigDecimal I = IX.add(OPE_Boldyreva_PBigDecimal.ONE); I.compareTo(M) <= 0; I = I
										.add(OPE_Boldyreva_PBigDecimal.ONE)) {
									F = F.multiply(I).multiply((new OPE_Boldyreva_PBigDecimal(N2 - K)).add(I))
											.divide((new OPE_Boldyreva_PBigDecimal(N1)).subtract(I))
											.divide((new OPE_Boldyreva_PBigDecimal(K)).subtract(I));
								}
							}
						}

						if (V.compareTo(F) <= 0) {
							REJECT = false;
						}
					} else {

						// SQUEEZE USING UPPER AND LOWER BOUNDS...
						OPE_Boldyreva_PBigDecimal Y = IX;
						OPE_Boldyreva_PBigDecimal Y1 = Y.add(OPE_Boldyreva_PBigDecimal.ONE);
						OPE_Boldyreva_PBigDecimal YM = Y.subtract(M);
						OPE_Boldyreva_PBigDecimal YN = (new OPE_Boldyreva_PBigDecimal(N1)).subtract(Y).add(OPE_Boldyreva_PBigDecimal.ONE);
						OPE_Boldyreva_PBigDecimal YK = (new OPE_Boldyreva_PBigDecimal(K)).subtract(Y).add(OPE_Boldyreva_PBigDecimal.ONE);
						OPE_Boldyreva_PBigDecimal NK = (new OPE_Boldyreva_PBigDecimal(N2 - K)).add(Y1);
						OPE_Boldyreva_PBigDecimal R = YM.divide(Y1).negate();
						S = YM.divide(YN);
						OPE_Boldyreva_PBigDecimal T = YM.divide(YK);
						OPE_Boldyreva_PBigDecimal E = YM.divide(NK).negate();
						OPE_Boldyreva_PBigDecimal G = YN.multiply(YK).divide(Y1.multiply(NK)).subtract(OPE_Boldyreva_PBigDecimal.ONE);
						OPE_Boldyreva_PBigDecimal DG = OPE_Boldyreva_PBigDecimal.ONE;

						if (G.compareTo(OPE_Boldyreva_PBigDecimal.ZERO) < 0) {
							DG = OPE_Boldyreva_PBigDecimal.ONE.add(G);
						}

						OPE_Boldyreva_PBigDecimal GU = G.multiply(OPE_Boldyreva_PBigDecimal.ONE.add(G.multiply((new OPE_Boldyreva_PBigDecimal(-0.5)).add(G
								.divide(new OPE_Boldyreva_PBigDecimal(3))))));
						OPE_Boldyreva_PBigDecimal GL = GU.subtract((new OPE_Boldyreva_PBigDecimal(0.25)).multiply(OPE_Boldyreva_PBigDecimal.pow((G.multiply(G)),
								new OPE_Boldyreva_PBigDecimal(2)).divide(DG)));
						OPE_Boldyreva_PBigDecimal XM = M.add(new OPE_Boldyreva_PBigDecimal(0.5));
						OPE_Boldyreva_PBigDecimal XN = (new OPE_Boldyreva_PBigDecimal(N1)).subtract(M).add(new OPE_Boldyreva_PBigDecimal(0.5));
						OPE_Boldyreva_PBigDecimal XK = (new OPE_Boldyreva_PBigDecimal(K)).subtract(M).add(new OPE_Boldyreva_PBigDecimal(0.5));
						OPE_Boldyreva_PBigDecimal NM = (new OPE_Boldyreva_PBigDecimal(N2 - K)).add(XM);
						OPE_Boldyreva_PBigDecimal UB = Y
								.multiply(GU)
								.subtract(M.multiply(GL))
								.add(DELTAU)
								.add(XM.multiply(R).multiply(
										OPE_Boldyreva_PBigDecimal.ONE.add(R.multiply((new OPE_Boldyreva_PBigDecimal(-0.5)).add(R
												.divide(new OPE_Boldyreva_PBigDecimal(3)))))))
								.add(XN.multiply(S).multiply(
										OPE_Boldyreva_PBigDecimal.ONE.add(S.multiply((new OPE_Boldyreva_PBigDecimal(-0.5)).add(S
												.divide(new OPE_Boldyreva_PBigDecimal(3)))))))
								.add(XK.multiply(T).multiply(
										OPE_Boldyreva_PBigDecimal.ONE.add(T.multiply((new OPE_Boldyreva_PBigDecimal(-0.5)).add(T
												.divide(new OPE_Boldyreva_PBigDecimal(3)))))))
								.add(NM.multiply(E).multiply(
										OPE_Boldyreva_PBigDecimal.ONE.add(E.multiply((new OPE_Boldyreva_PBigDecimal(-0.5)).add(E
												.divide(new OPE_Boldyreva_PBigDecimal(3)))))));

						// TEST AGAINST UPPER BOUND...
						OPE_Boldyreva_PBigDecimal ALV = OPE_Boldyreva_PBigDecimal.log(V);
						if (ALV.compareTo(UB) > 0) {
							REJECT = true;
						} else {
							// TEST AGAINST LOWER BOUND...

							OPE_Boldyreva_PBigDecimal DR = XM.multiply(R.pow(4));
							if (R.compareTo(OPE_Boldyreva_PBigDecimal.ZERO) < 0) {
								DR = DR.divide(OPE_Boldyreva_PBigDecimal.ONE.add(R));
							}
							OPE_Boldyreva_PBigDecimal DS = XN.multiply(S.pow(4));
							if (S.compareTo(OPE_Boldyreva_PBigDecimal.ZERO) < 0) {
								DS = DS.divide(OPE_Boldyreva_PBigDecimal.ONE.add(S));
							}
							OPE_Boldyreva_PBigDecimal DT = XK.multiply(T.pow(4));
							if (T.compareTo(OPE_Boldyreva_PBigDecimal.ZERO) < 0) {
								DT = DT.divide(OPE_Boldyreva_PBigDecimal.ONE.add(T));
							}
							OPE_Boldyreva_PBigDecimal DE = NM.multiply(E.pow(4));
							if (E.compareTo(OPE_Boldyreva_PBigDecimal.ZERO) < 0) {
								DE = DE.divide(OPE_Boldyreva_PBigDecimal.ONE.add(E));
							}

							if (ALV.compareTo(UB.subtract((new OPE_Boldyreva_PBigDecimal(0.25)).multiply(DR.add(DS).add(DT).add(DE)))
									.add(Y.add(M).multiply(GL.subtract(GU))).subtract(DELTAL)) < 0) {
								REJECT = false;
							} else {
								// STIRLING'S FORMULA TO MACHINE ACCURACY...
								if (ALV.compareTo(A.subtract(AFC(IX)).subtract(AFC((new OPE_Boldyreva_PBigDecimal(N1)).subtract(IX)))
										.subtract(AFC((new OPE_Boldyreva_PBigDecimal(K)).subtract(IX)))
										.subtract(AFC((new OPE_Boldyreva_PBigDecimal(N2 - K)).add(IX)))) <= 0) {
									REJECT = false;
								} else {
									REJECT = true;
								}
							}
						}
					}
				} while (REJECT);

			}
		}

		// RETURN APPROPRIATE VARIATE

		if (KK + KK >= TN) {
			if (NN1 > NN2) {
				IX = (new OPE_Boldyreva_PBigDecimal(KK - NN2)).add(IX);
			} else {
				IX = (new OPE_Boldyreva_PBigDecimal(NN1)).subtract(IX);
			}
		} else {
			if (NN1 > NN2) {
				IX = (new OPE_Boldyreva_PBigDecimal(KK)).subtract(IX);
			}
		}

		return IX.longValue();
	}

	/*
	 * FUNCTION TO EVALUATE LOGARITHM OF THE FACTORIAL I IF (I .GT. 7), USE
	 * STIRLING'S APPROXIMATION OTHERWISE, USE TABLE LOOKUP
	 */

	private static OPE_Boldyreva_PBigDecimal AFC(OPE_Boldyreva_PBigDecimal I) {
		double[] AL = { 0.0, 0.0, 0.6931471806, 1.791759469, 3.178053830, 4.787491743, 6.579251212, 8.525161361 };
		OPE_Boldyreva_PBigDecimal result;

		if (I.compareTo(OPE_Boldyreva_PBigDecimal.valueOf(7)) <= 0) {
			result = new OPE_Boldyreva_PBigDecimal(AL[I.add(new OPE_Boldyreva_PBigDecimal(0.5)).intValue()]);
		} else {
			result = (I.add(new OPE_Boldyreva_PBigDecimal(0.5))).multiply(OPE_Boldyreva_PBigDecimal.log(I)).subtract(I)
					.add((new OPE_Boldyreva_PBigDecimal(0.08333333333333)).divide(I))
					.add((new OPE_Boldyreva_PBigDecimal(-0.00277777777777)).divide(I).divide(I).divide(I))
					.add(new OPE_Boldyreva_PBigDecimal(0.9189385332));
		}
		return result;
	}

	private static OPE_Boldyreva_PBigDecimal AFC(long I) {
		return AFC(new OPE_Boldyreva_PBigDecimal(I));
	}

	/*
	 * UNIFORM RANDOM NUMBER GENERATOR REFERENCE: L. SCHRAGE,
	 * "A MORE PORTABLE FORTRAN RANDOM NUMBER GENERATOR," ACM TRANSACTIONS ON
	 * MATHEMATICAL SOFTWARE, 5(1979), 132-138.
	 */
	// private static double RAND() {
	// long XHI, XALO, LEFTLO, FHI, K;
	// long A = 16807;
	// long B15 = 32768;
	// long B16 = 65536;
	// long P = 2147483647;
	//
	// XHI = ISEED / B16;
	// XALO = (ISEED - XHI * B16) * A;
	// LEFTLO = XALO / B16;
	// FHI = XHI * A + LEFTLO;
	// K = FHI / B15;
	// ISEED = (((XALO - LEFTLO * B16) - P) + (FHI - K * B15) * B16) + K;
	// if (ISEED < 0) {
	// ISEED = ISEED + P;
	// }
	// double result = ISEED * 4.656612875E-10;
	// return result;
	// }

	private static OPE_Boldyreva_PBigDecimal RAND(OPE_Boldyreva_blockrng prng, int precision) {
		BigInteger div = BigInteger.valueOf(2).pow(precision);
		BigInteger rzz = prng.rand_zz_mod(div);
		return new OPE_Boldyreva_PBigDecimal(rzz).divide(new OPE_Boldyreva_PBigDecimal(div));
	}

	

}