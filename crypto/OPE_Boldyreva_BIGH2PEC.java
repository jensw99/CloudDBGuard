package crypto;

import java.math.BigInteger;

public class OPE_Boldyreva_BIGH2PEC implements HypergeometricSampler<BigInteger>{

	static private int extraPrecision = 20;
	
	
	/*
	 * 
	 * HYPERGEOMETRIC RANDOM VARIATE GENERATOR
	 * 
	 * METHOD IF (MODE - MAX(0,KK-NN2) .LT. 10), USE THE INVERSE CDF. OTHERWISE,
	 * USE ALGORITHM H2PE: ACCEPTANCE-REJECTION VIA THREE REGION COMPOSITION.
	 * THE THREE REGIONS ARE A RECTANGLE, AND EXPONENTIAL LEFT AND RIGHT TAILS.
	 * H2PE REFERS TO HYPERGEOMETRIC-2 POINTS-EXPONENTIAL TAILS.
	 * H2PEC REFERS TO H2PE AND "COMBINED."
	 * THUS H2PE IS THE RESEARCH RESULT AND H2PEC IS THE IMPLEMENTATION OF A
	 * COMPLETE USABLE ALGORITHM.
	 * 
	 * REFERENCE VORATAS KACHITVICHYANUKUL AND BRUCE SCHMEISER,
	 * 
	 * "COMPUTER GENERATION OF HYPERGEOMETRIC RANDOM VARIATES," JOURNAL OF
	 * STATISTICAL COMPUTATION AND SIMULATION, 22(1985), 2, 1985, 127-145.
	 * 
	 * REQUIRED SUBPROGRAMS
	 * AFC() : A DOUBLE-PRECISION FUNCTION TO EVALUATE THE LOGARITHM OF THE
	 * FACTORIAL.
	 * RAND(): A UNIFORM (0,1) RANDOM NUMBER GENERATOR.
	 * 
	 * ARGUMENTS
	 * NN1 : NUMBER OF WHITE BALLS (INPUT)
	 * NN2 : NUMBER OF BLACK BALLS (INPUT)
	 * KK : NUMBER OF BALLS TO BE DRAWN (INPUT)
	 * ISEED : RANDOM NUMBER SEED (INPUT AND OUTPUT)
	 * IX : NUMBER OF WHITE BALLS DRAWN (OUTPUT)
	 * 
	 * STRUCTURAL VARIABLES
	 * REJECT: LOGICAL FLAG TO REJECT THE VARIATE GENERATE BY H2PE.
	 * IX : INTEGER CANDIDATE VALUE.
	 * M : DISTRIBUTION MODE.
	 * MINJX : DISTRIBUTION LOWER BOUND.
	 * MAXJX : DISTRIBUTION UPPER BOUND.
	 * K,N1,N2: ALTERNATE VARIABLES FOR KK, NN1, AND NN2 (ALWAYS (N1 .LE. N2)
	 * AND (K .LE. (N1+N2)/2)).
	 * TN : TOTAL NUMBER OF WHITE AND BLACK BALLS
	 * 
	 * INVERSE-TRANSFORMATION VARIABLES
	 * CON : NATURAL LOGARITHM OF SCALE.
	 * P : CURRENT SCALED PROBABILITY FOR THE INVERSE CDF.
	 * SCALE : A BIG CONSTANT (1.E25) USED TO SCALE THE PROBABILITY TO AVOID
	 * NUMERICAL UNDERFLOW
	 * U : THE UNIFORM VARIATE BETWEEN (0, 1.E25).
	 * W : SCALED HYPERGEOMETRIC PROBABILITY OF MINJX.
	 * 
	 * H2PE VARIABLES
	 * S : DISTRIBUTION STANDARD DEVIATION.
	 * D : HALF THE AREA OF THE RECTANGLE.
	 * XL : LEFT END OF THE RECTANGLE.
	 * XR : RIGHT END OF THE RECTANGLE.
	 * A : A SCALING CONSTANT.
	 * KL : HIGHEST POINT OF THE LEFT-TAIL REGION.
	 * KR : HIGHEST POINT OF THE RIGHT-TAIL REGION.
	 * LAMDL : RATE FOR THE LEFT EXPONENTIAL TAIL.
	 * LAMDR : RATE FOR THE RIGHT EXPONENTIAL TAIL.
	 * P1 : AREA OF THE RECTANGLE.
	 * P2 : AREA OF THE LEFT EXPONENTIAL TAIL PLUS P1.
	 * P3 : AREA OF THE RIGHT EXPONENTIAL TAIL PLUS P2.
	 * U : A UNIFORM (0,P3) RANDOM VARIATE USED FIRST TO SELECT ONE OF THE THREE
	 * REGIONS AND THEN CONDITIONALLY TO GENERATE A VALUE FROM THE REGION.
	 * V : U(0,1) RANDOM NUMBER USED TO GENERATE THE RANDOM VALUE OR TO ACCEPT
	 * OR REJECT THE CANDIDATE VALUE.
	 * F : THE HEIGHT OF THE SCALED DENSITY FUNCTION USED IN THE ACCEPT/REJECT
	 * DECISION WHEN BOTH M AND IX ARE SMALL.
	 * I : INDEX FOR EXPLICIT CALCULATION OF F FOR H2PE.
	 * 
	 * THE FOLLOWING VARIABLES ARE TEMPORARY VARIABLES USED IN COMPUTING THE
	 * UPPER AND LOWER BOUNDS OF THE NATURAL LOGARITHM OF THE SCALED DENSITY.
	 * THE DETAILED DESCRIPTION IS GIVEN IN PROPOSITIONS 2 AND 3 OF THE APPENDIX
	 * IN THE REFERENCE.
	 * Y, Y1, YM, YN, YK, NK, R, S, T, E, G, DG, GU, GL, XM,, XN, XK, NM
	 * 
	 * Y : PRELIMINARY CONTINUOUS CANDIDATE VALUE, FLOAT(IX)
	 * UB : UPPER BOUND FOR THE NATURAL LOGARITHM OF THE SCALED DENSITY.
	 * ALV : NATURAL LOGARITHM OF THE ACCEPT/REJECT VARIATE V.
	 * DR, DS, DT, DE: ONE OF MANY TERMS SUBTRACTED FROM THE UPPER BOUND TO
	 * OBTAIN THE LOWER BOUND ON THE NATURAL LOGARITHM OF THE SCALED DENSITY.
	 * DELTAU: A CONSTANT, THE VALUE 0.0034 IS OBTAINED BY SETTING N1 = N2 =
	 * 200, K = 199, M = 100, AND Y = 50 IN THE FUNCTION DELTA_U IN LEMMA 1 AND
	 * ROUNDING THE VALUE TO FOUR DECIMAL PLACES.
	 * DELTAL: A CONSTANT, THE VALUE 0.0078 IS OBTAINED BY SETTING N1 = N2 =
	 * 200, K = 199, M = 100, AND Y = 50 IN THE FUNCTION DELTA_L IN LEMMA 1 AND
	 * ROUNDING THE VALUE TO FOUR DECIMAL PLACES.
	 */
	public BigInteger sample(BigInteger KK, BigInteger NN1, BigInteger NN2, OPE_Boldyreva_blockrng prng) throws Exception {

		int precision = (int) NN1.add(NN2).add(KK).bitLength() + extraPrecision;
		OPE_Boldyreva_PBigDecimal.setPrecision(precision);

		boolean REJECT = true;
		OPE_Boldyreva_PBigDecimal CON = new OPE_Boldyreva_PBigDecimal(57.56462733);
		OPE_Boldyreva_PBigDecimal DELTAL = new OPE_Boldyreva_PBigDecimal(0.0078);
		OPE_Boldyreva_PBigDecimal DELTAU = new OPE_Boldyreva_PBigDecimal(0.0034);
		OPE_Boldyreva_PBigDecimal SCALE = new OPE_Boldyreva_PBigDecimal(1.0e25);

		// CHECK PARAMETER VALIDITY
		if (NN1.compareTo(BigInteger.ZERO) < 0 || NN2.compareTo(BigInteger.ZERO) < 0
				|| KK.compareTo(BigInteger.ZERO) < 0 || KK.compareTo(NN1.add(NN2)) > 0) {
			throw new Exception("Invalid parameter values!");
		}

		BigInteger TN = NN1.add(NN2);
		BigInteger N1 = BigInteger.ZERO;
		BigInteger N2 = BigInteger.ZERO;

		if (NN1.compareTo(NN2) <= 0) {
			N1 = NN1;
			N2 = NN2;
		} else {
			N1 = NN2;
			N2 = NN1;
		}

		BigInteger K = BigInteger.ZERO;
		if (KK.add(KK).compareTo(TN) >= 0) {
			K = TN.subtract(KK);
		} else {

			K = KK;
		}

		// if result of calculation is negative, this code is not compatible
		// with original code
		// but this should never happen
		OPE_Boldyreva_PBigDecimal M = (new OPE_Boldyreva_PBigDecimal(K.add(BigInteger.ONE).multiply(N1.add(BigInteger.ONE))))
				.divide(new OPE_Boldyreva_PBigDecimal(TN.add(BigInteger.valueOf(2))));
		BigInteger MINJX = BigInteger.ZERO.max(K.subtract(N2));
		BigInteger MAXJX = N1.min(K);

		// GENERATE RANDOM VARIATE
		OPE_Boldyreva_PBigDecimal IX;

		if (MINJX.compareTo(MAXJX) == 0) {
			// DEGENERATE DISTRIBUTION...
			IX = new OPE_Boldyreva_PBigDecimal(MAXJX);

		} else {
			if (M.subtract(new OPE_Boldyreva_PBigDecimal(MINJX)).compareTo(new OPE_Boldyreva_PBigDecimal(10)) < 0) {
				// INVERSE TRANSFORMATION...
				OPE_Boldyreva_PBigDecimal W = OPE_Boldyreva_PBigDecimal.ZERO;
				if (K.compareTo(N2) < 0) {
					W = OPE_Boldyreva_PBigDecimal.exp(CON.add(AFC(N2)).add(AFC(N1.add(N2).subtract(K))).subtract(AFC(N2.subtract(K)))
							.subtract(AFC(N1.add(N2))));
				} else {
					W = OPE_Boldyreva_PBigDecimal.exp(CON.add(AFC(N1)).add(AFC(K)).subtract(AFC(K.subtract(N2)))
							.subtract(AFC(N1.add(N2))));
				}

				// ugly original code with gotos translated to java
				OPE_Boldyreva_PBigDecimal P = W;
				IX = new OPE_Boldyreva_PBigDecimal(MINJX);
				OPE_Boldyreva_PBigDecimal U = RAND(prng, precision).multiply(SCALE);

				while (U.compareTo(P) > 0) {

					U = U.subtract(P);
					P = P.multiply((new OPE_Boldyreva_PBigDecimal(N1)).subtract(IX)).multiply((new OPE_Boldyreva_PBigDecimal(K)).subtract(IX));
					IX = IX.add(OPE_Boldyreva_PBigDecimal.ONE);
					P = P.divide(IX).divide((new OPE_Boldyreva_PBigDecimal(N2.subtract(K))).add(IX));

					if (IX.compareTo(new OPE_Boldyreva_PBigDecimal(MAXJX)) > 0) {
						P = W;
						IX = new OPE_Boldyreva_PBigDecimal(MINJX);
						U = RAND(prng, precision).multiply(SCALE);
					}
				}
			}

			else {
				// H2PE

				OPE_Boldyreva_PBigDecimal S = OPE_Boldyreva_PBigDecimal.sqrt(new OPE_Boldyreva_PBigDecimal((TN.subtract(K)).multiply(K).multiply(N1).multiply(N2)
						.divide(TN.subtract(BigInteger.ONE)).divide(TN).divide(TN)));

				// REMARK: D IS DEFINED IN REFERENCE WITHOUT INT.
				// THE TRUNCATION CENTERS THE CELL BOUNDARIES AT 0.5
				OPE_Boldyreva_PBigDecimal D = new OPE_Boldyreva_PBigDecimal((S.multiply(new OPE_Boldyreva_PBigDecimal(1.5))).toBigInteger()).add(new OPE_Boldyreva_PBigDecimal(
						0.5));
				OPE_Boldyreva_PBigDecimal XL = M.subtract(D).add(new OPE_Boldyreva_PBigDecimal(0.5));
				OPE_Boldyreva_PBigDecimal XR = M.add(D).add(new OPE_Boldyreva_PBigDecimal(0.5));
				OPE_Boldyreva_PBigDecimal A = AFC(M).add(AFC((new OPE_Boldyreva_PBigDecimal(N1)).subtract(M)))
						.add(AFC((new OPE_Boldyreva_PBigDecimal(K)).subtract(M))).add(AFC(new OPE_Boldyreva_PBigDecimal(N2.subtract(K))).add(M));
				//System.out.println(A.subtract(AFC(XL)).subtract(AFC((new OPE_Boldyreva_PBigDecimal(N1)).subtract(XL)))
				//		.subtract(AFC((new OPE_Boldyreva_PBigDecimal(K)).subtract(XL)))
				//		.subtract(AFC((new OPE_Boldyreva_PBigDecimal(N2.subtract(K))).add(XL))));
				OPE_Boldyreva_PBigDecimal KL = OPE_Boldyreva_PBigDecimal.exp(A.subtract(AFC(XL)).subtract(AFC((new OPE_Boldyreva_PBigDecimal(N1)).subtract(XL)))
						.subtract(AFC((new OPE_Boldyreva_PBigDecimal(K)).subtract(XL)))
						.subtract(AFC((new OPE_Boldyreva_PBigDecimal(N2.subtract(K))).add(XL))));
				OPE_Boldyreva_PBigDecimal KR = OPE_Boldyreva_PBigDecimal.exp(A.subtract(AFC(XR.subtract(OPE_Boldyreva_PBigDecimal.ONE)))
						.subtract(AFC((new OPE_Boldyreva_PBigDecimal(N1)).subtract(XR).add(OPE_Boldyreva_PBigDecimal.ONE)))
						.subtract(AFC((new OPE_Boldyreva_PBigDecimal(K)).subtract(XR).add(OPE_Boldyreva_PBigDecimal.ONE)))
						.subtract(AFC((new OPE_Boldyreva_PBigDecimal(N2.subtract(K))).add(XR).subtract(OPE_Boldyreva_PBigDecimal.ONE))));
				OPE_Boldyreva_PBigDecimal LAMDL = OPE_Boldyreva_PBigDecimal.log(
						XL.multiply(((new OPE_Boldyreva_PBigDecimal(N2.subtract(K))).add(XL)))
								.divide(((new OPE_Boldyreva_PBigDecimal(N1)).subtract(XL).add(OPE_Boldyreva_PBigDecimal.ONE)))
								.divide(((new OPE_Boldyreva_PBigDecimal(K)).subtract(XL).add(OPE_Boldyreva_PBigDecimal.ONE)))).negate();

				OPE_Boldyreva_PBigDecimal LAMDR = OPE_Boldyreva_PBigDecimal.log(
						((new OPE_Boldyreva_PBigDecimal(N1)).subtract(XR).add(OPE_Boldyreva_PBigDecimal.ONE))
								.multiply(((new OPE_Boldyreva_PBigDecimal(K)).subtract(XR).add(OPE_Boldyreva_PBigDecimal.ONE))).divide(XR)
								.divide(((new OPE_Boldyreva_PBigDecimal(N2.subtract(K))).add(XR)))).negate();

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
										.divide((new OPE_Boldyreva_PBigDecimal(N2.subtract(K))).add(I)).divide(I);
							}
						} else {
							if (M.compareTo(IX) > 0) {
								// label 50
								// hgd.cc does only iterate until M-1 but
								// Fortran specification seems to be clear
								// about it
								for (OPE_Boldyreva_PBigDecimal I = IX.add(OPE_Boldyreva_PBigDecimal.ONE); I.compareTo(M) <= 0; I = I
										.add(OPE_Boldyreva_PBigDecimal.ONE)) {
									F = F.multiply(I).multiply((new OPE_Boldyreva_PBigDecimal(N2.subtract(K))).add(I))
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
						OPE_Boldyreva_PBigDecimal NK = (new OPE_Boldyreva_PBigDecimal(N2.subtract(K))).add(Y1);
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
						OPE_Boldyreva_PBigDecimal NM = (new OPE_Boldyreva_PBigDecimal(N2.subtract(K))).add(XM);
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
										.subtract(AFC((new OPE_Boldyreva_PBigDecimal(N2.subtract(K))).add(IX)))) <= 0) {
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

		if (KK.add(KK).compareTo(TN) >= 0) {
			if (NN1.compareTo(NN2) > 0) {
				IX = (new OPE_Boldyreva_PBigDecimal(KK.subtract(NN2))).add(IX);
			} else {
				IX = (new OPE_Boldyreva_PBigDecimal(NN1)).subtract(IX);
			}
		} else {
			if (NN1.compareTo(NN2) > 0) {
				IX = (new OPE_Boldyreva_PBigDecimal(KK)).subtract(IX);
			}
		}

		return IX.toBigInteger();
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

	private static OPE_Boldyreva_PBigDecimal AFC(BigInteger I) {
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