package crypto;
import java.security.SecureRandom;

/**
 * Implements the pseudorandom generator, in the SWP scheme referred to as G
 * Can't be static like the PRF, because SWP requires instances with a certain fixed seed
 * @author Tim Waage
 *
 */
public class PRG {
	
	// underlying random number generator
	private SecureRandom sr; 
	
	
	
	/**
	 * Constructor for random seed
	 * seed automatically chosen at random by not specifying it 
	 * (see http://www.cigital.com/justice-league-blog/2009/08/14/proper-use-of-javas-securerandom/)
	 */
	public PRG() {
		try {
			sr = SecureRandom.getInstance("SHA1PRNG"/*, "BC"*/);
		}
		catch (Exception e) {
			System.out.println("An error occured during the initialization of the pseudorandom generator");
		}	
	}
	
	
		
	/**
	 * Constructor for certain fixed seed
	 * @param w length of the random generated output (in Bytes)
	 * @param seed seed to be used for initialization
	 */
	public PRG(long seed) {
		
		this();	
		sr.setSeed(seed);
	}
	
	
	
	/**
	 * Generates random bytes
	 * @return length of random bytes array to be returned
	 */
	public byte[] generateRandomBytes(int outputLength) {
		
		byte[] result = new byte[outputLength];
		sr.nextBytes(result);
		
		return result;
	}
	
	
	
	/**
	 * Generates random printable bytes (for naming tables, columns, etc...)
	 * @return length of random bytes array to be returned
	 */
	public byte[] generateRandomPrintableBytes(int outputLength) {
		
		byte[] result = new byte[outputLength];
		
		int i = 0;
		byte[] testbyte = new byte[1];
		
		while(i<outputLength) {
			sr.nextBytes(testbyte);
			if(/*(testbyte[0] > 64)&&(testbyte[0] < 91)||*/  //Großbuchstaben evtl. problematisch bei Tabellen/Spaltennamen
			   (testbyte[0] > 96)&&(testbyte[0] < 123)||
			   (testbyte[0] > 47)&&(testbyte[0] < 58)) {
				result[i] = testbyte[0];
				i++;
			}
		}
				
		return result;
	}
	
	
	
	/**
	 * Generates random printable bytes where the first byte is no number (for usage as table/column names in databases)
	 * @param outputLength length of random bytes array to be returned
	 * @return random printable bytes
	 */
	public byte[] generateRandomTableOrColumnName(int outputLength) {
		
		byte[] result = generateRandomPrintableBytes(outputLength);
		
		while((result[0] > 47)&&(result[0] < 58)) result = generateRandomPrintableBytes(outputLength);
		
		return result;
	}
	
	
	
	/**
	 * generates a random int number within the interval [minValue, maxValue]
	 * @param minValue the minimal result
	 * @param maxValue the maximal result
	 * @return a random number within the interval [minValue, maxValue]
	 */
	public int generateRandomInt(int minValue, int maxValue) {
		
		return sr.nextInt(maxValue - minValue + 1) + minValue;
	}
	
	
	
	/**
	 * generates a random long number within the interval [minValue, maxValue]
	 * @param minValue the minimal result
	 * @param maxValue the maximal result
	 * @return a random number within the interval [minValue, maxValue]
	 */
	public long generateRandomLong(long minValue, long maxValue) {
		
		return minValue + ((long)(sr.nextDouble()*(maxValue - minValue + 1)));
	}
}