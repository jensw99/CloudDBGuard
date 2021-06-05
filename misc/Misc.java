package misc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
//import java.util.Collections;
//import java.util.HashMap;
import java.util.HashSet;
//import java.util.Map;
import java.util.Set;
//import java.util.StringTokenizer;
import java.util.Vector;
import java.util.ArrayList;

//import org.bouncycastle.crypto.paddings.PKCS7Padding;

import com.datastax.driver.core.utils.Bytes;
//import com.google.common.collect.ImmutableMap;



/**
 * a class for helper functions, that don't fit anywhere else 
 * @author Tim Waage
 *
 */
public class Misc {
	
	
	final protected static char[] hexArray = "0123456789abcdef".toCharArray();
	
	// for long <--> byte[] conversion needed in the HBase Client
	//private static ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);    

	public static byte[] longToBytes(long x) {
		
		ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
	    buf.putLong(0, x);
	     
	    return buf.array();	     
	}

	public static long bytesToLong(byte[] bytes) {
		
		ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
		buf.put(bytes, 0, bytes.length);
		buf.flip();//need flip 
		return buf.getLong();
	}
	
	
	
	
	
	
	/**
	 * converts a string into a long value
	 * @param the input string
	 * @return the long value that represents the string
	 */
	public static BigInteger stringToBigInteger(String s) {
		
		BigInteger result = BigInteger.valueOf(0);
		
		char[] array = s.toCharArray();
		
		for(int i=0; i<array.length; i++) {
			if(i == 32) return result; // make a cut for the sake of runtime
			//result += ((byte)array[i]) * Math.pow(128, i);	
			result = result.add(BigInteger.valueOf(array[i]).multiply(BigInteger.valueOf(128).pow(i)));
		}
		
		return result;
	}
	
	
	/**
	 * Converts a byte[] to Hex-String, used to inserts blobs in CQL
	 * @param bytes
	 * @return
	 */
	public static String bytesToCQLHexString(byte[] bytes) {
	    char[] hexChars = new char[bytes.length * 2];
	    for ( int j = 0; j < bytes.length; j++ ) {
	        int v = bytes[j] & 0xFF;
	        hexChars[j * 2] = hexArray[v >>> 4];
	        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
	    }
	    return "0x" + new String(hexChars);
	}
	
	/**
	 * extracts single words from a String
	 * @param input the input String
	 * @return the set of words obtained from the input string
	 */
	public static String[] getWords(String input) {
		
		return input.split(" |\t|/|=|<|>|\"|!|\n");
	}
	
	
	public static Vector<String> getWords2(String input) {
		
		String[] tmp = input.split(" |\t|/|=|<|>|\"|!|\n");
		Vector<String> results = new Vector<String>();
		
		for(int i=0; i<tmp.length; i++) {
			if(!tmp[i].equals("")) results.add(tmp[i]);
		}
		
		return results;
	}
	
	
	/**
	 * extracts the set of unique words contained in a String
	 * @param input 
	 * @return the set of unique distinct words
	 */
	public static Set<String> getUniqueWords(String input) {
		
		
		Set<String> result = new HashSet<String>();
		
		String[] words = getWords(input);
		for(String word : words) if(word.length() > 0) result.add(word);
		
		return result;
	}
	
//	private static PKCS7Padding pkcs7padder = new PKCS7Padding();

	/**
	 * turns the components of a byte array into a String, only for debugging
	 * @param b the byte array
	 * @return the contents of a byte array as String
	 */
	
	public static String ByteArrayToString(byte[] b) {
		
		String result = "(" + b.length + ")[";
		
		for (int i=0; i<b.length; i++) {
			result += b[i];
			if(i != b.length -1) result += ",";
		}
		
		return result + "]";
		
		
	}
	
	
	public static String ByteArrayToCharString(byte[] b) {
		
		String result = "";
		
		for (int i=0; i<b.length; i++) {
			result += (char)b[i];
		}
		
		return result;		
	}
	
	
	public static byte[] CharStringToByteArray(String s) {
		
		byte[] result = new byte[s.length()];
		
		for (int i=0; i<s.length(); i++) {
			result[i] += (char)s.charAt(i);
			
		}
		
		return result;		
	}

	
	public static String ByteArrayToHexString(byte[] b) {
		
		String result = "";
		
		for (int i=0; i<b.length; i++) {
			result += String.format("%02X", b[i]);
			if(i != b.length -1) result += ",";
		}
		
		return result;
		
	}
	
	/**
	 * implements PKCS7 padding and pads a the word to length n, expects to be n < word.length()
	 * @param word the word to pad
	 * @param n the final length
	 * @return the padded String
	*/ 
	public static String pkcs7pad(String word, int n) {
	//not very good, but fast
		switch(n - (word.length() % n)) { 
			case 1: return word += "1";
			case 2: return word += "22";
			case 3: return word += "333";
			case 4: return word += "4444";
			case 5: return word += "55555";
			case 6: return word += "666666";
			case 7: return word += "7777777";
			case 8: return word += "88888888";
			case 9: return word += "999999999";
			case 10: return word += "AAAAAAAAAA";
			case 11: return word += "BBBBBBBBBBB";
			case 12: return word += "CCCCCCCCCCCC";
			case 13: return word += "DDDDDDDDDDDDD";
			case 14: return word += "EEEEEEEEEEEEEE";
			case 15: return word += "FFFFFFFFFFFFFFF";
		}
		
		//return word + new String(pkcs7pad(word.getBytes(), n));
		
		//should never happen anyway
		return null;
	}
	
	/*	int diff = n - word.length(); //not nice, but fast
		String padString = "";
		switch(diff) { 
			case 1: padString = "1"; break;
			case 2: padString = "22"; break;
			case 3: padString = "333"; break;
			case 4: padString = "4444"; break;
			case 5: padString = "55555"; break;
			case 6: padString = "666666"; break;
			case 7: padString = "7777777"; break;
			case 8: padString = "88888888"; break;
			case 9: padString = "999999999"; break;
		}
		
		return word += padString;
	}*/
	

	/**
	 * implements PKCS7 padding and pads a the word to length n
	 * @param word the word to pad
	 * @param n the final length
	 * @return the padded String
	 */
	public static byte[] pkcs7pad(byte[] word, int n) {
		
		int diff = n - word.length;
		
		byte[] result = new byte[n];
		
		for(int i=0; i<word.length; i++) result[i] = word[i];
		for(int i=word.length; i<n; i++) result[i] = (byte)(diff+48);
		
		
		/*byte[] result = new byte[n];
		try {
			System.arraycopy(word, 0, result, 0, word.length);
		}
		catch(ArrayIndexOutOfBoundsException e) {
			System.out.println("n: " + n + ", word.length: " + word.length);
			e.printStackTrace();
		}
		
		pkcs7padder.addPadding(result, word.length);
		*/
		return result;
	}
	
	/**
	 * splits a word in to pieces, where every piece is n bytes long, 
	 * the last piece gets padded, if necessary
	 * @param word the original word
	 * @param n the bit length of the individual parts
	 * @return a String Vector containing the n-bit long parts of the original word
	*/ 
	public static Vector<String> splitWord (String word, int n) {
		
        Vector<String> result = new Vector<String>();
		
		int iterations = word.length() / n;
		for(int i=0; i<iterations; i++) {
			
			String tmp = word.substring(i*n, (i+1)*n);
			
			if(tmp.length() != n) System.out.println(tmp  + " " + tmp.length());		
					
			result.add(tmp);
		}
		
		//pad the rest
		
		
		if(word.length() % n != 0) {
			String tmp2 = Misc.pkcs7pad(word.substring(word.length() - (word.length() % n),word.length() ), n);
			if(tmp2.length() != n) System.out.println(tmp2);
			result.add(tmp2);
		}
		return result;
	}
	
	
	/**
	 * splits a word in to pieces, where every piece is n bytes long, 
	 * the last piece gets padded, if necessary
	 * @param word the original word
	 * @param n the bit length of the individual parts
	 * @return a String Vector containing the n-bit long parts of the original word
	 */
	public static Vector<byte[]> splitWord (byte[] word, int n) {
		
		//System.out.println("vorher: " + Misc.ByteArrayToString(word));
		
		Vector<byte[]> result = new Vector<byte[]>();
		
		while(word.length > n) {
			//split as long as word length > n
			
			byte[] newPart = new byte[n];
			for(int i=0; i<n; i++) newPart[i] = word[i];
			result.add(newPart);
			
			
			byte[] next = new byte[word.length - n];
			for (int i=0; i<word.length - n; i++) next[i] = word[i+n];
			word = next;
		}
		//pad the rest
		//TODO: next line buggy, see String splitWord(String...)! (best rewrite the whole method accrordingly, if needed)
		result.add(Misc.pkcs7pad(word, n));
		
		//for (int i=0; i<result.size(); i++) System.out.println("i:" + i + " " + Misc.ByteArrayToString(result.get(i)));
		
		return result;
	}
	
	
	
	/**
	 * get the size of a file or directory
	 * @param inputfile or directory
	 * @return the size of the inputfile or directory
	 */
	public static long getSize(File input) {
		
		long size = 0;
		
		if(input.isDirectory()){
		
			File[] files = input.listFiles();
			if (files != null) {
				for (int i = 0; i < files.length; i++) {
					if (files[i].isDirectory()) {
						size += getSize(files[i]);
					}
					else {
						size += files[i].length(); 
					}
				}
			}
			return size;
		}
		else if(input.isFile()) {
			return input.length();
		}
		
		return 0;
	}
	
	/**
	 * get the number of files in a directory
	 * @param inputfile or directory
	 * @return the number of files in a directory, 1 if it's just a file
	 */
	public static long getFileCount(File input) {
		
		int count = 0;
		
		if(input.isDirectory()){
		
			File[] files = input.listFiles();
			if (files != null) {
				for (int i = 0; i < files.length; i++) {
					if (files[i].isDirectory()) {
						count += getFileCount(files[i]);
					}
					else {
						count += 1; 
					}
				}
			}
			return count;
		}
		else if(input.isFile()) {
			return 1;
		}
		
		return 0;
	}
	
	/**
	 * splits a word in n bit long parts or pads it to n bit
	 * @param word
	 * @return the parts as a vector of Strings
	 */
	public static Vector<String> prepareForEncryptionAsStringVector(String word, int n) {
				
		if(word.length() > n) {
			return Misc.splitWord(word, n);
		}
		else if(word.length() <= n) {
			Vector<String> result = new Vector<String>();
			
			if(word.length() < n) word = Misc.pkcs7pad(word, n);
	
			result.add(word);
			return result;
		}	
		
		//should obviously never happen
		return null;
	}
	
	/**
	 * checks, if the parts of the matches really occur in subsequent positions
	 * for searches with word lenghts > n
	 * @param list list of potential matches
	 * @param parts how many parts has the split search word?
	 * @return the position of validated matches
	 */
	public static HashSet<Integer> checkHashSets(ArrayList<HashSet<Integer>> list, int parts) {
		
		HashSet<Integer> results = new HashSet<Integer>();
				
		//if there's any match at all -> as many elements in partMatches as there are parts of the original word
		if(list.size() == parts) {
			
			HashSet<Integer> firstSet = list.get(0);
			//if(firstSet.size() > 0) System.out.println(firstSet.size());
			for(int pos: firstSet) {
				
				boolean match = true;
				
				for(int i=0; i<parts-1; i++) {
					
					if(!(list.get(i+1).contains(pos+i+1))) {
						match = false;	
						
					} 
					//else System.out.println("-" + (int)(pos+i+1));
				}	
				
				if(match) {
					results.add(pos);
					//System.out.println(parts"");
				}
				
			}				
		}
		
		return results;
	}
	
	
	
	/**
	 * Prints status messages to the console
	 * @param status the status message to print
	 */
	public static void printStatus(String status) {
		
		Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
        System.out.println("[" + sdf.format(cal.getTime()) + "] - " + status);
	}
	
	
	/**
	 * converts a string into a long value
	 * @param the input string
	 * @return the long value that represents the string
	 */
	public static long stringToLong(String s) {
		
		long result = 0;
		
		char[] s_array = s.toCharArray();
//		char[] array = new char[]{0, 0, 0, 0};
		
		if(s.length() > 0) result += s_array[0] * Math.pow(128, 3);
		if(s.length() > 1) result += s_array[1] * Math.pow(128, 2);
		if(s.length() > 2) result += s_array[2] * 128;
		if(s.length() > 3) result += s_array[3];
		
		return result;
	}
	
	
	/**
	 * converts a string into a long hashset value
	 * @param the input string
	 * @return the long hashset value that represents the string
	 */
	public static ArrayList<Long> stringToLongArrayList(String s) {
		
		ArrayList<Long> result = new ArrayList<Long>();
		
		for (int i = 4; i<s.length(); i += 4) {
			result.add(Misc.stringToLong(s.substring(i-4, i)));
		}
		
		if(s.length() % 4 == 1) result.add(Misc.stringToLong(s.substring(s.length()-1)));
		if(s.length() % 4 == 2) result.add(Misc.stringToLong(s.substring(s.length()-2)));
		if(s.length() % 4 == 3) result.add(Misc.stringToLong(s.substring(s.length()-3)));
		
		return result;
	}
	
	/**
	 * converts a long arraylist to a byte array
	 * @param the input long arraylist
	 * @return the byte array value that represents the long arraylist
	 */
	public static byte[] longArrayListToByteArray(ArrayList<Long> longArrayList) {
		byte[] b = new byte[longArrayList.size()*8];
		
		int counter = 0;
		for(long l : longArrayList) {
			byte[] longToBytes = Misc.longToBytes(l);
			for (byte longByte : longToBytes) {
				b[counter] = longByte;
				counter++;
			}
		}
		
		
		return b;
	}
	
	
	
	/**
	 * Converts HashSet<String> to HashSet<ByteBuffer>
	 * @param stringHashSet the input string hashset
	 * @return the given string hashset as bytebuffer hashset
	 */
	public static HashSet<ByteBuffer> StringHashSet2ByteBufferHashSet(HashSet<String> stringHashSet) {
		
		HashSet<ByteBuffer> byteBufferHashSet = new HashSet<ByteBuffer>();
		
		// if nothing comes in, return empty set
		if(stringHashSet == null) return byteBufferHashSet;
				
		for(String s : stringHashSet) byteBufferHashSet.add(ByteBuffer.wrap(s.getBytes()));
		return byteBufferHashSet;
	}
	
	
	
	/**
	 * Converts HashSet<String> to HashSet<byte[]>
	 * @param stringHashSet the input string hashset
	 * @return the given string hashset as bytebuffer hashset
	 */
	public static HashSet<byte[]> StringHashSet2ByteHashSet(Set<String> stringHashSet) {
		
		HashSet<byte[]> byteHashSet = new HashSet<byte[]>();
		
		// if nothing comes in, return empty set
		if(stringHashSet == null) return byteHashSet;
				
		for(String s : stringHashSet) byteHashSet.add(s.getBytes());
		
		return byteHashSet;
	}
	
	
	
	/**
	 * Converts HashSet<byte[]> to HashSet<ByteBuffer>
	 * @param byteHashSet the input string hashset
	 * @return the given string hashset as bytebuffer hashset
	 */
	public static HashSet<ByteBuffer> byteHashSet2ByteBufferHashSet(HashSet<byte[]> byteHashSet) {
		
		HashSet<ByteBuffer> byteBufferHashSet = new HashSet<ByteBuffer>();
		
		// if nothing comes in, return empty set
		if(byteHashSet == null) return byteBufferHashSet;
		
		for(byte[] b : byteHashSet) {
			ByteBuffer tmp = ByteBuffer.wrap(b);
			byteBufferHashSet.add(tmp);
		}
		
		return byteBufferHashSet;
	}
	
	
	
	/**
	 * Converts HashSet<ByteBuffer> to HashSet<byte[]>
	 * @param byteHashSet the input string hashset
	 * @return the given string hashset as bytebuffer hashset
	 */
	public static HashSet<byte[]> byteBufferHashSet2ByteHashSet(Set<ByteBuffer> byteBufferHashSet) {
		
		HashSet<byte[]> byteHashSet = new HashSet<byte[]>();
		
		// if nothing comes in, return empty set
		if(byteBufferHashSet == null) return byteHashSet;
		
		for(ByteBuffer bb : byteBufferHashSet) {
			
			byteHashSet.add(Bytes.getArray(bb));
			
		}
		
		return byteHashSet;
	}
	
	
	
	/**
	 * Converts HashSet<Long> to HashSet<ByteBuffer>
	 * @param longHashSet the input string hashset
	 * @return the given string hashset as bytebuffer hashset
	 */
	public static HashSet<ByteBuffer> LongHashSet2ByteBufferHashSet(HashSet<Long> longHashSet) {
		
		// if nothing comes in, nothing goes out
		if(longHashSet == null) return null;
		
		HashSet<ByteBuffer> byteBufferHashSet = new HashSet<ByteBuffer>();
		for(Long l : longHashSet) byteBufferHashSet.add(ByteBuffer.wrap(longToBytes(l)));
		
		return byteBufferHashSet;
	}
	
	
	
	/**
	 * Converts HashSet<Long> to HashSet<ByteBuffer>
	 * @param longHashSet the input string hashset
	 * @return the given string hashset as bytebuffer hashset
	 */
	public static HashSet<byte[]> LongHashSet2ByteHashSet(Set<Long> longHashSet) {
		
		// if nothing comes in, nothing goes out
		if(longHashSet == null) return null;
		
		HashSet<byte[]> byteHashSet = new HashSet<byte[]>();
		for(Long l : longHashSet) byteHashSet.add(Misc.longToBytes(l));
		
		return byteHashSet;
	}
	
	
	
	/**
	 * brings s string to a given length by padding with spaces or cutting off
	 * @param s the input string
	 * @param length the desired length
	 * @return s with length length
	 */
	public static String makeLength(String s, int length) {
		
		if(s.length() < length) while(s.length() < length - 1) s += " ";
		else s = s.substring(0, length - 1);
		
		return s;
	}
	


	public static byte[] objectToByteArray(Object obj) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(out);
		os.writeObject(obj);
		return out.toByteArray();
	}

	
	public static Object byteArrayToObject(byte[] data) throws IOException, ClassNotFoundException {
		ByteArrayInputStream in = new ByteArrayInputStream(data);
		ObjectInputStream is = new ObjectInputStream(in);
		return is.readObject();
	}
	
	public static Set<String>byteArraySetToStringSet(Set<byte[]> input) {
		
		Set<String> result = new HashSet<String>();
		
		for(byte[] b : input) result.add(Misc.ByteArrayToCharString(b));
		
		return result;
	}
	
	public static Set<Long>byteArraySetToLongSet(Set<byte[]> input) {
		
		Set<Long> result = new HashSet<Long>();
		
		for(byte[] b : input) result.add(Misc.bytesToLong(b));
		
		return result;
	}


}