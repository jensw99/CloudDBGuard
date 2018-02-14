package misc;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.TreeMap;

/**
 * provides methods for accessing the file system, similar to accessing the databases
 * 
 * @author Tim Waage
 *
 */
public class FileSystemHelper {

	/**
	 * Reads a file from the file system and returns its content as String
	 * 
	 * @param inputFilePath the path of the input file
	 * @return the file content as String
	 */
	public static String readFileIntoString(String inputFilePath) {
		
		try {
			// put the whole file in a string, so that it can be pre-processed like mail data
			// (use only for smaller files, not 50MB and more...)
			// TODO: code an alternative for very large files
			return new String(Files.readAllBytes(Paths.get(inputFilePath)), Charset.defaultCharset());
			// alternatively:
			// BufferedReader scanner = new BufferedReader(new FileReader(inputFilePath));
			// while ((line = scanner.readLine()) != null) result += line;
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error reading file " + inputFilePath);
		}
		
		return null;
	}

	
	
	/**
	 * Reads a file from the file system and returns its content as Byte array
	 * 
	 * @param inputFilePath
	 * @return the file content as Byte Array
	 */
	public static byte[] readFileIntoBytes(String inputFilePath) {
		
		try {
			// put the whole file in a string, so that it can be pre-processed like mail data
			// (use only for smaller files, not 50MB and more...)
			// TODO: code an alternative for very large files
			return Files.readAllBytes(Paths.get(inputFilePath));
			// alternatively:
			// BufferedReader scanner = new BufferedReader(new FileReader(inputFilePath));
			// while ((line = scanner.readLine()) != null) result += line;
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error reading file " + inputFilePath);
		}
		
		return null;
	}
	
	
	/**
	 * reads a file into a ByteBuffer
	 * @param inputFilePath path of the input file
	 * @return the ByteBuffer containing the content of the input file
	 */
	public static ByteBuffer readFileIntoByteBuffer(String inputFilePath) {
		
		File inputFile = new File(inputFilePath);
		FileInputStream fis = null;
		
		try {
			fis = new FileInputStream(inputFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println(inputFilePath + " not found!");
		}
		
		byte[] fileAsByteArray = null;
		try {
			fileAsByteArray = new byte[fis.available()];
			fis.read(fileAsByteArray, 0, fileAsByteArray.length);
			fis.close();
		} catch (IOException e) {			
			e.printStackTrace();
		}
				
		return ByteBuffer.wrap(fileAsByteArray);
	}
	
	/**
	 * writes data given as byte[] to the file system
	 * 
	 * @param content the content of the file about to be written
	 * @param outputPath the destination in the file system
	 * @return the number of bytes written to the file system
	 */
	public static int writeFile(byte[] content, String outputPath) {
		
		int bytesWritten = 0;
		
		try {
			DataOutputStream os = new DataOutputStream(new FileOutputStream(outputPath));
						
			os.write(content);	
			os.close();
			
			bytesWritten = content.length;
						
		} catch (Exception e) {
			e.printStackTrace();
			System.out.print("Error outputting file " + outputPath);
		}
		
		return bytesWritten;
	}

	/**
	 * writes data given as byte sequence to the existing file in the file system
	 * 
	 * @param content the content of the file about to be written
	 * @param outputPath the destination in the file system
	 * @param append to file, true to append and false to overwrite if file exists
	 * @return the number of bytes written to the file system
	 */
	public static int writeFile(byte[] content, String outputPath, boolean append) {
		
		int bytesWritten = 0;
		
		try{
    		FileWriter fileWritter;
 
   			fileWritter = new FileWriter(outputPath,true);
    	    
   			fileWritter.write(content+"\n");
   			fileWritter.close();
    	    
    	    bytesWritten = content.length;
			
    	}catch(IOException e){
    		e.printStackTrace();
    	}
		
		return bytesWritten;
	}
	
	/**
	 * writes data given as string to the existing file in the file system
	 * 
	 * @param content the content of the file about to be written
	 * @param outputPath the destination in the file system
	 * @param append to file, true to append and false to overwrite if file exists
	 * @return the number of bytes written to the file system
	 */
	public static int writeFile(String content, String outputPath, boolean append) {
		
		int bytesWritten = 0;
		
		try{
    		FileWriter fileWritter;
 
   			fileWritter = new FileWriter(outputPath,append);
    	    
   			fileWritter.write(content+"\n");
   			fileWritter.close();
    	    
    	    bytesWritten = content.length();
			
    	}catch(IOException e){
    		e.printStackTrace();
    	}
		
		return bytesWritten;
	}
	
	/**
	 * reads a hashtable from a file
	 * @param path location of the hashtable
	 * @return the hashtable variable read from the file
	 */
	public static <T> TreeMap<T, T> readTreeMapFromFile(String path) {
		
		TreeMap<T, T> result = null; 
		    
	    try {
	    	FileInputStream fis = new FileInputStream(path);
		    ObjectInputStream ois = new ObjectInputStream(fis);
			
	    	result = (TreeMap<T, T>)ois.readObject();
	    	
	    	ois.close();
	 	    fis.close();
	    } 
	    catch (FileNotFoundException ef) {
			System.out.println("No TreeMap file found for " + path);
	    }		
		catch (Exception e) {
			e.printStackTrace();
		}
		    	
		return result;
	}
	
	public static <T> void writeTreeMapToFile(TreeMap<T, T> dict, String path) throws IOException {
		
		File file = new File(path);
		file.getParentFile().mkdirs(); 
		file.createNewFile();
		FileOutputStream fos = new FileOutputStream(path);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(dict);
		oos.close();
		fos.close();
		
	}
	
	public static <T> void writeHashMapToFile(HashMap<T, T> dict, String path) throws IOException {
		
		File file = new File(path);
		file.getParentFile().mkdirs(); 
		file.createNewFile();
		FileOutputStream fos = new FileOutputStream(path);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(dict);
		oos.close();
		fos.close();
		
	}
	
	/**
	 * reads a hashtable from a file
	 * @param path location of the hashtable
	 * @return the hashtable variable read from the file
	 */
	public static Hashtable<Float, Float> readFloatHashtableFromFile(String path) {
		
		Hashtable<Float, Float> result = null; 
		    
	    try {
	    	FileInputStream fis = new FileInputStream(path);
		    ObjectInputStream ois = new ObjectInputStream(fis);
			
	    	result = (Hashtable<Float, Float>)ois.readObject();
	    	
	    	ois.close();
	 	    fis.close();
	    } 
	    catch (FileNotFoundException ef) {
			System.out.println("No hashtable file found for " + path);
	    }		
		catch (Exception e) {
			e.printStackTrace();
		}
		    	
		return result;
	}
	
	
	
	/**
	 * reads a hashtable from a file
	 * @param path location of the hashtable
	 * @return the hashtable variable read from the file
	 */
	public static Hashtable<Double, Double> readDoubleHashtableFromFile(String path) {
		
		Hashtable<Double, Double> result = null; 
		    
	    try {
	    	FileInputStream fis = new FileInputStream(path);
		    ObjectInputStream ois = new ObjectInputStream(fis);
			
	    	result = (Hashtable<Double, Double>)ois.readObject();
	    	
	    	ois.close();
	 	    fis.close();
	    } 
	    catch (FileNotFoundException ef) {
			System.out.println("No hashtable file found for " + path);
	    }		
		catch (Exception e) {
			e.printStackTrace();
		}
		    	
		return result;
	}
	
	
	
	/**
	 * reads a hashtable from a file
	 * @param path location of the hashtable
	 * @return the hashmap variable read from the file
	 */
	public static HashMap<String, byte[]> readStringByteHashMapFromFile(String path) {
		
		HashMap<String, byte[]> result = null; 
		    
	    try {
	    	FileInputStream fis = new FileInputStream(path);
		    ObjectInputStream ois = new ObjectInputStream(fis);
			
	    	result = (HashMap<String, byte[]>)ois.readObject();
	    	
	    	ois.close();
	 	    fis.close();
	    } 
	    catch (FileNotFoundException ef) {
			//System.out.println("No HashMap file found in \"" + path + "\"");
	    }		
		catch (Exception e) {
			e.printStackTrace();
		}
		    	
		return result;
	}
	
	
	
	public static void writeStringByteHashMapToFile(HashMap<String, byte[]> ht, String path) {
		
		try {
			FileOutputStream fos = new FileOutputStream(path);
			ObjectOutputStream oos = new ObjectOutputStream(fos);

			oos.writeObject(ht);
			oos.close();
		}
		catch(Exception e) {
			e.printStackTrace();
			System.out.println("Unable to write HashMap to file \"" + path + "\"");
		}
	}
	
	public static <T> void writeHashSetToFile(HashSet<T> h, String path) {
		
		try {
			File f = new File(path);
			f.getParentFile().mkdirs(); 
			f.createNewFile();
			FileOutputStream fos = new FileOutputStream(path);
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			oos.writeObject(h);
			oos.close();
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error while saving the HashSet");
		}
	}
	
	public static <T> HashSet<T> readHashSetFromFile(String path) {
		
		HashSet<T> result = new HashSet<T>();
		
		try {
			FileInputStream fis = new FileInputStream(path);
		    ObjectInputStream ois = new ObjectInputStream(fis);
		    result = (HashSet<T>)ois.readObject();
		    ois.close();
		    fis.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error while loading the HashSet");
		}
		
		return result;
	}
	
	public static <T> HashMap<T, T> readHashMapFromFile(String path) {
		
		HashMap<T, T> result = new HashMap<T, T>();
		
		try {
			FileInputStream fis = new FileInputStream(path);
		    ObjectInputStream ois = new ObjectInputStream(fis);
		    result = (HashMap<T, T>)ois.readObject();
		    ois.close();
		    fis.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error while loading the HashMap");
		}
		
		return result;
	}
	
	
	
	/**
	 * Appends contents to a file, needed for creating import scripts
	 * @param file the target file
	 * @param input the new line
	 */
	public void appendToFile(String file, String input) {

	      BufferedWriter bw = null;

	      try {
	         
	         bw = new BufferedWriter(new FileWriter(file, true));
	         bw.write(input);
	         bw.newLine();
	         bw.flush();
	      } catch (IOException ioe) {
	    	  ioe.printStackTrace();
	      } finally {                       
	    	  if (bw != null) 
	    		  try {
	    			  bw.close();
	    		  } catch (IOException ioe2) {
		    
	    		  }
	      } 

	   }
	
}