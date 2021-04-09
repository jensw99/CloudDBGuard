import java.io.File;
import java.util.HashMap;
import java.util.HashSet;

import misc.Enron_Mail;
import misc.Timer;

/**
 * Class for uploading the enron dataset to a cassandra database
 * @author Tim Waage, Jens Weigel
 *
 */
public class BenchEnronUnencrypted {
	
	private DBCassandraUnencrypted db;
	
	// statistics
	private int filesAdded;
	private long timeForDBCommunication;
	private Timer timerWithParsing;
	// API object
	
	
	
	public BenchEnronUnencrypted(String _ip, int _port) {
		System.out.println("Benching Enron on " + _ip + ":" + _port);
		db = new DBCassandraUnencrypted(_ip, _port);
	}
	
	
	public void upload(String path) {
		
		// reset everything
		filesAdded = 0;
		timeForDBCommunication = 0;
		timerWithParsing = new Timer();
		
		db.dropEnronKeyspace();
		
		db.createEnronKeyspace();
		
		db.createEnronTable();
					
		File dir = new File(path);
				
		// benchmark
		System.out.println("Importing " + dir.getAbsolutePath() + "...");			
		importDir(dir, dir.getAbsolutePath());
		
		
		// print statistics
		System.out.println("Insertion complete.");
		System.out.println(filesAdded + " files added");
		System.out.println("time for insertions excl. parsing: " + Timer.getTimeAsString(timeForDBCommunication));
		System.out.println("time for insertions incl. parsing:      " + timerWithParsing.getRuntimeAsString());
		
	}
	
	
	//public void search() {
		// not needed anymore, as searching = querying via the API class
	//}
	
	
	
	
	/**
	 * Populates the Database with (encrypted) testdata 
	 * @param input a File object representing a directory, should be the Enron root folder
	 * @param rootFolder the absolute path of the root folder, where the encryption starts
	 */
	public void importDir(File input, String rootFolder) {
		
		File[] inputFiles = input.listFiles();
		System.out.println("contains "+inputFiles.length+" files");
		if (inputFiles != null) {
			for (int i = 0; i < inputFiles.length; i++) {
				
				if(inputFiles[i].isDirectory()) {
					System.out.println("import dir "+inputFiles[i].getAbsolutePath());
					importDir(inputFiles[i], rootFolder);
				}
				else {	
					importFile(inputFiles[i].getAbsolutePath());
					//System.out.println("Adding: " + inputFiles[i].getAbsolutePath());
					filesAdded++;
				}
		    }
		}
		
		
	}
	
	
	
	/**
	 * Encrypts a file according to the given parameters and the parameter specified in the constructor
	 * @param inputFilePath name of the input file
	 * @return time needed for insertion incl API overhead
	 */
	protected long importFile(String inputFilePath) {
		
		//System.out.println("import file "+inputFilePath);
		timerWithParsing.start();
		Enron_Mail mail = Enron_Mail.parseFile(inputFilePath);
		//System.out.println("parse ok.");
		
		timeForDBCommunication += db.insertRow(
				new HashMap<String, String>(){				// Strings
					{
					 if(mail.getID() != null) put("id", mail.getID());
					 if(mail.getFrom() != null) put("sender", mail.getFrom());
					 if(mail.getSubject() != null) put("subject", mail.getSubject());
					 if(mail.getBody() != null) put("body", mail.getBody());
					 if(mail.getPath() != null) put("path", mail.getPath());
					 if(mail.getXCc() != null) put("xcc", mail.getXCc());
					 if(mail.getXFolder() != null) put("xfolder", mail.getXFolder());
					 if(mail.getXOrigin() != null) put("xorigin", mail.getXOrigin());
					 if(mail.getMimeVersion() != null) put("mimeversion", mail.getMimeVersion());
					 if(mail.getXBcc() != null) put("xbcc", mail.getXBcc());
					 if(mail.getXFileName() != null) put("xfilename", mail.getXFileName());
					 if(mail.getXTo() != null) put("xto", mail.getXTo());
					 if(mail.getContentTransferEncoding() != null) put("cte", mail.getContentTransferEncoding());
					 if(mail.getXFrom() != null) put("xfrom", mail.getXFrom());
					 if(mail.getContentType() != null) put("contenttype", mail.getContentType());
					 if(mail.getWriter() != null) put("writer", mail.getWriter());
					 }
				},
				new HashMap<String, Long>(){				// Numerical Values
					{
					 put("size", mail.getSize());
					 put("year", mail.getDateYear());
					 put("month", mail.getDateMonth());
					 put("day", mail.getDateDay());
					 put("timestamp", mail.getTimestamp());
					}
				},
				new HashMap<String, HashSet<String>>(){		// String sets
					{
					if(mail.getTo() != null) put("receiver", mail.getTo());
					if(mail.getCc() != null) put("cc", mail.getCc());
				    if(mail.getBcc() != null) put("bcc", mail.getBcc());
					}
				}
			);
		
		timerWithParsing.stop();
		
		return timerWithParsing.getRuntime();
	}

}

