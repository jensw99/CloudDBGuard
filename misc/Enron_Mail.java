package misc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

/** 
 * reflects the mail box use case, turns text files representing mails into mail objects,
 * provides methods for writing them to the databases,
 * works with Enron as well as with the TREC spam corpus
 * 
 * @author Tim Waage
 *
 */
public class Enron_Mail {
	
	// mail ID
	private String id = "";
	
	// mail sender
	private String from = "";
		
	// mail recipient
	private HashSet<String> to = new HashSet<String>();
	
	// mail carbon copy
	private HashSet<String> cc  = new HashSet<String>();
	
	// mail blind carbon copy
	private HashSet<String> bcc  = new HashSet<String>();
	
	// mail subject
	private String subject = "";
	
	// mail body
	private String body = "";
	
	// mail date
	private long date_year;
	private long date_month;
	private long date_day;
	private long date_hours;
	private long date_minutes;
	private long date_seconds;
	
	// weekday
	private String weekday = "";
	
	// size of a single mail file
	private long size;
	
	// the mail's file path
	private String path;
	private String writer;
	
	// all aother attributes
	private String xcc;
	private String xfolder;
	private String xorigin;
	private String mimeversion;
	private String xbcc;
	private String xfilename;
	private String xto;
	private String contenttransferencoding;
	private String xfrom;
	private String contenttype;
	
	
	
	// for converting the month string to a long
	private static HashMap<String, Long> months = null;
	
	
	
	/**
	 * Constructor for a single mail object
	 * 
	 * @param _id mail identifier
	 * @param _from mail from field
	 * @param _to mail to field
	 * @param _cc mail cc field
	 * @param _subject mail subject field
	 * @param _body mail body field
	 * @param _size size of the original mail text file
	 */
	public Enron_Mail(String _id, 
			long _date_year, 
			long _date_month, 
			long _date_day, 
			long _date_hours,
			long _date_minutes,
			long _date_seconds,
			String _weekday, 
			String _from, 
			HashSet<String> _to, 
			HashSet<String> _cc,
			HashSet<String> _bcc,
			String _subject, 
			String _body, 
			long _size,
			String _path,
			String _xcc,
			String _xfolder,
			String _xorigin,
			String _mimeversion,
			String _xbcc,
			String _xfilename,
			String _xto,
			String _contenttransferencoding,
			String _xfrom,
			String _contenttype) {
			
		this.id = _id;
		this.date_year = _date_year;
		this.date_month = _date_month;
		this.date_day = _date_day;
		this.date_hours = _date_hours;
		this.date_minutes = _date_minutes;
		this.date_seconds = _date_seconds;
		this.weekday = _weekday;
		this.from = _from;
		this.to = _to;
		this.cc = _cc;
		this.bcc = _bcc;
		this.subject = _subject;
		this.body = _body;
		this.size = _size;
		this.path = _path;
		this.xcc = _xcc;
		this.xfolder = _xfolder;
		this.xorigin = _xorigin;
		this.mimeversion = _mimeversion;
		this.xbcc = _xbcc;
		this.xfilename = _xfilename;
		this.xto = _xto;
		this.contenttransferencoding = _contenttransferencoding;
		this.xfrom = _xfrom;
		this.contenttype = _contenttype;
		
		//System.out.println("path="+_path);
		this.writer = _path.split("/")[4];  //SHIT war 4
			
	}
	
	
	
	/**
	 * gets a mail's content type field
	 * @return the mail's content type field
	 */
	public String getContentType() {
		return contenttype;
	}
	
	

	/**
	 * gets a mail's x-from field
	 * @return the mail's x-from field
	 */
	public String getXFrom() {
		return xfrom;
	}
	
	

	/**
	 * gets a mail's content transfer encoding field
	 * @return the mail's content transfer encoding field
	 */
	public String getContentTransferEncoding() {
		return contenttransferencoding;
	}
	
	

	/**
	 * gets a mail's x-to field
	 * @return the mail's x-to field
	 */
	public String getXTo() {
		return xto;
	}
	
	

	/**
	 * gets a mail's x-filename field
	 * @return the mail's x-filename field
	 */
	public String getXFileName() {
		return xfilename;
	}
	
	

	/**
	 * gets a mail's x-bcc field
	 * @return the mail's x-bcc field
	 */
	public String getXBcc() {
		return xbcc;
	}
	
	

	/**
	 * gets a mail's mime version field
	 * @return the mail's mime version field
	 */
	public String getMimeVersion() {
		return mimeversion;
	}
	
	

	/**
	 * gets a mail's x-origin field
	 * @return the mail's x-origin field
	 */
	public String getXOrigin() {
		return xorigin;
	}
	
	

	/**
	 * gets a mail's x-folder field
	 * @return the mail's x-folder field
	 */
	public String getXFolder() {
		return xfolder;
	}
	
	

	/**
	 * gets a mail's x-cc field
	 * @return the mail's x-cc field
	 */
	public String getXCc() {
		return xcc;
	}
	
	

	/**
	 * gets a mail's identifier
	 * @return the mail's identifier
	 */
	public String getID() {
		return id;
	}
	
	
	/**
	 * gets a mail's year
	 * @return the mail's year
	 */
	public Long getDateYear() {
		return date_year;
	}
	
	
	
	/**
	 * gets a mail's month
	 * @return the mail's month
	 */
	public Long getDateMonth() {
		return date_month;
	}
	
	
	
	/**
	 * gets a mail's day
	 * @return the mail's day
	 */
	public Long getDateDay() {
		return date_day;
	}
	
	
	
	/**
	 * gets a mail's weekday
	 * @return the mail's weekday
	 */
	public String getWeekday() {
		return weekday;
	}
	
	
	
	/**
	 * gets a mail's from field
	 * @return the mail's from field
	 */
	public String getFrom() {
		return from;
	}
	
	
	
	/**
	 * gets a mail's to field
	 * @return the mail's to field
	 */
	public HashSet<String> getTo() {
		return to;
	}
	
	
	
	/**
	 * gets a mail's carbon copy field
	 * @return the mail's corbon copy field
	 */
	public HashSet<String> getCc() {
		return cc;
	}
	
	
	
	/**
	 * gets a mail's carbon copy field
	 * @return the mail's corbon copy field
	 */
	public HashSet<String> getBcc() {
		return bcc;
	}
	
	
	
	/**
	 * gets a mail's subject field
	 * @return the mail's subject field
	 */
	public String getSubject() {
		return subject;
	}
	
	
	
	/** 
	 * gets a mail's body
	 * @return the mail's body
	 */
	public String getBody() {
		return body;
	}
	
	
	
	/** 
	 * gets a mail's path
	 * @return the mail's path
	 */
	public String getPath() {
		return path;
	}
	
	
	
	/** 
	 * gets a mail's writer
	 * @return the mail's writer
	 */
	public String getWriter() {
		return writer;
	}
	
	
	
	/**
	 * gets the file size of the original mail file
	 * @return the file size of the original mail file
	 */
	public long getSize() {
		return size;
	}

	
	
	/** gets an entire mail as one single String
	 * 
	 * @return entire mail as one single String
	 */
	public String getMailAsString() {
		
		return from + " " + to + " " + cc + " " + subject + " " + body;
	}

	
	
	/**
	 * Parses a mail text file and creates a mail object
	 * 
	 * @param inputFileName the input mail text file
	 * @return the mail object reflecting the input text file
	 */
	public static Enron_Mail parseFile(String inputFileName) {
		
		String line = null;
		BufferedReader scanner = null;
		
		Boolean foundMessageID = false;
		Boolean foundFrom = false;
		Boolean foundDate = false;
		Boolean foundSubject = false;
		Boolean foundXCc = false;
		Boolean foundXFolder = false;
		Boolean foundXOrigin = false;
		Boolean foundMimeVersion = false;
		Boolean foundXBcc = false;
		Boolean foundXFileName = false;
		Boolean foundXTo = false;
		Boolean foundContentTransferEncoding = false;
		Boolean foundXFrom = false;
		Boolean foundContentType = false;
		
		// temp variables for creating the mail fields
		String tmp_id = "";
		String tmp_date = "";
		String tmp_from = "";
		HashSet<String> tmp_to  = new HashSet<String>();
		HashSet<String> tmp_cc  = new HashSet<String>();
		HashSet<String> tmp_bcc  = new HashSet<String>();
		String tmp_subject = "";
		String tmp_body = "";
		long tmp_size = 0;
		long tmp_date_year = 0;
		long tmp_date_month = 0;
		long tmp_date_day = 0;
		long tmp_date_hours = 0;
		long tmp_date_minutes = 0;
		long tmp_date_seconds = 0;
		String tmp_weekday = "";
		String tmp_xcc = "";
		String tmp_xfolder = "";
		String tmp_xorigin = "";
		String tmp_mimeversion = "";
		String tmp_xbcc = "";
		String tmp_xfilename = "";
		String tmp_xto = "";
		String tmp_contenttransferencoding = "";
		String tmp_xfrom = "";
		String tmp_contenttype = "";
		
		// parsing mode
		int mode = 0; // 0 = nothing, 1 = from, 2 = to, 3 = cc, 4 = bcc, 5 = body
		
		try {
	         
			//open file
			//File file = new File(inputFileName);
			tmp_size = new File(inputFileName).length();
			
			scanner = new BufferedReader(new FileReader(inputFileName));
	         
			// go through the file line by line
			while ((line = scanner.readLine()) != null) {
				
				//line = scanner.nextLine();
				
				if(mode < 5) { //only search for other components, if we're not already in the mail's body
				
					if(mode == 2) {
						// if a line represents another recipient, it starts with a tab character
						if(line.startsWith("\t")) {
							String[] tmp = line.split(" ");
							for(String s : tmp) {
								s = s.replace("\t", "");
								tmp_to.add(s.replace(",", ""));
							}
						}
						//otherwise leave "to"-mode
						else mode = 0;
					}
					
					else if (mode == 3) {
						// if a line represents another cc-recipient, it starts with a tab character
						if(line.startsWith("\t")) {
							String[] tmp = line.split(" ");
							for(String s : tmp) {
								s = s.replace("\t", "");
								tmp_cc.add(s.replace(",", ""));
							}
						}
						//otherwise leave "cc"-mode
						else mode = 0;
					}
					
					else if (mode == 4) {
						// if a line represents another bcc-recipient, it starts with a tab character
						if(line.startsWith("\t")) {
							String[] tmp = line.split(" ");
							for(String s : tmp) {
								s = s.replace("\t", "");
								tmp_bcc.add(s.replace(",", ""));
							}
						}
						//otherwise leave "bcc"-mode
						else mode = 0;
					}
					
					//message-id field has always only one line, so we can take it directly
					if(line.startsWith("Message-ID: ")&&(!foundMessageID)) {
						tmp_id = line.substring(12, line.length());
						foundMessageID = true;
					}
					
					//subject field has always only one line, so we can take it directly
					else if(line.startsWith("Date: ")&&(!foundDate)) {
						tmp_date = line.substring(6, line.length());
						
						tmp_weekday = tmp_date.substring(0, 3);
						
						if(tmp_date.charAt(6) != ' ') {
							tmp_date_day = Long.valueOf(tmp_date.substring(5, 7));
							tmp_date_month = convertMonthStringToLong(tmp_date.substring(8, 11));
							tmp_date_year = Long.valueOf(tmp_date.substring(12, 16));
							tmp_date_hours = Long.valueOf(tmp_date.substring(17, 19));
							tmp_date_minutes = Long.valueOf(tmp_date.substring(20, 22));
							tmp_date_seconds = Long.valueOf(tmp_date.substring(23, 25));
						}
						else {
							tmp_date_day = Long.valueOf(tmp_date.substring(5, 6));
							tmp_date_month = convertMonthStringToLong(tmp_date.substring(7, 10));
							tmp_date_year = Long.valueOf(tmp_date.substring(11, 15));
							tmp_date_hours = Long.valueOf(tmp_date.substring(16, 18));
							tmp_date_minutes = Long.valueOf(tmp_date.substring(19, 21));
							tmp_date_seconds = Long.valueOf(tmp_date.substring(22, 24));
						}
						foundDate = true;
					}
					
					//from field has always only one line, so we can take it directly
					else if(line.startsWith("From: ")&&(!foundFrom)) {
						tmp_from = line.substring(6, line.length());
						foundFrom = true;
					}
					
					//x-cc field has always only one line, so we can take it directly
					else if(line.startsWith("X-cc: ")&&(!foundXCc)) {
						tmp_xcc = line.substring(6, line.length());
						foundXCc = true;
					}
					
					//x-folder field has always only one line, so we can take it directly
					else if(line.startsWith("X-Folder: ")&&(!foundXFolder)) {
						tmp_xfolder = line.substring(10, line.length());
						foundXFolder = true;
					}
					
					//x-origin field has always only one line, so we can take it directly
					else if(line.startsWith("X-Origin: ")&&(!foundXOrigin)) {
						tmp_xorigin = line.substring(10, line.length());
						foundXOrigin = true;
					}
					
					//mime version field has always only one line, so we can take it directly
					else if(line.startsWith("Mime-Version: ")&&(!foundMimeVersion)) {
						tmp_mimeversion = line.substring(14, line.length());
						foundMimeVersion = true;
					}
					
					//x-bcc field has always only one line, so we can take it directly
					else if(line.startsWith("X-bcc: ")&&(!foundXBcc)) {
						tmp_xbcc = line.substring(7, line.length());
						foundXBcc = true;
					}
					
					//x-filename field has always only one line, so we can take it directly
					else if(line.startsWith("X-FileName: ")&&(!foundXFileName)) {
						tmp_xfilename = line.substring(12, line.length());
						foundXFileName = true;
					}
					
					//x-to field has always only one line, so we can take it directly
					else if(line.startsWith("X-To: ")&&(!foundXTo)) {
						tmp_xto = line.substring(6, line.length());
						foundXTo = true;
					}
					
					//content transfer encoding field has always only one line, so we can take it directly
					else if(line.startsWith("Content-Transfer-Encoding: ")&&(!foundContentTransferEncoding)) {
						tmp_contenttransferencoding = line.substring(27, line.length());
						foundContentTransferEncoding = true;
					}
					
					//x-from field has always only one line, so we can take it directly
					else if(line.startsWith("X-From: ")&&(!foundXFrom)) {
						tmp_xfrom = line.substring(8, line.length());
						foundXFrom = true;
					}
					
					//content type field has always only one line, so we can take it directly
					else if(line.startsWith("Content-Type: ")&&(!foundContentType)) {
						tmp_contenttype = line.substring(14, line.length());
						foundContentType = true;
					}
					
					//subject field has always only one line, so we can take it directly
					else if(line.startsWith("Subject: ")&&(!foundSubject)) {
						tmp_subject = line.substring(9, line.length());
						foundSubject = true;
					}
										
					else if(line.startsWith("To: ")) {
						// grab the first "To" line
						String[] tmp = line.substring(4, line.length()).split(" ");
						for(String s : tmp) {
							s = s.replace("\t", "");
							tmp_to.add(s.replace(",", ""));
						}
						// look for other recipients
						mode = 2;
					}
					
					else if(line.startsWith("Cc: ")) {
						// grab the first "To" line
						String[] tmp = line.substring(4, line.length()).split(" ");
						for(String s : tmp) {
							s = s.replace("\t", "");
							tmp_cc.add(s.replace(",", ""));
						}
						// look for other recipients
						mode = 3;
					}
					
					
					else if(line.startsWith("Bcc: ")) {
						// grab the first "Bcc" line
						String[] tmp = line.substring(5, line.length()).split(" ");
						for(String s : tmp) {
							s = s.replace("\t", "");
							tmp_bcc.add(s.replace(",", ""));
						}
						// look for other recipients
						mode = 4;
					}
					
					// mail bodies always start with a single empty line
					else if(line.equals("")) {
						
						mode = 5;
					}
				}
				else {
					tmp_body += " " + line;
					
				}				
	        }
	        scanner.close();
	        
	    } 
		catch (Exception e) {
	         
			e.printStackTrace();
	    }
		finally {
			try {
				if (scanner != null) scanner.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
				
		if(tmp_id.length() == 0) return 
			new Enron_Mail(inputFileName, 
				tmp_date_year, 
				tmp_date_month, 
				tmp_date_day, 
				tmp_date_hours, 
				tmp_date_minutes, 
				tmp_date_seconds, 
				tmp_weekday, 
				tmp_from, 
				tmp_to, 
				tmp_cc, 
				tmp_bcc,
				tmp_subject, 
				tmp_body, 
				tmp_size, 
				inputFileName,
				tmp_xcc,
				tmp_xfolder,
				tmp_xorigin,
				tmp_mimeversion,
				tmp_xbcc,
				tmp_xfilename,
				tmp_xto,
				tmp_contenttransferencoding,
				tmp_xfrom,
				tmp_contenttype);
		else return 
			new Enron_Mail(tmp_id, 
				tmp_date_year, 
				tmp_date_month, 
				tmp_date_day,
				tmp_date_hours, 
				tmp_date_minutes, 
				tmp_date_seconds,
				tmp_weekday, 
				tmp_from, 
				tmp_to, 
				tmp_cc, 
				tmp_bcc,
				tmp_subject, 
				tmp_body, 
				tmp_size, 
				inputFileName,
				tmp_xcc,
				tmp_xfolder,
				tmp_xorigin,
				tmp_mimeversion,
				tmp_xbcc,
				tmp_xfilename,
				tmp_xto,
				tmp_contenttransferencoding,
				tmp_xfrom,
				tmp_contenttype);
	}


	
	private static long convertMonthStringToLong(String s) {
		
		if(months == null) {
			months = new HashMap<String, Long>();
			months.put("Jan", 1L);
			months.put("Feb", 2L);
			months.put("Mar", 3L);
			months.put("Apr", 4L);
			months.put("May", 5L);
			months.put("Jun", 6L);
			months.put("Jul", 7L);
			months.put("Aug", 8L);
			months.put("Sep", 9L);
			months.put("Oct", 10L);
			months.put("Nov", 11L);
			months.put("Dec", 12L);
		}
		
		return months.get(s);
		
	}
	
	
	
	public long getTimestamp() {
		
		Calendar c = new GregorianCalendar((int)date_year, (int)date_month, (int)date_day, (int)date_hours, (int)date_minutes, (int)date_seconds);		
		long result = (c.getTimeInMillis()/ 1000) - 2584800; // Timezone Offset hardcoded to avoid parsing it (is always -8 PST)
	
		return result / 60; // seconds are always 0
	}
	
	

		
}
