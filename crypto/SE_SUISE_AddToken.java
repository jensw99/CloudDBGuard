package crypto;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Vector;

import databases.DBLocation;



/**
 * Implementation of the "Add-Token" as it appears in the Hahn & Kerschbaum SE Paper
 * 
 * @author Tim Waage
 */
public class SE_SUISE_AddToken {

	// location within the database this token was made for
	private DBLocation id = null;
	
	// corresponds to "c" in the paper
	private HashSet<byte[]> c = null;
	
	// corresponds to "x" in the paper
	private Vector<byte[]> x = null;
	
	
	
	/**
	 * Constructor
	 */
	public SE_SUISE_AddToken() {
		
		c = new HashSet<byte[]>();
		x = new Vector<byte[]>();
	}
	
	
	
	/**
	 * Constructor
	 * @param _id location within the database this token was made for
	 * @param _c corresponds to "c" in the paper
	 * @param _x corresponds to "x" in the paper
	 */
	public SE_SUISE_AddToken(DBLocation _id, HashSet<byte[]> _c, Vector<byte[]> _x) {
		id = _id;
		c = _c;
		x = _x;
	}
	
	
	
	/**
	 * gets the affected database location
	 * @return the affected database location
	 */
	public DBLocation getID() {
		return id;
	}
	
	
	
	/**
	 * gets c
	 * @return c as it appears in the paper
	 */
	public HashSet<byte[]> getC() {
		
		return c;
	}
	
	
	
	/**
	 * gets c in the form of bytebuffers
	 * @return c in the form of bytebuffers
	 */
	public HashSet<ByteBuffer> getCAsByteBuffers() {
		
		HashSet<ByteBuffer> result = new HashSet<ByteBuffer>();
		
		for(byte[] x : c) result.add(ByteBuffer.wrap(x));
		
		return result;
	}
	
	
	
	/**
	 * gets x
	 * @return x as it appears in the paper
	 */
	public Vector<byte[]> getX() {
		
		return x;
	}
	
	
	
	/**
	 * sets the database location
	 * @param _id the database location
	 */
	public void setID (DBLocation _id) {
		id = _id;
	}
	
	
	
	/**
	 * adds an element to c
	 * @param newC the new c element
	 */
	public void addToC(byte[] newC) {
		c.add(newC);
	}
	
	
	
	/**
	 * adds an element to x
	 * @param newX the new x element
	 */
	public void addToX(byte[] newX) {
		x.add(newX);
	}
	
}
