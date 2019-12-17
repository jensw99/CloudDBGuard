package interfaces;

import org.jdom2.*;

/**
 * Classes implementing this interface allow their instances to be saved to and loaded
 * from XML Elements
 * 
 * @author Tim Waage
 */
public interface SaveableInXMLElement {
	/**
	 * Stores all information necessary to instantiate an object of this class in a XML Element
	 * @return the resulting XML Element
	 */
	public Element getThisAsXMLElement();
	
	
	
	/**
	 * Initializes an object using information stored in an XML Element
	 * @param data the XML Element containing the necessary information
	 */
	public void initializeFromXMLElement(Element data); 

}
