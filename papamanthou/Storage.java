package papamanthou;

import java.util.List;

public interface Storage {
	/**
	 * 
	 * @param untilLevel
	 *            Remove any content with level = 0 .. untilLevel
	 * @param input
	 *            Enter this nodes in level = untilLevel
	 */
	public void clearLevelsAndAdd(int untilLevel, List<ServerNode> input);
	
	/**
	 * Removes any content until level untillevel (including)
	 * 
	 * @param untilLevel
	 */
	public void clearLevels(int untilLevel);
	
	/**
	 * enters nodes in list into level level. should check if input.size() == 2^level
	 * 
	 * @param level
	 * @param input
	 */
	public void insertNodes(int level, List<ServerNode> input);
	
	/**
	 * Setups the chunk variant of clearLevelsandAadd The new nodes are after
	 * this send with the method sendChunk
	 * 
	 * @param untilLevel
	 *            Remove any content with level = 0..untilLevel
	 * 
	 */
	public void setupClearLevelsAndAddChunks(int untilLevel);

	/**
	 * send a chunk for adding to the main table
	 * 
	 * @param input
	 *            data to add
	 */
	public void sendChunk(List<ServerNode> input);

	/**
	 * Clears all the temp Storage
	 */
	public void clearTempStorage();

	/**
	 * Mainly for debugging purpose
	 * 
	 * @return the number of elements in the temp storage
	 */
	public int getSizeOfTempStorage();

	/**
	 * Puts the elements in input to the positions index, index+1, ... index +
	 * input.size()-1 in the temp storage
	 * 
	 * @param input
	 *            elements to add
	 * @param index
	 *            position of the first element
	 */
	public void putToTempStorage(List<C2> input, int index);

	/**
	 * Returns chunksize elements starting at startindex from the temp storage
	 * 
	 * @param startindex
	 * @param chunkSize
	 * @return the desired elements of the temp storage or null (if no such
	 *         elements exist)
	 */
	public List<C2> getFromTempStorage(int startindex, int chunkSize);

	/**
	 * This method is important. It "commits" the written data to the temp
	 * storage Before this method is called getFromTempStorage still delivers
	 * the results as it was before the first putToTempStorage was called.
	 * switchtTempStorage persists the changes made by putToTempStorage and
	 * makes getFromTempStorage read from the updated version of the temp
	 * storage
	 * 
	 */
	public void switchTempStorage();

	/**
	 * 
	 * @param level
	 * @return all c2 values and init Vectors with level<=level
	 */
	public List<C2> getAllC2UntilLevel(int level);

	/**
	 * 
	 * @param level
	 *            until which level do we want the data
	 * @param chunkSize
	 *            size of a single chunk Sets up the Storage so that it can
	 *            return all the data until level in chunkSize large chunks
	 */
	public void setupChunksGetAllC2UntilLevel(int level, int chunkSize);

	/**
	 * gives the next chunk of data from the main storage
	 * 
	 * @return the next Chunk of data
	 */
	public List<C2> getNextChunk();

	/**
	 * 
	 * @return first level of main storage without any nodes
	 */
	public int getFirstEmptyLevel();

	/**
	 * 
	 * @return number of keyword-document pairs in main storage
	 */
	public int getNumberOfEntries();

	/**
	 * 
	 * @return number of Levels which contain content or have a higher level
	 *         which contains content
	 */
	public int getNumberOfLevels();

	/**
	 * 
	 * @param level
	 * @return is this level empty
	 */
	public boolean isLevelEmpty(int level);

	/**
	 * Insert a single node
	 * 
	 * @param level
	 *            in which the node should be inserted
	 * @param node
	 *            that should be inserted
	 */
	public void insertInLevel0(ServerNode node);

	/**
	 * 
	 * @param hkey
	 * @param level
	 * @return is an entry with the value hkey in level?
	 */
	public boolean isKeyInLevel(byte[] hkey, int level);

	/**
	 * Get C1 for a certain entry
	 * 
	 * @param hkey
	 * @param level
	 * @return return c1 value corresponding to hkey in level
	 */
	public byte[] getC1(byte[] hkey, int level);

	/**
	 * Setup the storage
	 * 
	 * @param levels
	 *            number of levels to create
	 */
	public void setup(int levels);
	
	/**
	 * deletes the main storage
	 */
	public void deleteMainStorage();
	
	public void close();

	
	/**
	 * gets all Nodes from a level
	 * @param level
	 * @return
	 */
	public List<ServerNode> getNodes(int level);
	
	/**
	 * deletes metadata created by this storage
	 */
	public void deleteMetaData();

}
