package task.store;

import java.io.Serializable;

/**
 * An index entry containing information about the object on a disk 
 * 
 * @author Fedor Trofimov
 *
 */
public class Index implements Serializable {

	private boolean isDeleted;
	private int fileNumber;
	private long dataOffset;
	private int dataSize;
	private transient long indexOffset;
	private String key;

	public Index(boolean isDeleted, int fileNumber, long dataOffset,
			int dataSize, long indexOffset, String key) {
		this.isDeleted = isDeleted;
		this.fileNumber = fileNumber;
		this.dataOffset = dataOffset;
		this.dataSize = dataSize;
		this.indexOffset = indexOffset;
		this.key = key;
	}

	/**
	 * 
	 * @return true if the object is removed, otherwise false
	 */
	public boolean isDeleted() {
		return isDeleted;
	}

	/**
	 * Marks the object as removed
	 * @param deletion flag
	 */
	public void setDeleted(boolean isDeleted) {
		this.isDeleted = isDeleted;
	}

	/**
	 * 
	 * @return object offset in the data file
	 */
	public long getDataOffset() {
		return dataOffset;
	}

	/**
	 * 
	 * @return object size in the data file
	 */
	public int getDataSize() {
		return dataSize;
	}

	/**
	 * 
	 * @return index offset in the index file
	 */
	public long getIndexOffset() {
		return indexOffset;
	}

	/**
	 * Updates index offset in the index file
	 * @param indexOffset - new index offset in the index file
	 */
	public void setIndexOffset(long indexOffset) {
		this.indexOffset = indexOffset;
	}

	/**
	 * 
	 * @return object's key
	 */
	public String getKey() {
		return key;
	}

	/**
	 * 
	 * @return ordinal number of the file where the object is stored
	 */
	public int getFileNumber() {
		return fileNumber;
	}

	@Override
	public String toString() {
		return "Index [isDeleted=" + isDeleted + ", fileNumber=" + fileNumber
				+ ", dataOffset=" + dataOffset + ", dataSize=" + dataSize
				+ ", indexOffset=" + indexOffset + ", key=" + key + "]";
	}

}
