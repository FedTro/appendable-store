package task.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Appendable-only object Store persisted data on a disk
 * 
 * @author Fedor Trofimov
 * @param <T> The type of a value in the Store
 */
public class Store<T extends Serializable> implements AppendableStore<T> {

	private Map<String, Index> indexMap;
	private FileChannel ifc;
	private List<FileChannel> dfcs;

	private int capacity;
	private int size;
	private float loadFactor;
	private String directory;

	private final String IFILE_EXT = ".ind";
	private final String DFILE_EXT = ".dat";
	private final String FILE_PREFIX = "store";
	private final String FILE_COPY_PREFIX = "_copy";
	private final int DATA_FILE_SIZE_THRESHOLD = 1 << 20; // 1Mb

	/**
	 * Constructs this Store persisted on a disk in the specified FS directory
	 * @param directory - directory where placed index and data files of this Store, the directory must be existed
	 */
	public Store(String directory) {
		this(directory, 0.75f);
	}

	/**
	 * 
	 * Constructs this Store persisted on a disk in the specified FS directory
	 * @param directory - directory where placed index and data files of this Store, the directory must be existed
	 * @param loadFactor - load factor
	 */
	public Store(String directory, float loadFactor) {
		if (loadFactor <= 0)
			throw new IllegalArgumentException("The load factor must be positive");
		this.directory = directory;
		this.loadFactor = loadFactor;
		indexMap = new HashMap<>();
		try {
			ifc = createIndexFile("");
			collectDataFiles();
			if (Files.exists(Paths.get(constructIndexFileName("")))) {
				loadStore();
			} else {
				capacity = 0;
				size = 0;
			}
		} catch (IOException | ClassNotFoundException exc) {
			throw new RuntimeException("An error has occurred during data restore", exc);
		}
	}

	/**
	 * Puts the value into this Store with the associated key.
	 * 
	 * @param key - key with which the specified value is to be associated
	 * @param value - value to be associated with the specified key
	 */
	public void append(String key, T value) {

		if (indexMap.containsKey(key))
			throw new IllegalArgumentException("Object with the specified key has already existed");

		Index index = null;
		try {
			index = appendOnDisk(key, value, ifc, dfcs, false);
		} catch (IOException exc) {
			throw new RuntimeException(exc);
		}

		// write into a memory
		indexMap.put(key, index);
		capacity++;
		size++;
	}

	/**
	 * Returns the value to which the specified key is mapped, or null if this Store contains no value for the key.
	 * 
	 * @param key - key whose associated value is to be returned
	 * @return value to which the specified key is associated, or null if this Store contains no mapping for the key
	 */
	public T get(String key) {
		Index index = indexMap.get(key);
		if (index == null)
			return null;
		byte[] byteArray = new byte[(int) index.getDataSize()];
		FileChannel dfc = dfcs.get(index.getFileNumber());
		try {
			dfc.position(index.getDataOffset());
			dfc.read(ByteBuffer.wrap(byteArray));
			return (T) deserialize(byteArray);
		} catch (IOException | ClassNotFoundException exc) {
			throw new RuntimeException(exc);
		}
	}

	/**
	 * Removes the value for the specified key from this Store if present.
	 * 
	 * @param key - key whose value is to be removed from the Store
	 * @return true if the value associated with the key exists, false if there was no value for the key
	 */
	public boolean remove(String key) {
		// remove from a memory
		Index index = indexMap.remove(key);
		if (index == null)
			return false;
		size--;

		// mark as removed on a disk
		try {
			index.setDeleted(true);
			byte[] byteArray = serialize(index);
			ifc.position(index.getIndexOffset() + 4);
			ifc.write(ByteBuffer.wrap(byteArray));
		} catch (IOException exc) {
			indexMap.put(key, index);
			size++;
			throw new RuntimeException(exc);
		}

		float ratio = (float) size / capacity;
		if (ratio < loadFactor)
			relocate();
		return true;
	}

	/**
	 * Generates a unique key provided as UUID string
	 * 
	 * @return a randomly generated 16 byte key
	 */
	public String generateKey() {
		return UUID.randomUUID().toString();
	}

	/**
	 * Closes file channels resources
	 */
	public void close() {
		try {
			ifc.close();
			for (FileChannel channel : dfcs)
				channel.close();
		} catch (IOException exc) {
			throw new RuntimeException(exc);
		}
	}

	/**
	 * Reload this Store from index and data files persisted on a disk
	 * 
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private void loadStore() throws IOException, ClassNotFoundException {
		while (ifc.position() < ifc.size()) {
			Index index = readIndexFromDisk();

			if (!index.isDeleted()) {
				indexMap.put(index.getKey(), index);
				size++;
			}
			capacity++;
		}
	}

	/**
	 * 
	 * @param key - key with which the specified value is to be associated
	 * @param value - value to be associated with the specified key
	 * @param indexChannel - file channel of an index file
	 * @param dataChannels - file channels list of data files
	 * @param relocation - flag indicates the phase in which the value is appended
	 * @return the index constructed for the specified value
	 * @throws IOException
	 */
	private Index appendOnDisk(String key, T value, FileChannel indexChannel,
			List<FileChannel> dataChannels, boolean relocation)
			throws IOException {
		// persist data on a disk
		int lastFileNumber = dataChannels.size() - 1;
		byte[] byteArray = serialize(value);
		FileChannel dfc = dataChannels.get(lastFileNumber); // write to last file
		long dataOffset = dfc.size();
		dfc.position(dataOffset);
		dfc.write(ByteBuffer.wrap(byteArray));
		int dataSize = (int) (dfc.size() - dataOffset);

		// persist index on a disk
		long indexOffset = indexChannel.size();
		Index index = new Index(false, lastFileNumber, dataOffset, dataSize,
				indexOffset, key);
		byteArray = serialize(index);
		indexChannel.position(indexOffset + 4);
		indexChannel.write(ByteBuffer.wrap(byteArray));
		int indexSize = (int) (indexChannel.size() - indexOffset - 4);
		ByteBuffer byteBuffer = ByteBuffer.allocate(4).putInt(indexSize);
		byteBuffer.rewind();
		indexChannel.write(byteBuffer, indexOffset);

		// create a new data file on reaching threshold for the last data file
		if (dataChannels.get(lastFileNumber).size() > DATA_FILE_SIZE_THRESHOLD) {
			createNextDataFile(dataChannels, relocation ? FILE_COPY_PREFIX : "");
		}
		return index;
	}

	/**
	 * Reads the index from the index file
	 * @return read index
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private Index readIndexFromDisk() throws IOException,
			ClassNotFoundException {
		long indexOffset = ifc.position();
		ByteBuffer byteBuffer = ByteBuffer.allocate(4);
		ifc.read(byteBuffer);
		int indexSize = byteBuffer.getInt(0);
		byte[] byteArray = new byte[(int) indexSize];
		ifc.read(ByteBuffer.wrap(byteArray));
		Index index = (Index) deserialize(byteArray);
		index.setIndexOffset(indexOffset);
		return index;
	}

	/**
	 * Serializes the object to a byte array
	 * @param value  - serializable object
	 * @return serialized object as a byte array
	 * @throws IOException
	 */
	private byte[] serialize(Object value) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(value);
		oos.flush();
		return baos.toByteArray();
	}

	/**
	 * Deserializes the object from a byte array
	 * @param byteArray - array
	 * @return deserialized object
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private Object deserialize(byte[] byteArray) throws IOException,
			ClassNotFoundException {

		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(byteArray));

		return ois.readObject();
	}

	/**
	 * Relocates index and data files on a disk if the actual Store's loading is less than the load factor
	 */
	private void relocate() {
		capacity = 0;

		FileChannel newIndexChannel = createIndexFile(FILE_COPY_PREFIX);

		List<FileChannel> newDataChannels = new ArrayList<>();
		createNextDataFile(newDataChannels, FILE_COPY_PREFIX);

		int prevDataFileNumber = 0;

		try {
			ifc.position(0);
			while (ifc.position() < ifc.size()) {
				Index index = readIndexFromDisk();
				if (!index.isDeleted()) {
					String key = index.getKey();
					indexMap.put(key, appendOnDisk(key, get(key), newIndexChannel, newDataChannels, true));
				}
				if (prevDataFileNumber != index.getFileNumber()) {
					// delete previous data file
					dfcs.get(prevDataFileNumber).close();
					Files.delete(Paths.get(constructDataFileName(prevDataFileNumber, "")));
				}
				prevDataFileNumber = index.getFileNumber();

			}
			// delete previous data file
			dfcs.get(prevDataFileNumber).close();
			Files.delete(Paths.get(constructDataFileName(prevDataFileNumber, "")));

			// rename new data file
			for (int i = 0; i < newDataChannels.size(); i++) {
				newDataChannels.get(i).close();
				Files.move(
						Paths.get(constructDataFileName(i, FILE_COPY_PREFIX)),
						Paths.get(constructDataFileName(i, "")));
			}
			collectDataFiles();

			// delete previous index file
			ifc.close();
			Files.delete(Paths.get(constructIndexFileName("")));

			// rename new index file
			Files.move(Paths.get(constructIndexFileName(FILE_COPY_PREFIX)),
					Paths.get(constructIndexFileName("")));

			ifc = createIndexFile("");

		} catch (IOException | ClassNotFoundException exc) {
			// TODO to write a recovery scenario
			throw new IllegalStateException("An error has occurred during data relocation", exc);
		}

		this.capacity = this.size;
	}

	/**
	 * Collects file channels of all data file stored on a disk
	 * @throws IOException
	 */
	private void collectDataFiles() throws IOException {
		File file = new File(directory);
		if (dfcs != null && !dfcs.isEmpty())
			dfcs.clear();
		dfcs = Arrays
				.stream(file.list((dir, name) -> name.matches(FILE_PREFIX + "_\\d+.dat")))
				.sorted()
				.map(f -> {
					try {
						FileChannel ch = new RandomAccessFile(new File(directory, f), "rw").getChannel();
						ch.force(true);
						return ch;
					} catch (Exception exc) {
						throw new IllegalStateException("Store's data file opening error", exc);
					}
				}).collect(Collectors.toList());
		if (dfcs.isEmpty())
			createNextDataFile(dfcs, "");
	}

	/**
	 * Creates a new data file channel if the last one is filled  
	 * @param dataChannels - list of data file channels
	 * @param suffix - suffix of the created file name associated with the channel
	 */
	private void createNextDataFile(List<FileChannel> dataChannels,
			String suffix) {
		String path = constructDataFileName(dataChannels.size(), suffix);
		try {
			FileChannel ch = new RandomAccessFile(path, "rw").getChannel();
			ch.force(true);
			dataChannels.add(ch);
		} catch (IOException exc) {
			throw new RuntimeException("Unable to open the data file '" + path + "'", exc);
		}
	}

	/**
	 * Constructs the data file name
	 * @param number - ordinal file number
	 * @param suffix - suffix of the file name
	 * @return constructed data file name
	 */
	private String constructDataFileName(int number, String suffix) {
		return Paths.get(directory, FILE_PREFIX + "_" + String.format("%04d", number) + suffix + DFILE_EXT).toAbsolutePath().toString();
	}

	/**
	 * Creates a new index file channel
	 * @param suffix - suffix of the created file name associated with the channel
	 * @return created index file channel
	 */
	private FileChannel createIndexFile(String suffix) {
		String path = constructIndexFileName(suffix);
		try {
			FileChannel ch = new RandomAccessFile(path, "rw").getChannel();
			ch.force(true);
			return ch;
		} catch (IOException exc) {
			throw new RuntimeException("Unable to open the index file '" + path + "'", exc);
		}
	}

	/**
	 * Constructs the index file name
	 * @param suffix - suffix of the file name
	 * @return constructed file name
	 */
	private String constructIndexFileName(String suffix) {
		return Paths.get(directory, FILE_PREFIX + suffix + IFILE_EXT).toAbsolutePath().toString();
	}
}