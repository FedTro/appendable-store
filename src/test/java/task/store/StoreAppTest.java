package task.store;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import task.store.testobjects.ClippedText;

public class StoreAppTest {

	private static final String statement = "Java is a general-purpose computer-programming language that is concurrent, class-based, object-oriented, and specifically designed to have as few implementation dependencies as possible. ";

	private static final int N = 100000;
	
	public static final String path = "tmp/";
	public static Store<ClippedText> store;
	
	@BeforeClass
	public static void init() throws ClassNotFoundException, IOException {
		File dir = new File(path);
		if(dir.exists()){
			for(File f: dir.listFiles()){
				assertTrue(f.delete());
			}
		} else {
			dir.mkdir();
		}
		store = new Store<ClippedText>(path);
	}

	@Test
	public void testStore() throws ClassNotFoundException, IOException, NoSuchAlgorithmException {
		
		Random random = new Random();
		
		String key = null;
		ClippedText value = null;
		
		Map<String, Long> checksumMap = new HashMap<>();
		List<String> keys = new ArrayList<>();
		
		// 1. generate N records with a different size
		for(int i = 0; i< N; i++) {
			int lowerLimit = random.nextInt(statement.length()/2);
			int upperLimit = statement.length()/2 + random.nextInt(statement.length()/2);
			key = store.generateKey();
			value = new ClippedText(key, statement.substring(lowerLimit, upperLimit));
			// 2. calculate records checksum
			checksumMap.put(key, getChecksum(value));
			keys.add(key);
			// 3. put records into the Store
			store.append(key, value);
		}
		
		// 4. read some record from the Store
		int recordIndex = random.nextInt(keys.size());
		key = keys.get(recordIndex);
		value = store.get(key);
		// 4.1 compare a checksum
		assertEquals((long) checksumMap.get(key), getChecksum(value));
		
		recordIndex = random.nextInt(keys.size());
		key = keys.get(recordIndex);
		value = store.get(key);
		// 4.1 compare the checksum
		assertEquals((long) checksumMap.get(key), getChecksum(value));
		
		// 4.2 estimate reading speed
		long startTime = System.currentTimeMillis();
		int n = 1000 < N ? 1000 : N;
		for(int i = 0; i < n; i++) {
			store.get(keys.get(random.nextInt(keys.size())));
		}
		long stopTime = System.currentTimeMillis();
		System.out.println(String.format("Reading speed: %d records in %d ms", n, (stopTime-startTime)));
		
		
		// 5. delete approximately one half of the records according to the uniform distribution of random.nextBoolean() method
		Iterator<String> it = keys.iterator();
		while (it.hasNext()) {
		   key = it.next();
		   if(random.nextBoolean()){
			   store.remove(key);
			   checksumMap.remove(key);
			   it.remove();
			}
		}
		
		System.out.println("Number of remaining records: " + keys.size());
		
		// 6. check whether the deletion has broken access to the remaining records 
		for(String k: keys) 
			assertEquals((long) checksumMap.get(k), getChecksum(store.get(k)));
		
	}
	
	private static long getChecksum(Object obj) throws IOException, NoSuchAlgorithmException {
	
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(obj);
		oos.close();

		byte bytes[] = baos.toByteArray();
        Checksum checksum = new CRC32();
        checksum.update(bytes, 0, bytes.length);
        return checksum.getValue();
	}
	
	@AfterClass
	public static void end() {
		store.close();
	}
	
}
