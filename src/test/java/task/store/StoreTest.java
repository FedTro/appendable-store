package task.store;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import task.store.testobjects.Car;

public class StoreTest {

	public static final String path = "tmp/";
	public static Store<Car> store;

	@BeforeClass
	public static void init() throws ClassNotFoundException, IOException {
		File dir = new File(path);
		if(!dir.exists())
			dir.mkdir();
		else
			deleteDirContent(dir);
		store = new Store<Car>(path);
	}

	@Test
	public void testAppend() throws ClassNotFoundException, IOException {
		Car car = new Car("Kia", "Rio", 2016);
		store.append("1", car);
		Car savedCard = store.get("1");
		assertNotNull(savedCard);
		assertEquals("Kia", savedCard.brand);
		assertEquals("Rio", savedCard.model);
		assertEquals(2016, savedCard.year);
	}

	@Test
	public void testRemove() throws IOException, ClassNotFoundException {
		Car car = new Car("Kia", "Optima", 2017);
		store.append("2", car);
		Car savedCard = store.get("2");
		assertNotNull(savedCard);
		store.remove("2");
		assertNotNull(savedCard);
	}

	@Test
	public void testGenerateKey() {
		assertNotNull(store.generateKey());
	}

	@Test(expected = RuntimeException.class)
	public void testInitializationInNonexistentDir() {
		new Store<Car>("abc/");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInitializtionWithIllegalLoadFactor() {
		new Store<Car>("tmp/", 0.00f);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDuplicatesAppending() {
		String key = store.generateKey();
		store.append(key, new Car("BMW", "M5", 2014));
		store.append(key, new Car("BMW", "X5", 2014));
	}

	@Test(expected = RuntimeException.class)
	public void testAccessToClosedStore() {
		String dirName = "tmp2/";
		File dir = new File(dirName);
		if (!dir.exists()) {
			dir.mkdir();
		} else {
			deleteDirContent(dir);
		}
		Store<Car> s = new Store<>(dirName);
		String key = store.generateKey();
		s.append(key, new Car("BMW", "M5", 2014));
		s.close();
		deleteDirContent(dir);
		dir.delete();
		s.get(key);
	}

	@AfterClass
	public static void end() {
		store.close();
	}

	private static void deleteDirContent(File dir) {
		for (File f : dir.listFiles()) {
			assertTrue(f.delete());
		}
	}

}
