package task.store;

/**
 * Appendable-only object Store
 * 
 * @author Fedor Trofimov
 * @param <T> The type of a value in the Store
 */
public interface AppendableStore<T> {
	
	public void append(String key, T value);
	
	public T get(String key);
	
	public boolean remove(String key);
	
	public String generateKey();

}
