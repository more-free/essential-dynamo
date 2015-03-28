package db;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

public interface KeyValueDBClient <K, V extends Serializable> {
    void put(K key, V value);
    V get(K key);
    V delete(K key);
    Set<K> keys();
    Collection<V> values();
}
