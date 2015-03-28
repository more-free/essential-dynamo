package db;

import org.mapdb.*;
import java.io.File;
import java.io.Serializable;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * An agile choice for small-scale key-value storage.
 * Use it as local key-value storage. It only provides simple wrappers of the
 * original synchronous APIs. More efficient client APIs are provided in scala directory
 * with actor/future-based asynchronous manner.
 */
public class MapDBClient {
    private DB db;
    private String dbPath;

    public MapDBClient(String dbPath) {
        this.dbPath = dbPath;
        this.db = DBMaker.newFileDB(new File(dbPath)).closeOnJvmShutdown().make();
    }

    public <K, V extends Serializable> CollectionBuilder getCollectionBuilder(String collectionName) {
        return new CollectionBuilder<K, V>(db, collectionName);
    }

    public void commit() {
        db.commit();
    }

    public void rollback() {
        db.rollback();
    }

    public void close() {
        db.close();
    }

    public static class CollectionBuilder <K, V extends Serializable> implements KeyValueDBClient <K, V> {
        private DB db;
        private ConcurrentNavigableMap <K, V> map;
        private boolean autoCommit;

        CollectionBuilder(DB db, String name) {
            this.db = db;
            this.map = this.db.getTreeMap(name);
            autoCommit = false;
        }

        public CollectionBuilder withAutoCommit() {
            autoCommit = true;
            return this;
        }

        @Override
        public void put(K key, V value) {
            map.put(key, value);
            if(autoCommit)
                db.commit();
        }

        @Override
        public V get(K key) {
            return map.get(key);
        }

        @Override
        public V delete(K key) {
            V value = map.remove(key);
            if(autoCommit)
                db.commit();
            return value;
        }

        @Override
        public Set<K> keys() {
            return map.keySet();
        }

        @Override
        public Collection<V> values() {
            return map.values();
        }
    }
}

