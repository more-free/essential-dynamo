package rpc.thrift;

import db.KeyValueDBClient;
import db.MapDBClient;
import org.apache.thrift.TException;
import utils.Failure;
import utils.Success;
import utils.Try;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class HintedHandoffService implements Callable<Iterable<Try<HintedHandoffService.Value>>> {
    private KeyValueDBClient<Key, Value> dbClient;

    /**
     * @param db
     * @param collection
     */
    public HintedHandoffService(String db, String collection) {
        this.dbClient = new MapDBClient(db).getCollectionBuilder(collection).withAutoCommit();
    }

    public void put(Key key, Value value) {
        dbClient.put(key, value);
    }

    public Value get(Key key) {
        return dbClient.get(key);
    }

    public List<Versioned> get(String key) {
        List<Versioned> list = new ArrayList<>();
        dbClient.keys().stream().filter(k -> k.key.equals(key)).forEach(k -> {
            Value v = dbClient.get(k);
            list.add(toVersioned(v.value, k.version));
        });
        return list;
    }

    @Override
    public Iterable<Try<Value>> call() {
        List<Try<Value>> res = new ArrayList<>();
        dbClient.keys().forEach(key -> {
            try {
                Value value = dbClient.get(key);
                PersistentStorageClient storageClient = new PersistentStorageClient(value.host, value.port);
                storageClient.put(key.key, toVersioned(value.value, key.version));
                res.add(new Success(dbClient.delete(key)));
            } catch (TException | IOException cause) {
                res.add(new Failure(new RuntimeException(
                        "could not connect to client. perhaps it is still down. auto-retry next time",
                        cause)));
            }
            // expose (do not catch) all other potential runtime exceptions as bugs
        });

        return res;
    }

    public Versioned toVersioned(byte [] value, String version) {
        return new Versioned(ByteBuffer.wrap(value), version);
    }

    public static class Key implements Serializable {
        private String key;
        private String version;

        public Key(String key, String version) {
            this.key = key;
            this.version = version;
        }

        public int hashCode() {
            return key.hashCode() * 37 + version.hashCode();
        }

        public boolean equals(Object that) {
            if(that instanceof Key) {
                Key thatKey = (Key) that;
                return thatKey.key.equals(key) && thatKey.version.equals(version);
            }
            return false;
        }
    }

    public static class Value implements Serializable {
        private String host;
        private int port;
        private byte [] value;

        public Value(String host, int port, byte [] value) {
            this.host = host;
            this.port = port;
            this.value = value;
        }
    }
}
