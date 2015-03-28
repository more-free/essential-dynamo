package rpc.thrift;

import db.KeyValueDBClient;
import db.MapDBClient;
import org.apache.thrift.TException;

import java.util.*;

/**
 * TODO maybe change to AsyncIface; more decoupled with low-layer storage APIs;
 * TODO put / get operations are both inefficient.
 */
public class PersistentStorageServiceHandler implements LayerService.Iface {
    private KeyValueDBClient<String, ArrayList<VersionedStorageUnit>> dbClient;
    private HintedHandoffService hintedHandoffService;

    /**
     * @param db
     * @param collection
     * @return
     */
    public static PersistentStorageServiceHandler createFromMapDB(String db, String collection) {
        return new PersistentStorageServiceHandler(
            new MapDBClient(db)
                .<String, ArrayList<VersionedStorageUnit>> getCollectionBuilder(collection)
                .withAutoCommit()
        );
    }

    public PersistentStorageServiceHandler withHintedHandoffService(HintedHandoffService service) {
        this.hintedHandoffService = service;
        return this;
    }

    private PersistentStorageServiceHandler(KeyValueDBClient<String, ArrayList<VersionedStorageUnit>> dbClient) {
        this.dbClient = dbClient;
    }

    @Override
    public void put(String key, Versioned versioned) throws TException {
        ArrayList<VersionedStorageUnit> existingUnits = dbClient.get(key);
        if(existingUnits == null)
            existingUnits = new ArrayList<>();
        existingUnits.add(new VersionedStorageUnit(versioned));

        dbClient.put(key, existingUnits);
    }

    @Override
    public List<Versioned> get(String key) throws TException {
        List<Versioned> list = new ArrayList<>();
        dbClient.get(key).stream().map(v -> v.toVersioned()).forEach(v -> list.add(v));

        // search from database for hinted hand-off
        if(list.isEmpty())
            list.addAll(hintedHandoffService.get(key));

        return list;
    }

    @Override
    public void hintedHandoff(String key, Versioned versioned, Node origin) throws TException {
        put(key, versioned);
        HintedHandoffService.Key hk = new HintedHandoffService.Key(key, versioned.version);
        HintedHandoffService.Value hv = new HintedHandoffService.Value(origin.host, origin.port, versioned.getValue());
        hintedHandoffService.put(hk, hv);
    }
}
