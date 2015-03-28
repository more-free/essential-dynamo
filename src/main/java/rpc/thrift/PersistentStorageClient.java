package rpc.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.*;

import java.io.IOException;
import java.util.List;

// TODO using Async client + non-blocking server
public class PersistentStorageClient {
    private String host;
    private int port;
    private TBinaryProtocol.Factory factory;
    // private TAsyncClientManager manager = new TAsyncClientManager();

    public PersistentStorageClient(String host, int port) {
        this.host = host;
        this.port = port;
        factory = new TBinaryProtocol.Factory();
    }

    public void put(String key, Versioned versioned) throws IOException, TException {
        TTransport transport = new TSocket(host, port);
        transport.open();
        TProtocol protocol = new  TBinaryProtocol(transport);
        LayerService.Client client = new LayerService.Client(protocol);
        client.put(key, versioned);
        transport.close();
    }

    public List<Versioned> get(String key) throws IOException, TException {
        TTransport transport = new TSocket(host, port);
        transport.open();
        TProtocol protocol = new  TBinaryProtocol(transport);
        LayerService.Client client = new LayerService.Client(protocol);
        List<Versioned> list = client.get(key);
        transport.close();

        return list;
    }

    public void hintedHandoff(String key, Versioned versioned, String host, int port) throws IOException, TException {
        TTransport transport = new TSocket(host, port);
        transport.open();
        TProtocol protocol = new  TBinaryProtocol(transport);
        LayerService.Client client = new LayerService.Client(protocol);
        client.hintedHandoff(key, versioned, new Node(host, port));
        transport.close();
    }

    /*
    public void putAsync(String key, Versioned versioned,
                    AsyncMethodCallback<LayerService.AsyncClient.put_call> callback)
            throws IOException, TException {
        createClient().put(key, versioned, callback);
    }

    public void getAsync(String key,
                    AsyncMethodCallback<LayerService.AsyncClient.get_call> callback)
            throws IOException, TException {
        createClient().get(key, callback);
    }

    private LayerService.AsyncClient createClient() throws IOException {
        return new LayerService.AsyncClient(factory, manager, new TNonblockingSocket(host, port));
    }
    */
}