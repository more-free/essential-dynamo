package rpc.thrift;


import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TestProtocol {
    @Test
    public void testPersistentStorageProtocol() throws Exception {
        /*
        final int port = 9876;
        final String host = "127.0.0.1";
        final PersistentStorageServer server = new PersistentStorageServer(port);
        new Thread(() -> { try { server.start(); } catch(Exception e) {} } ).start();

        TimeUnit.SECONDS.sleep(3);

        PersistentStorageClient client = new PersistentStorageClient(host, port);
        client.get("some", null);

        TimeUnit.SECONDS.sleep(1);
        server.stop();
        */
    }
}
