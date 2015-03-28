package rpc.thrift;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Try;

import java.util.Timer;
import java.util.TimerTask;

// TODO using non-blocking server instead of TThreadPoolServer; set client timeout
public class PersistentStorageServer {
    private TServer server;
    private int port;
    private String db;
    private String coll;
    private int period;
    private Timer timer;
    private Logger logger;
    private Iterable<Try<HintedHandoffService.Value>> curHintedServiceResult;

    /**
     * @param port
     * @param db
     * @param coll
     * @param period scan period for hinted handoff
     */
    public PersistentStorageServer(int port, String db, String coll, int period) {
        this.port = port;
        this.db = db;
        this.coll = coll;
        this.period = period;
        this.timer = new Timer(true); // set as daemon
        this.logger = LoggerFactory.getLogger(this.getClass());
    }

    public void start() {
        HintedHandoffService hintedHandoffService = new HintedHandoffService(db + "hh", coll);
        PersistentStorageServiceHandler handler = PersistentStorageServiceHandler
                .createFromMapDB(db, coll)
                .withHintedHandoffService(hintedHandoffService);

        startHintedHandoffService(hintedHandoffService);

        try {
            startRPCService(handler);
        } catch (TTransportException e) {
            logger.warn("could not start RPC service. shutting down all starting services now...", e);
            timer.cancel();
        }
    }

    /**
     * start hinted hand-off service in separate thread
     * TODO monitor the service and restart it when it fails. To enable "shutdown". To enable log.
     * @param service
     */
    private void startHintedHandoffService(HintedHandoffService service) {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                curHintedServiceResult = service.call();
                curHintedServiceResult.forEach(r -> {
                    r.onFailure(e -> {
                        logger.warn(e.getMessage(), e);
                    });
                });
            }
        }, 0, period);
    }

    /**
     *  NOTE it blocks current thread
     */
    private void startRPCService(LayerService.Iface handler) throws TTransportException {
        LayerService.Processor processor = new LayerService.Processor(handler);
        TServerTransport serverTransport = new TServerSocket(port);
        server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
        server.serve();
    }

    public void stop() {
        if(server != null) {
            timer.cancel();
            server.stop();
        }
    }

    public Iterable<Try<HintedHandoffService.Value>> hintedServiceStatus() {
        return curHintedServiceResult;
    }
}
