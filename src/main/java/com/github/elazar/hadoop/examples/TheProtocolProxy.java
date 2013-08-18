package com.github.elazar.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * An example of proper usage of Hadoop RPC
 */
public class TheProtocolProxy {
    @ProtocolInfo(protocolName = "ping", protocolVersion = 42)
    public static interface PingProtocol extends VersionedProtocol {

        public abstract String ping();
    }

    public static class Ping implements PingProtocol {
        @Override
        public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
            return RPC.getProtocolVersion(PingProtocol.class);
        }

        @Override
        public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) throws IOException {
            return ProtocolSignature.getProtocolSignature(clientMethodsHash,
                    RPC.getProtocolVersion(PingProtocol.class),
                    PingProtocol.class);
        }

        public String ping() {
            return "pong";
        }
    }
    public static void main( String[] args ) throws IOException {
        final RPC.Server server = new RPC.Builder(new Configuration()).
                setInstance(new Ping()).
                setProtocol(PingProtocol.class).
                build();
        ExecutorService svc = Executors.newCachedThreadPool();
//        server.start();

        final ProtocolProxy<PingProtocol> protocolProxy = RPC.getProtocolProxy(PingProtocol.class,
                RPC.getProtocolVersion(PingProtocol.class),
                server.getListenerAddress(), new Configuration());
        System.out.println("method: " + protocolProxy.isMethodSupported("ping"));

        final PingProtocol proxy = RPC.getProxy(PingProtocol.class, RPC.getProtocolVersion(PingProtocol.class),
                server.getListenerAddress(), new Configuration());
        System.out.println("Server: ping " + proxy.ping());
        System.out.println( "Hello World!" );
        server.stop();
    }
}
