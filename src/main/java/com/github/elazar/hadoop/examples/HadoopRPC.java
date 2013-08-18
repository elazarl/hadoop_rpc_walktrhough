package com.github.elazar.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Hello world!
 *
 */
public class HadoopRPC
{
    @ProtocolInfo(protocolName = "ping", protocolVersion = 1)
    public static interface PingProtocol  {
        String ping();
    }

    public static class Ping implements PingProtocol {
        public String ping() {
            System.out.println("Server: ");
            return "pong";
        }
    }

    public static InetSocketAddress addr = new InetSocketAddress("localhost", 5121);

    public static RPC.Server server() throws IOException {
        final RPC.Server server = new RPC.Builder(new Configuration()).
                setBindAddress(addr.getHostName()).
                setPort(addr.getPort()).
                setInstance(new Ping()).
                setProtocol(PingProtocol.class).
                build();
        server.start();
        return server;
    }

    public static void client() throws IOException {
        final PingProtocol proxy = RPC.getProxy(PingProtocol.class, RPC.getProtocolVersion(PingProtocol.class),
                addr, new Configuration());
        System.out.println("Client: ping " + proxy.ping());
    }

    public static void main(String[] args ) throws IOException {
        final String runThis = args.length > 0 ? args[0] : "";
        if (runThis.equals("server")) {
            server();
        } else if (runThis.equals("client")) {
            client();
        } else {
            final RPC.Server server = server();
            client();
            server.stop();
        }
    }
}
