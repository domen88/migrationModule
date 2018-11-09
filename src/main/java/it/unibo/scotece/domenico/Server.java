package it.unibo.scotece.domenico;

import it.unibo.scotece.domenico.services.ServerSendFileProto;

import java.io.IOException;

public class Server {

    public static void main(String[] args) throws IOException, InterruptedException {
        final ServerSendFileProto server = new ServerSendFileProto();
        server.start();
        server.blockUntilShutdown();
    }
}
