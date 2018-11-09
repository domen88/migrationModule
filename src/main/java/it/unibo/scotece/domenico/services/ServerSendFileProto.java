package it.unibo.scotece.domenico.services;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import it.unibo.scotece.domenico.services.protogen.Empty;
import it.unibo.scotece.domenico.services.protogen.FileSendGrpc;
import it.unibo.scotece.domenico.services.protogen.Files;

import java.io.IOException;
import java.nio.file.Paths;

public class ServerSendFileProto {

    private Server server;

    public void start() throws IOException {
        /* The port on which the server should run */
        int port = 9000;
        server = ServerBuilder.forPort(port)
                .addService(new FileSendImpl())
                .build()
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                ServerSendFileProto.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    static class FileSendImpl extends FileSendGrpc.FileSendImplBase {

        @Override
        public void send(it.unibo.scotece.domenico.services.protogen.File request,
                         io.grpc.stub.StreamObserver<it.unibo.scotece.domenico.services.protogen.Empty> responseObserver) {

        }

        @Override
        public void sendMany(Files request, StreamObserver<Empty> responseObserver) {

            Empty empty = Empty.newBuilder().build();

            for (int i = 0; i < request.getFilesCount(); i++){
                var file = request.getFiles(i);
                var filename = file.getName() + ".archive";
                try {
                    java.nio.file.Files.write(Paths.get(filename), file.getNameBytes().toByteArray());
                } catch (IOException e) {
                   throw new RuntimeException();
                }
            }

            responseObserver.onNext(empty);
            responseObserver.onCompleted();

        }


    }
}
