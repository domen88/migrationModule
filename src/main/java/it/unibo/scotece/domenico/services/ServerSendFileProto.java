package it.unibo.scotece.domenico.services;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import it.unibo.scotece.domenico.services.protogen.*;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ServerSendFileProto {

    private static Server server;
    private static Map<String, String> hash;
    private static Map<String, String> hashcollections;
    private static String filename;

    public void start() throws IOException {
        /* The port on which the server should run */
        int port = 9000;
        var filesendimpl = new FileSendImpl();
        server = ServerBuilder.forPort(port)
                .addService(filesendimpl)
                .maxInboundMessageSize(51*1024*1024)
                .build()
                .start();

        hash = new HashMap<>();
        hashcollections = new HashMap<>();

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

    public static Map<String, String> getHash() {
        return hash;
    }

    public static Map<String, String> getHashCollections() {
        return hashcollections;
    }

    public static String getFilename(){
        return filename;
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

            Empty empty = Empty.newBuilder().build();

            var file = request.getFile();
            var filename = request.getName();

            try {
                java.nio.file.Files.write(Paths.get(filename), file.toByteArray());
            } catch (IOException e) {
                e.printStackTrace();
            }

            responseObserver.onNext(empty);
            responseObserver.onCompleted();
            ServerSendFileProto.server.shutdown();

        }

        @Override
        public StreamObserver<FileChunk> sendChunks(StreamObserver<Empty> responseObserver) {

            return new StreamObserver<FileChunk>() {

                Instant start = Instant.now();

                @Override
                public void onNext(FileChunk value) {
                    var filename = value.getName();
                    Path filepath = Paths.get(filename);
                    ServerSendFileProto.filename = filename;
                    System.out.println("Ricevuto pezzo di file: " + value.getName());
                    try {
                        if (!java.nio.file.Files.exists(filepath)){
                            System.out.println("FILE NON ESISTE " + value.getChunk().toByteArray().length);
                            java.nio.file.Files.write(filepath, value.getChunk().toByteArray(), StandardOpenOption.CREATE);
                        } else {
                            System.out.println("FILE ESISTE--- scrivo tot byte " + value.getChunk().toByteArray().length);
                            java.nio.file.Files.write(filepath, value.getChunk().toByteArray(), StandardOpenOption.APPEND);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }


                }

                @Override
                public void onError(Throwable t) {
                    System.out.println("onError");
                    System.out.println("ERROR: " + t.getMessage());
                }

                @Override
                public void onCompleted() {
                    Instant stop = Instant.now();
                    System.out.println("DURATION: onCompleted "  + Duration.between(start, stop));
                    responseObserver.onNext(Empty.newBuilder().build());
                    responseObserver.onCompleted();
                    ServerSendFileProto.server.shutdown();

                }
            };
        }

        @Override
        public void sendMany(Files request, StreamObserver<Empty> responseObserver) {

            Instant start = Instant.now();
            ServerSendFileProto.hash = new HashMap<>();
            Empty empty = Empty.newBuilder().build();

            for (int i = 0; i < request.getFilesCount(); i++){
                var file = request.getFiles(i);
                var filename = file.getName();
                var name = filename.substring(0, filename.lastIndexOf("."));
                ServerSendFileProto.hash.put(name, file.getHash());
                try {
                    //System.out.println("Ricevuto File: " + filename + " size: " + file.getFile());
                    file.getNameBytes().toByteArray();
                    java.nio.file.Files.write(Paths.get(filename), file.getFile().toByteArray());
                    //java.nio.file.Files.copy(file.getFile().newInput(), Paths.get(filename));
                } catch (IOException e) {
                   throw new RuntimeException();
                }
            }

            responseObserver.onNext(empty);
            responseObserver.onCompleted();
            ServerSendFileProto.server.shutdown();
            Instant stop = Instant.now();
            System.out.println("DURATION: sendMany Procedure "  + Duration.between(start, stop));
        }

        @Override
        public void sendHash(HashCollections request, StreamObserver<Empty> responseObserver) {

            Instant start = Instant.now();
            ServerSendFileProto.hashcollections = new HashMap<>();
            Empty empty = Empty.newBuilder().build();

            for (int i = 0; i < request.getHcollectionsCount(); i++){
                var h = request.getHcollections(i);
                ServerSendFileProto.hashcollections.put(h.getName(), h.getHash());
            }

            responseObserver.onNext(empty);
            responseObserver.onCompleted();
            ServerSendFileProto.server.shutdown();
            Instant stop = Instant.now();
            System.out.println("DURATION: sendHash Procedure "  + Duration.between(start, stop));

        }
    }
}
