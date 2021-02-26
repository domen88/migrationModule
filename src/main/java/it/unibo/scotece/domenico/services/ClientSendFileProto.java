package it.unibo.scotece.domenico.services;


import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import it.unibo.scotece.domenico.services.protogen.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class ClientSendFileProto {

    private final ManagedChannel channel;
    private final FileSendGrpc.FileSendBlockingStub blockingStub;
    private final FileSendGrpc.FileSendStub asyncStub;
    private Map<String, Double> tosend;
    private Map<String, String> hash;

    public ClientSendFileProto(String host, int port){
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext()
                .maxInboundMessageSize(51*1024*1024)
                .build(), null, null);
    }

    public ClientSendFileProto(String host, int port, Map<String, String> hash){
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext()
                .build(), null, hash);
    }


    public ClientSendFileProto(String host, int port, Map<String, Double> tosend, Map<String, String> hash){
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext()
                .build(), tosend, hash);

    }

    ClientSendFileProto(ManagedChannel channel, Map<String, Double> tosend, Map<String, String> hash) {
        this.channel = channel;
        blockingStub = FileSendGrpc.newBlockingStub(channel);
        asyncStub = FileSendGrpc.newStub(channel);
        this.tosend = tosend;
        this.hash = hash;
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void sendFiles() throws IOException {

        var filesbuilder = Files.newBuilder();

        for(var p : this.tosend.entrySet()){
            var filename = p.getKey() + ".archive";
            var file = java.nio.file.Files.readAllBytes(Paths.get(filename));
            System.out.println("Invio File: " + filename + " size: " + file.length);

            filesbuilder.addFiles(File.newBuilder()
                    .setFile(ByteString.copyFrom(file))
                    .setName(filename)
                    .setHash(hash.get(p.getKey()))
                    .build());
        }

        try {
            var response = blockingStub.sendMany(filesbuilder.build());
        } catch (StatusRuntimeException e) {
            System.out.println("RPC failed: {0} " + e.getStatus());
            return;
        }

    }

    public void sendResidualFiles() throws IOException {

        var filesbuilder = Files.newBuilder();

        for(var p : this.tosend.entrySet()){
            var filename = p.getKey() + ".archive";
            var file = java.nio.file.Files.readAllBytes(Paths.get(filename));
            System.out.println("Invio File: " + filename + " size: " + file.length);

            filesbuilder.addFiles(File.newBuilder()
                    .setFile(ByteString.copyFrom(file))
                    .setName(filename)
                    .setHash(" ")
                    .build());
        }

        try {
            var response = blockingStub.sendMany(filesbuilder.build());
        } catch (StatusRuntimeException e) {
            System.out.println("RPC failed: {0} " + e.getStatus());
            return;
        }

    }

    public void sendFile(String filename) throws IOException {
        var filebuilder = File.newBuilder();
        var file = java.nio.file.Files.readAllBytes(Paths.get(filename));

        filebuilder.setFile(ByteString.copyFrom(file))
                .setName(filename)
                .setHash("");
        try {
            var response = blockingStub.send(filebuilder.build());
        } catch (StatusRuntimeException e) {
            System.out.println("RPC failed: {0} " + e.getStatus());
            return;
        }
    }

    public void sendChunkFile(String filename, String user) throws IOException, InterruptedException {
        final CountDownLatch finishLatch = new CountDownLatch(1);

        StreamObserver<Empty> responseObserver = new StreamObserver<Empty>() {
            @Override
            public void onNext(Empty value) {

            }

            @Override
            public void onError(Throwable t) {
                System.out.println("StreamObserver<Empty> responseObserver: onError");
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                System.out.println("StreamObserver<Empty> responseObserver: onCompleted");
                finishLatch.countDown();
            }
        };

        StreamObserver<FileChunk> requestObserver = asyncStub.sendChunks(responseObserver);

        var filechunk = FileChunk.newBuilder();
        final String userpath = "/home/" + user + "/rec";
        final String currentUsersHomeDir = Paths.get(userpath).toAbsolutePath().normalize().toString() + "/";
        //var file = java.nio.file.Files.readAllBytes(Paths.get(filename));
        //filename = "backup1.tar";

        final int BUFFER_SIZE = 50*1024*1024;
        FileInputStream fis = new FileInputStream(currentUsersHomeDir+filename);
        byte[] buffer = new byte[BUFFER_SIZE];
        int read = 0;
        System.out.println("Invio File: " + filename);
        try {

            while( ( read = fis.read(buffer) ) > 0 ) {
                byte[] b = Arrays.copyOfRange(buffer, 0, read);

                System.out.println("Invio tot byte: " + b.length);
                filechunk.setChunk(ByteString.copyFrom(b))
                        .setName(filename);

                requestObserver.onNext(filechunk.build());

                // Sleep for a bit before sending the next one.
                Thread.sleep(500);
                if (finishLatch.getCount() == 0) {
                    // RPC completed or errored before we finished sending.
                    // Sending further requests won't error, but they will just be thrown away.
                    return;
                }

            }

        } catch (InterruptedException e) {
            // Cancel RPC
            System.out.println("onError");
            requestObserver.onError(e);
            throw e;
        }

        fis.close();
        System.out.println("onCompleted");
        requestObserver.onCompleted();

        // Receiving happens asynchronously
        if (!finishLatch.await(60, TimeUnit.MINUTES)) {
            System.out.println("recordRoute can not finish within 10 minutes");
        }

    }

    public void sendHash(){

        var hashbuilder = HashCollections.newBuilder();

        for(var h : this.hash.entrySet()){

            hashbuilder.addHcollections(HashCollection.newBuilder()
                    .setName(h.getKey())
                    .setHash(h.getValue())
                    .build());
        }

        try {
            var response = blockingStub.sendHash(hashbuilder.build());
        } catch (StatusRuntimeException e) {
            System.out.println("RPC failed: {0} " + e.getStatus());
            return;
        }

    }
}
