package it.unibo.scotece.domenico.services;


import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import it.unibo.scotece.domenico.services.protogen.FileSendGrpc;
import it.unibo.scotece.domenico.services.protogen.Files;
import it.unibo.scotece.domenico.services.protogen.File;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class ClientSendFileProto {

    private final ManagedChannel channel;
    private final FileSendGrpc.FileSendBlockingStub blockingStub;
    private Map<String, Double> tosend;

    public ClientSendFileProto(String host, int port, Map<String, Double> tosend){
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext()
                .build(), tosend);
    }

    ClientSendFileProto(ManagedChannel channel, Map<String, Double> tosend) {
        this.channel = channel;
        blockingStub = FileSendGrpc.newBlockingStub(channel);
        this.tosend = tosend;
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void sendFiles() throws IOException {

        var filesbuilder = Files.newBuilder();

        for(var p : this.tosend.entrySet()){
            var filename = p.getKey() + ".archive";
            var file = java.nio.file.Files.readAllBytes(Paths.get(filename));

            filesbuilder.addFiles(File.newBuilder()
                    .setFile(ByteString.copyFrom(file))
                    .setName(filename)
                    .build());

        }

        try {
            var response = blockingStub.sendMany(filesbuilder.build());
        } catch (StatusRuntimeException e) {
            System.out.println("RPC failed: {0} " + e.getStatus());
            return;
        }

    }


}
