package it.unibo.scotece.domenico;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.ContainerNotFoundException;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.HostConfig;
import it.unibo.scotece.domenico.services.ServerSendFileProto;
import it.unibo.scotece.domenico.services.impl.DockerConnectImpl;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class Server {

    public static void main(String[] args) throws IOException, InterruptedException, DockerCertificateException, DockerException, ExecutionException {
        //Docker
        DockerConnectImpl currentDockerConnector =  new DockerConnectImpl();
        DockerClient docker = currentDockerConnector.setConnection();

        //User
        if (args.length <= 0){
            System.out.println("Usage jar [user]");
            System.exit(0);
        }
        final String user = args[0];

        final ServerSendFileProto server = new ServerSendFileProto();
        server.start(user);
        server.blockUntilShutdown();
        System.out.println("----RESTORE REATTIVA----");



        /**-----REACTIVE-----**/
        Instant start = Instant.now();
        restore(docker, "dbdata", user);
        Instant stop = Instant.now();
        System.out.println("DURATION: restore REACTIVE APPLICATION AGNOSTIC "  + Duration.between(start, stop));
        currentDockerConnector.close();


        /*---RESTORE DUMP UNICO----*/
        /*Instant start = Instant.now();
        var filename = ServerSendFileProto.getFilename();
        System.out.println("FILENAME "  + filename);
        restore(filename);
        Instant stop = Instant.now();
        System.out.println("DURATION: restore REACTIVE APPLICATION AGNOSTIC "  + Duration.between(start, stop));*/

        /*----RESTORE---*/
        /*var hash = ServerSendFileProto.getHashCollections();

        //Restart server
        server.start();
        server.blockUntilShutdown();

        Instant start = Instant.now();
        var filename = ServerSendFileProto.getFilename();
        System.out.println("FILENAME "  + filename);
        restore(filename, hash);
        Instant stop = Instant.now();
        System.out.println("DURATION: restore proattivo "  + Duration.between(start, stop));

        //Restart server
        server.start();
        server.blockUntilShutdown();

        System.out.println("HANDOFF!!!");

        start = Instant.now();
        var hashrestore = ServerSendFileProto.getHashCollections();
        var residual = new HashMap<String, String>();
        createResidualCollectionsDump(hashrestore, hash, residual);
        stop = Instant.now();
        System.out.println("DURATION: createResidualCollectionsDump "  + Duration.between(start, stop));

        //Restart server
        server.start();
        server.blockUntilShutdown();

        start = Instant.now();
        filename = ServerSendFileProto.getFilename();
        System.out.println("FILENAME "  + filename);
        restore(filename, residual);
        stop = Instant.now();
        System.out.println("DURATION: restore after handoff "  + Duration.between(start, stop));*/



        /*

        /*---RESTORE---
        var hash = ServerSendFileProto.getHash();
        Instant start = Instant.now();
        restore(hash);
        Instant stop = Instant.now();
        System.out.println("DURATION: restore Hash Procedure "  + Duration.between(start, stop));

        //Restart server
        server.start();
        server.blockUntilShutdown();

        /*---RESTORE ALL---
        hash = ServerSendFileProto.getHash();
        start = Instant.now();
        restore(hash);
        stop = Instant.now();
        System.out.println("DURATION: restore All Procedure "  + Duration.between(start, stop));
        */

        //Restart server
        //server.start();
        //server.blockUntilShutdown();

        /*---CHECK HASH---*/
        //var hashcollections = ServerSendFileProto.getHashCollections();
        start = Instant.now();
        //checkHash(hash, hashrestore);
        stop = Instant.now();
        //System.out.println("DURATION: checkHash Procedure "  + Duration.between(start, stop));

        //Restart server
        //server.start();
        //server.blockUntilShutdown();

        start = Instant.now();
        //filename = ServerSendFileProto.getFilename();
        //System.out.println("FILENAME "  + filename);
        //restore(filename);
        stop = Instant.now();
        //System.out.println("DURATION: restore miss "  + Duration.between(start, stop));

        /*----START DOWNSTREAM------*/
        String userpath = "/home/" + user + "/rec";
        String currentUsersHomeDir = Paths.get(userpath).toAbsolutePath().normalize().toString();
        System.out.println("USER PATH: " + currentUsersHomeDir);
        Process process = Runtime.getRuntime().exec(new String[]{"sh", currentUsersHomeDir + "/ping-reply.sh"});
        //process.waitFor();
        System.out.println("PING STARTED - PROCESS ID: " + process.pid());

        System.exit(0);

    }

    private static void restore(Map<String, String> restore) throws InterruptedException, IOException {

        for (var set : restore.entrySet()) {
            System.out.println("Restore collection " + set.getKey());
            var process = Runtime.getRuntime().exec(new String[]{"sh", "-c", "mongorestore --archive=" + set.getKey() + ".archive --db testdb"});
            process.waitFor();
            //System.out.println("Restore finished");
        }

    }

    private static void restore(String filename) throws InterruptedException, IOException {

        InputStream io = java.nio.file.Files.newInputStream(Paths.get(filename));
        ArchiveInputStream archive = new TarArchiveInputStream(io);
        ArchiveEntry entry = null;
        String name = "";

        while ((entry = archive.getNextEntry()) != null){
            if (!archive.canReadEntryData(entry)){
                continue;
            }

            name = entry.getName();
            File f = new File(name);

            try (OutputStream o = java.nio.file.Files.newOutputStream(f.toPath())) {
                IOUtils.copy(archive, o);
            }
            System.out.print(" Rest coll " + name);
            var process = Runtime.getRuntime().exec(new String[]{"sh", "-c", "mongorestore --archive=" + name + " --db testdb"});
            process.waitFor();

        }




    }

    private static void restore(String filename, Map<String, String> restore) throws InterruptedException, IOException {

        System.out.println("Dentro restore");
        InputStream io = java.nio.file.Files.newInputStream(Paths.get(filename));
        ArchiveInputStream archive = new TarArchiveInputStream(io);
        ArchiveEntry entry = null;

        while ((entry = archive.getNextEntry()) != null){
            if (!archive.canReadEntryData(entry)){
                continue;
            }

            String name = entry.getName();
            System.out.println("String name = " + name);
            File f = new File(name);

            try (OutputStream o = java.nio.file.Files.newOutputStream(f.toPath())) {
                IOUtils.copy(archive, o);
                //System.out.println("IOUtils.copy");
            }
        }

        io.close();

        for (var set : restore.entrySet()) {
            System.out.print(" Rest coll " + set.getKey());
            var process = Runtime.getRuntime().exec(new String[]{"sh", "-c", "mongorestore --archive=" + set.getKey() + ".archive --db testdb"});
            process.waitFor();
            //System.out.println("Restore finished");
        }

    }

    private static void restore(DockerClient docker, String volumesFrom, String user) throws DockerException, InterruptedException, ExecutionException {
        //Pull latest ubuntu images from docker hub
        docker.pull("busybox:latest");
        String userpath = "/home/" + user + "/rec";
        String currentUsersHomeDir = Paths.get(userpath + "/knowledge1").toAbsolutePath().normalize().toString();

        HostConfig hostConfig = HostConfig.builder()
                .autoRemove(Boolean.TRUE)
                .binds(new HostConfig.Bind[] { HostConfig.Bind.from(currentUsersHomeDir).to("/backup").build() })
                .build();

        final ContainerConfig containerConfig = ContainerConfig.builder().image("busybox").hostConfig(hostConfig).cmd(new String[] { "tar", "xvf", "/backup/backup1.tar" }).build();


        final ContainerCreation container = docker.createContainer(containerConfig, "dbBackup");
        docker.startContainer(container.id());

        try{
            docker.waitContainer(container.id());
        } catch (ContainerNotFoundException e){
            System.out.println("Container exit!");
        }

    }

    private static void checkHash(Map<String, String> hash, Map<String, String> hashcollections){
        for (var set : hash.entrySet()) {
            System.out.println("Collection: " + set.getKey() + ", " + set.getValue() + "-" + hashcollections.get(set.getKey()));
        }
    }

    private static void createResidualCollectionsDump(Map<String, String> dbcollections, Map<String, String> proactive, Map<String, String> residual){
        for (var set : dbcollections.entrySet()){
            if (!proactive.containsKey(set.getKey())){
                residual.put(set.getKey(), set.getValue());
            }
        }
    }

}
