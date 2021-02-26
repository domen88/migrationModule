package it.unibo.scotece.domenico;

import com.google.common.collect.ImmutableSet;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.ContainerNotFoundException;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.HostConfig;
import it.unibo.scotece.domenico.services.ClientSendFileProto;
import it.unibo.scotece.domenico.services.impl.DockerConnectImpl;
import it.unibo.scotece.domenico.services.impl.MongoDBConnector;
import it.unibo.scotece.domenico.utils.LineChartAWT;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.bson.Document;
import org.jfree.ui.RefineryUtilities;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import static java.lang.System.exit;

public class Program {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException, DockerCertificateException, DockerException {
        var access = new HashMap<String, Integer>();
        var faccess = new HashMap<String, Double>();
        var invfreq = new HashMap<String, Double>();
        var probability = new HashMap<String, Double>();
        var chart = new HashMap<Double, Double>();
        var dbcollections = new HashMap<String, Double>();
        var tosend = new HashMap<String, Double>();
        var residual = new HashMap<String, Double>();
        var hash = new HashMap<String, String>();
        //User
        if (args.length <= 0){
            System.out.println("Usage jar [user]");
            System.exit(0);
        }
        final String user = args[0];


        /**-----------REACTIVE----------**/
        //Current docker connector
        DockerConnectImpl currentDockerConnector =  new DockerConnectImpl();
        DockerClient docker = currentDockerConnector.setConnection();
        //System.out.println("docker info "  + docker.info());
        Instant start = Instant.now();
        exportContainer(docker, "dbdata", user);
        Instant stop = Instant.now();
        System.out.println("DURATION: exportContainer Procedure "  + Duration.between(start, stop));
        start = Instant.now();
        sendDump("backup.tar", user);
        stop = Instant.now();
        System.out.println("DURATION: sendDump Procedure "  + Duration.between(start, stop));
        currentDockerConnector.close();

        System.exit(0);

        /**---------PROACTIVE----------**/
        /*
        Instant start = Instant.now();
        Instant stop = Instant.now();


        MongoDBConnector mongo = new MongoDBConnector();
        MongoClient mongoClient = mongo.getConnection();

        MongoDatabase database = mongoClient.getDatabase("testdb"); */

        /*-----Calcolo delle probabilità---*/
        //start = Instant.now();
        //calculateProbability(database, access, faccess, invfreq, probability, chart, dbcollections);
        //stop = Instant.now();
        //System.out.println("DURATION: calculateProbability Procedure "  + Duration.between(start, stop));

        /*----Crea i dump----*/
        //start = Instant.now();
        //createDumps(probability, tosend);
        //stop = Instant.now();
        //System.out.println("DURATION: createDumps Procedure "  + Duration.between(start, stop));

        /*----Calcolo hash---*/
        //start = Instant.now();
        //calculateHash(database, tosend, hash);
        //stop = Instant.now();
        //System.out.println("DURATION: calculateHash (PROACTIVE) Procedure "  + Duration.between(start, stop));
        /*----Invia hash---*/
        //start = Instant.now();
        //sendHash(hash);
        //stop = Instant.now();
        //System.out.println("DURATION: sendHash (PROACTIVE)  Procedure "  + Duration.between(start, stop));

        /*----Creo archivio---*/
        //start = Instant.now();
        //var filename = "archivep.tar";
        //createTar(tosend, filename);
        //stop = Instant.now();
        //System.out.println("DURATION: createTar (PROACTIVE) Procedure "  + Duration.between(start, stop));
        /*----Invio i dumps---*/
        //start = Instant.now();
        //sendDump(filename);
        //stop = Instant.now();
        //System.out.println("DURATION: sendDumps (PROACTIVE) Procedure "  + Duration.between(start, stop));


        /*---DataBase Dump Unico---*/
        /*start = Instant.now();
        createDump();
        stop = Instant.now();
        System.out.println("DURATION: createDump REACTIVE APPLICATION AGNOSTIC "  + Duration.between(start, stop));
        var dumpmap = new HashMap<String, Double>();
        dumpmap.put("testdb1", Double.valueOf(1));
        start = Instant.now();
        createTar(dumpmap, "archive.tar");
        stop = Instant.now();
        System.out.println("DURATION: createTar REACTIVE APPLICATION AGNOSTIC "  + Duration.between(start, stop));
        start = Instant.now();
        sendDump("archive.tar");
        stop = Instant.now();
        System.out.println("DURATION: sendDump REACTIVE APPLICATION AGNOSTIC "  + Duration.between(start, stop));*/

        /*----Invio dump----*/
        //sendDump("testdb.archive");

        /*-----Stampa grafico---*/
        //createChart(chart);

        /*---HANDOFF---*/
        //Thread.sleep(60000);
        //System.out.println("HANDOFF SIGNAL!!!");

        //start = Instant.now();
        //createResidualCollectionsDump(dbcollections, tosend, residual);
        //stop = Instant.now();
        //System.out.println("DURATION: createResidualCollectionsDump (REACTIVE) Procedure "  + Duration.between(start, stop));

        //start = Instant.now();
        //createDumps(residual);
        //stop = Instant.now();
        //System.out.println("DURATION: createResidualDumps (REACTIVE) Procedure "  + Duration.between(start, stop));

        //start = Instant.now();
        //calculateHash(database, dbcollections, hash);
        //stop = Instant.now();
        //System.out.println("DURATION: calculateAllHash (REACTIVE) Procedure "  + Duration.between(start, stop));

        //start = Instant.now();
        //sendHash(hash);
        //stop = Instant.now();
        //System.out.println("DURATION: sendAllHash (REACTIVE) Procedure "  + Duration.between(start, stop));

        /*----Creo archivio---*/
        //start = Instant.now();
        //filename = "archiver.tar";
        //createTar(residual, filename);
        //stop = Instant.now();
        //System.out.println("DURATION: createTar (REACTIVE) Procedure "  + Duration.between(start, stop));
        /*----Invio i dumps---*/
        //start = Instant.now();
        //sendDump(filename);
        //stop = Instant.now();
        //System.out.println("DURATION: sendDumps (REACTIVE) Procedure "  + Duration.between(start, stop));

        //Thread.sleep(60000);

        //start = Instant.now();
        //sendResidualCollectionsDump(residual);
        //stop = Instant.now();
        //System.out.println("DURATION: sendResidualCollectionsDump Procedure "  + Duration.between(start, stop));



        /*-----CALCOLO MISS------*/
        /*var percentage = 0.25;
        var hit = tosend.size() * percentage;
        var i = 0;
        var hitcoll = new HashMap<String, Double>();

        for (var s : tosend.entrySet()){
            if (i>hit) break;
            hitcoll.put(s.getKey(), s.getValue());
            i++;
        }
        */

        //start = Instant.now();
        //createDumps(hitcoll);
        //stop = Instant.now();
        //System.out.println("DURATION: createDumps (CALCOLO MISS) Procedure "  + Duration.between(start, stop));

        /*----Creo archivio---*/
        //start = Instant.now();
        //filename = "archivem.tar";
        //createTar(hitcoll, filename);
        //stop = Instant.now();
        //System.out.println("DURATION: createTar (CALCOLO MISS) Procedure "  + Duration.between(start, stop));
        /*----Invio i dumps---*/
        //start = Instant.now();
        //sendDump(filename);
        //stop = Instant.now();
        //System.out.println("DURATION: sendDumps (CALCOLO MISS) Procedure "  + Duration.between(start, stop));


        //mongo.closeConnection();


    }

    private static void exportContainer(DockerClient docker) throws DockerException, InterruptedException, IOException {
        var id = "05e0330e3323";

        OutputStream fo = java.nio.file.Files.newOutputStream(Paths.get("exp.tar"));
        ArchiveOutputStream archive = new TarArchiveOutputStream(fo);



        ImmutableSet.Builder<String> files = ImmutableSet.builder();
        try (TarArchiveInputStream tarStream = new TarArchiveInputStream(docker.exportContainer(id))) {
            TarArchiveEntry entry;
            while ((entry = tarStream.getNextTarEntry()) != null) {
                files.add(entry.getName());
            }
        }

       /* try (TarArchiveInputStream tarStream = new TarArchiveInputStream(docker.exportContainer(id))) {
            TarArchiveEntry entry;
            while ((entry = tarStream.getNextTarEntry()) != null) {
                //ArchiveEntry entryArchive = archive.createArchiveEntry(entry.getFile(), entry.getName());
                //archive.putArchiveEntry(entryArchive);
                //archive.closeArchiveEntry();
            }
        }
        archive.finish();*/
        fo.close();

    }

    private static void exportContainer(DockerClient docker, String volumesFrom, String user) throws DockerException, InterruptedException, IOException {
        //Pull latest ubuntu images from docker hub
        docker.pull("busybox:latest");
        String userpath = "/home/" + user + "/rec";
        String currentUsersHomeDir = Paths.get(userpath).toAbsolutePath().normalize().toString();
        HostConfig hostConfig = HostConfig.builder()
                .autoRemove(Boolean.TRUE)
                .binds(new HostConfig.Bind[] { HostConfig.Bind.from(currentUsersHomeDir).to("/backup").build() })
                .build();

        //Configuration of Container Data Volume
        final ContainerConfig containerConfig = ContainerConfig.builder().image("busybox").hostConfig(hostConfig).cmd(new String[] { "tar", "cvf", "/backup/backup.tar", "/backup/knowledge"}).build();

        //Create Container Data Volume
        ContainerCreation container = docker.createContainer(containerConfig, "dbBackup");
        docker.startContainer(container.id());

        try{
            docker.waitContainer(container.id());
        } catch (ContainerNotFoundException e){
            System.out.println("Container exit!");
        }

        /*---REMOVE FILES---*/
        Process process = Runtime.getRuntime().exec(new String[]{"sh", "-c", "rm -f " + currentUsersHomeDir +"/knowledge/*.png"});
        process.waitFor();
        System.out.println("Files removed");


    }

    private static void calculateProbability(MongoDatabase database, HashMap<String, Integer> access, HashMap<String, Double> faccess,
                        HashMap<String, Double> invfreq, HashMap<String, Double> probability, HashMap<Double, Double> chart, HashMap<String, Double> dbcollections){

        var counter = 0;
        var ok = 0;
        var i = 0;

        for (String name : database.listCollectionNames()) {
            dbcollections.put(name, (double) i++);
            Document stats = database.runCommand(new Document("collStats", name));

            /*-------VERSIONE BASATA SUI COUNT-----*/
            for (var set : stats.entrySet()) {
                if (set.getKey().equals("count")){
                    var value = (Integer) set.getValue();
                    access.put(name, value);
                    counter+=value;
                }
            }

            /*-------VERSIONE BASATA SUGLI ACCESSI-----*/
            /*for (var set : stats.entrySet()) {
                if (set.getKey().equals("wiredTiger")){
                    var value = (Map<String,Object>) set.getValue();
                    for (var set1 : value.entrySet()) {
                        if (set1.getKey().equals("cursor")){
                            var value1 = (Map<String,Object>) set1.getValue();
                            for (var set2 : value1.entrySet()) {
                                if (set2.getKey().equals("insert calls")){
                                    access.put(name, (Integer) set2.getValue());
                                    counter+=(Integer) set2.getValue();
                                }
                            }
                        }
                    }
                }
            }*/
        }

        for (var acc : access.entrySet()){
            /*---Calcolo frequenza di accesso---*/
            Double f = (double)acc.getValue()/counter;
            faccess.put(acc.getKey(), f*100);
            System.out.println("Calcolo frequenza di accesso");
            //System.out.println(acc.getKey() + " : " + f*100);

            /*---Calcolo inverso della frequenza---*/
            Double inv = 1/f;
            invfreq.put(acc.getKey(), inv);
            System.out.println("Calcolo inverso della frequenza");
            //System.out.println(acc.getKey() + " : " + inv);

        }

        NormalDistribution n = new NormalDistribution(getMean(invfreq), getSdv(invfreq));
        for (var freq : invfreq.entrySet()){
            /*---Calcolo della probabilità di migrazione---*/
            double p = n.cumulativeProbability(freq.getValue());
            probability.put(freq.getKey(), p);
            chart.put(faccess.get(freq.getKey()), p);
            if (p > 0.45) ok++;
            System.out.println(freq.getKey() + " : " + p);
        }

        System.out.println("Totale Operazioni: " + counter);
        System.out.println("Totale OK: " + ok);

    }

    private static void createDumps(HashMap<String, Double> probability, HashMap<String, Double> tosend) throws IOException, InterruptedException {
        double threshold = 0.45;
        System.out.println("Create dump PROACTIVE");
        for (var p : probability.entrySet()){
            if (p.getValue()>=threshold){
                tosend.put(p.getKey(), p.getValue());
                System.out.println("Create dump for collection " + p.getKey());
                var process = Runtime.getRuntime().exec(new String[]{"sh", "-c", "mongodump --db testdb --collection " +
                        p.getKey() + " --archive="+p.getKey()+".archive"});
                process.waitFor();
                System.out.println("Dump finished");
            }
        }
    }

    private static void createDumps(HashMap<String, Double> tosend) throws IOException, InterruptedException {
        System.out.println("Create dump REACTIVE");
        for (var t : tosend.entrySet()){
            //System.out.println("Create dump for collection " + t.getKey());
            var process = Runtime.getRuntime().exec(new String[]{"sh", "-c", "mongodump --db testdb --collection " +
                    t.getKey() + " --archive="+t.getKey()+".archive"});
            process.waitFor();
            //System.out.println("Dump finished");
        }
    }

    private static void sendDumps(HashMap<String, Double> tosend, HashMap<String, String> hash) throws IOException, InterruptedException {
        var clientProto = new ClientSendFileProto("192.168.2.14", 9000, tosend, hash);
        try {
            clientProto.sendFiles();
        } finally {
            clientProto.shutdown();
        }
    }

    private static void createDump() throws IOException, InterruptedException {
        System.out.println("CREATE DUMP!!!");
        var p = Runtime.getRuntime().exec(new String[]{"sh", "-c", "mongodump --db testdb --archive=testdb1.archive"});
        p.waitFor();
        System.out.println("DUMP FINISHED!!!");
    }

    private static void sendDump(String filename, String user) throws IOException, InterruptedException {
        var clientProto = new ClientSendFileProto("10.0.0.4", 9000);
        try {
            clientProto.sendChunkFile(filename, user);
        } finally {
            clientProto.shutdown();
        }
    }

    private static void createResidualCollectionsDump(HashMap<String, Double> dbcollections, HashMap<String, Double> tosend, HashMap<String, Double> residual){
        Double i = Double.valueOf(0);
        for (var set : dbcollections.entrySet()){
                if (!tosend.containsKey(set.getKey())){
                    residual.put(set.getKey(), i++);
                }
            }
    }

    private static void sendResidualCollectionsDump(HashMap<String, Double> residual) throws IOException, InterruptedException {
        var clientProto = new ClientSendFileProto("192.168.2.14", 9000, residual, null);
        try {
            clientProto.sendResidualFiles();
        } finally {
            clientProto.shutdown();
        }

    }

    private static void calculateHash(MongoDatabase database, HashMap<String, Double> tosend, HashMap<String, String> hash){
        Document command = new Document();
        command.put("dbHash", 1);
        var collections = new ArrayList<String>();
        for (var p : tosend.entrySet()){
            collections.add(p.getKey());
        }
        command.put("collections", collections);
        Document collStatsResults = database.runCommand(command);

        for (var set : collStatsResults.entrySet()) {
            if (set.getKey().equals("collections")){
                var value = (Map<String,Object>) set.getValue();
                for (var set1 : value.entrySet()) {
                    hash.put(set1.getKey(), (String) set1.getValue());
                }
            }
        }
    }

    private static void sendHash(HashMap<String, String> hash) throws InterruptedException {
        var clientProto = new ClientSendFileProto("192.168.2.14", 9000, hash);
        try {
            clientProto.sendHash();
        } finally {
            clientProto.shutdown();
        }
    }

    private static double getMean(HashMap<String, Double> records){
        double acc = 0;
        for (var r : records.entrySet()){
            acc+=r.getValue();
        }
        return acc/records.size();
    }

    private static double getSdv(HashMap<String, Double> records){
        double mean = getMean(records);
        double n = records.size();
        double dv = 0;
        for (var r : records.entrySet()) {
            double dm = r.getValue() - mean;
            dv += dm * dm;
        }
        return Math.sqrt(dv / n);
    }

    private static void createChart(HashMap<Double, Double> chart){
        var graf = new LineChartAWT("Probability", chart);
        graf.pack();
        RefineryUtilities.centerFrameOnScreen(graf);
        graf.setVisible(true);
    }

    private static void createTar(HashMap<String, Double> tosend, String archivename) throws IOException {
        OutputStream fo = java.nio.file.Files.newOutputStream(Paths.get(archivename));
        ArchiveOutputStream archive = new TarArchiveOutputStream(fo);

        for (var set : tosend.entrySet()){
            var filename = set.getKey() + ".archive";
            Path path = Paths.get(filename);
            var file = path.toFile();
            ArchiveEntry entry = archive.createArchiveEntry(file, filename);
            archive.putArchiveEntry(entry);
            if (file.isFile()){
                try (InputStream i = java.nio.file.Files.newInputStream(file.toPath())){
                    IOUtils.copy(i, archive);
                }
            }
            archive.closeArchiveEntry();
        }
        archive.finish();
        fo.close();

    }
}
