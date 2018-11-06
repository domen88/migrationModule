package it.unibo.scotece.domenico;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import it.unibo.scotece.domenico.services.impl.MongoDBConnector;
import it.unibo.scotece.domenico.utils.LineChartAWT;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.bson.Document;
import org.jfree.ui.RefineryUtilities;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Program {

    public static void main(String[] args) {
        var access = new HashMap<String, Integer>();
        var faccess = new HashMap<String, Double>();
        var invfreq = new HashMap<String, Double>();
        var probability = new HashMap<String, Double>();
        var chart = new HashMap<Double, Double>();
        var counter = 0;

        MongoDBConnector mongo = new MongoDBConnector();
        MongoClient mongoClient = mongo.getConnection();

        MongoDatabase database = mongoClient.getDatabase("testdb");

        for (String name : database.listCollectionNames()) {
            MongoCollection<Document> coll = database.getCollection(name);
            Document stats = database.runCommand(new Document("collStats", name));

            for (var set : stats.entrySet()) {
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
            }
        }

        for (var acc : access.entrySet()){
            /*---Calcolo frequenza di accesso---*/
            Double f = (double)acc.getValue()/counter;
            faccess.put(acc.getKey(), f*100);

            /*---Calcolo inverso della frequenza---*/
            Double inv = 1/f;
            invfreq.put(acc.getKey(), inv);

        }

        NormalDistribution n = new NormalDistribution(getMean(invfreq), getSdv(invfreq));
        for (var freq : invfreq.entrySet()){
            /*---Calcolo della probabilità di migrazione---*/
            double p = n.cumulativeProbability(freq.getValue());
            probability.put(freq.getKey(), p);
            chart.put(1/freq.getValue(), p);
            System.out.println(freq.getKey() + " : " + p);
        }

        var graf = new LineChartAWT("Probability", chart);
        graf.pack();
        RefineryUtilities.centerFrameOnScreen(graf);
        graf.setVisible(true);

        System.out.println("Totale Operazioni: " + counter);
        mongo.closeConnection();
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
}
