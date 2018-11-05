package it.unibo.scotece.domenico.services.impl;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import it.unibo.scotece.domenico.services.Connector;

import java.util.ArrayList;
import java.util.List;

public class MongoDBConnector implements Connector {

    private MongoClient mongo;
    private List<MongoClient> cachedMongo = new ArrayList<>(1);

    public MongoDBConnector(){
        this.mongo = connection(null);
        this.cachedMongo.add(this.mongo);
    }

    @Override
    public MongoClient connection(Object[] args) {
        //different host:port
        //var host = (String) args[0];
        if (this.cachedMongo.isEmpty()) {
            MongoClient mongoClient = MongoClients.create();
            return mongoClient;
        } else {
            return this.cachedMongo.get(0);
        }

    }

    public MongoClient getConnection(){
        if (this.cachedMongo.isEmpty()) {
            return connection(null);
        } else {
            return this.cachedMongo.get(0);
        }
    }

    public MongoClient setConnection(Object[] args){
        if (!this.cachedMongo.isEmpty()) {
            this.cachedMongo.remove(this.mongo);
        }
        this.mongo = connection(args);
        this.cachedMongo.add(this.mongo);
        return this.mongo;
    }
}
