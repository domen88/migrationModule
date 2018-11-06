package it.unibo.scotece.domenico.services;

public interface Connector<T,U> {

    T createConnection(U... args);
    void closeConnection();


}
