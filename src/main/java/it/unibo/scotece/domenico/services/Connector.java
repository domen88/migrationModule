package it.unibo.scotece.domenico.services;

public interface Connector<T,U> {

    T connection(U... args);



}
