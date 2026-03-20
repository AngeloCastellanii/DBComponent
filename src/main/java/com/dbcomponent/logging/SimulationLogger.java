package com.dbcomponent.logging;

/**
 * Logger minimo para mantener la firma del pool del Proyecto 2.
 */
public class SimulationLogger {

    public void logInfo(String message) {
        System.out.println("[INFO] " + message);
    }

    public void logError(String message) {
        System.err.println("[ERROR] " + message);
    }
}
