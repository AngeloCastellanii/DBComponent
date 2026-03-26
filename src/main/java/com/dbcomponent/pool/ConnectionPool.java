package com.dbcomponent.pool;

import com.dbcomponent.logging.SimulationLogger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * PARTE 5 - Pool de conexiones propio
 *
 * Mantiene un conjunto de conexiones reutilizables en una BlockingQueue.
 * Implementa escalado dinamico:
 * - Escala hacia arriba: si el tiempo de espera supera scaleUpThresholdMs, agrega
 * conexiones (hasta maxSize).
 * - Escala hacia abajo: si el pool tiene mas conexiones de las necesarias, las cierra
 * (hasta minSize).
 */
public class ConnectionPool {

    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;

    private final int poolMinSize;
    private final int poolMaxSize;
    private final long poolScaleUpThresholdMs;
    private final long poolScaleDownThresholdMs;
    private final long poolAcquireTimeoutMs;

    private final SimulationLogger logger;
    private final BlockingDeque<Connection> pool;
    private final AtomicInteger totalConexiones; // activas (en uso + disponibles)
    private final AtomicLong ultimoTiempoEsperaMs;
    private volatile boolean cerrado = false;

    public ConnectionPool(
            String dbUrl,
            String dbUser,
            String dbPassword,
            int poolMinSize,
            int poolMaxSize,
            long poolScaleUpThresholdMs,
            long poolScaleDownThresholdMs,
            long poolAcquireTimeoutMs,
            SimulationLogger logger) throws SQLException {
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.poolMinSize = poolMinSize;
        this.poolMaxSize = poolMaxSize;
        this.poolScaleUpThresholdMs = poolScaleUpThresholdMs;
        this.poolScaleDownThresholdMs = poolScaleDownThresholdMs;
        this.poolAcquireTimeoutMs = poolAcquireTimeoutMs;
        this.logger = logger;
        this.pool = new LinkedBlockingDeque<>();
        this.totalConexiones = new AtomicInteger(0);
        this.ultimoTiempoEsperaMs = new AtomicLong(Long.MAX_VALUE);
        inicializar();
    }

    // -- Inicializacion -------------------------------------------------------

    private void inicializar() throws SQLException {
        logInfo("Inicializando pool con " + poolMinSize + " conexiones...");
        for (int i = 0; i < poolMinSize; i++) {
            pool.offer(crearConexion());
            totalConexiones.incrementAndGet();
        }
        logInfo("Pool listo - " + totalConexiones.get() + " conexiones disponibles.");
    }

    // -- Adquirir conexion ----------------------------------------------------

    public Connection acquire() throws SQLException, InterruptedException {
        if (cerrado)
            throw new SQLException("El pool esta cerrado.");

        long inicio = System.currentTimeMillis();
        Connection conn = pool.poll(poolAcquireTimeoutMs, TimeUnit.MILLISECONDS);
        long tiempoEspera = System.currentTimeMillis() - inicio;
        ultimoTiempoEsperaMs.set(tiempoEspera);

        if (conn == null || !isValid(conn)) {
            if (totalConexiones.get() < poolMaxSize) {
                conn = crearConexion();
                totalConexiones.incrementAndGet();
                logInfo("Pool scale-up (sin disponibles): "
                        + totalConexiones.get() + " conexiones totales.");
            } else {
                throw new SQLException(
                        "Pool agotado: no hay conexiones disponibles (max="
                                + poolMaxSize + ")");
            }
        }

        if (tiempoEspera > poolScaleUpThresholdMs
                && totalConexiones.get() < poolMaxSize) {
            try {
                pool.offer(crearConexion());
                totalConexiones.incrementAndGet();
                logInfo("Pool scale-up (umbral espera=" + tiempoEspera + "ms): "
                        + totalConexiones.get() + " conexiones totales.");
            } catch (SQLException ignored) {
            }
        }

        return conn;
    }

    // -- Liberar conexion -----------------------------------------------------

    public void release(Connection conn) {
        if (conn == null || cerrado) {
            cerrarConexion(conn);
            return;
        }

        if (totalConexiones.get() > poolMinSize
                && pool.size() >= poolMinSize
                && ultimoTiempoEsperaMs.get() <= poolScaleDownThresholdMs) {
            cerrarConexion(conn);
            totalConexiones.decrementAndGet();
            logInfo("Pool scale-down (espera=" + ultimoTiempoEsperaMs.get() + "ms): "
                    + totalConexiones.get() + " conexiones totales.");
            return;
        }

        if (isValid(conn)) {
            pool.offerFirst(conn);
        } else {
            cerrarConexion(conn);
            totalConexiones.decrementAndGet();
            try {
                pool.offer(crearConexion());
                totalConexiones.incrementAndGet();
            } catch (SQLException e) {
                logError("No se pudo reemplazar conexion invalida: " + e.getMessage());
            }
        }
    }

    // -- Cierre del pool ------------------------------------------------------

    public void shutdown() {
        cerrado = true;
        int cerradas = 0;
        Connection conn;
        while ((conn = pool.poll()) != null) {
            cerrarConexion(conn);
            cerradas++;
        }
        logInfo("Pool cerrado - " + cerradas + " conexiones cerradas.");
        totalConexiones.set(0);
    }

    // -- Metricas del pool ----------------------------------------------------

    public int getTotalConexiones() {
        return totalConexiones.get();
    }

    public int getConexionesDisponibles() {
        return pool.size();
    }

    public int getConexionesEnUso() {
        return totalConexiones.get() - pool.size();
    }

    // -- Internos -------------------------------------------------------------

    private Connection crearConexion() throws SQLException {
        return DriverManager.getConnection(dbUrl, dbUser, dbPassword);
    }

    private void logInfo(String message) {
        if (logger != null) {
            logger.logInfo(message);
        }
    }

    private void logError(String message) {
        if (logger != null) {
            logger.logError(message);
        }
    }

    private boolean isValid(Connection conn) {
        try {
            return conn != null && !conn.isClosed() && conn.isValid(1);
        } catch (SQLException e) {
            return false;
        }
    }

    private void cerrarConexion(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
            }
        }
    }
}
