package com.dbcomponent;

import com.dbcomponent.adapter.IAdapter;
import com.dbcomponent.adapter.PostgresAdapter;
import com.dbcomponent.core.DbComponent;
import com.dbcomponent.model.DbQueryResult;

/**
 * Modo consola de demostracion.
 * Para visualizar desacople y pool en vivo usar: mvn javafx:run
 */
public class Main {

    public static void main(String[] args) {
        IAdapter adapter = new PostgresAdapter();

        DbComponent db = new DbComponent(
                adapter,
                "localhost",
                5432,
                "pool_simulator_db",
                "postgres",
                "1610",
                2,
                6,
                300,
                100,
                3000,
                "queries.properties");

        try {
            DbQueryResult r1 = db.query("producto.listar");
            System.out.println("producto.listar -> rows=" + r1.getRows().size());

            DbQueryResult r2 = db.query("producto.stock_bajo");
            System.out.println("producto.stock_bajo -> rows=" + r2.getRows().size());

            try (DbComponent.DbTransaction tx = db.transaction()) {
                DbQueryResult r3 = tx.query("pedido.pendientes");
                System.out.println("pedido.pendientes (tx) -> rows=" + r3.getRows().size());
                tx.commit();
            }

            System.out.println("DbComponent demo completed.");
        } catch (Exception e) {
            System.err.println("Error running DbComponent demo: " + e.getMessage());
            e.printStackTrace();
        } finally {
            db.shutdown();
        }
    }
}
