package com.dbcomponent.ui;

import com.dbcomponent.adapter.H2Adapter;
import com.dbcomponent.adapter.IAdapter;
import com.dbcomponent.adapter.PostgresAdapter;
import com.dbcomponent.adapter.SqliteAdapter;
import com.dbcomponent.core.DbComponent;
import com.dbcomponent.model.DbQueryResult;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DbComponentFxApp extends Application {

    private static final long FIXED_SCALE_UP_MS = 300;
    private static final long FIXED_SCALE_DOWN_MS = 100;
    private static final long FIXED_ACQUIRE_TIMEOUT_MS = 3000;

    private final ExecutorService worker = Executors.newSingleThreadExecutor();

    private DbComponent db;

    private final ComboBox<String> adapterBox = new ComboBox<>();
    private final TextField hostField = new TextField("localhost");
    private final TextField portField = new TextField("5432");
    private final TextField databaseField = new TextField("pool_simulator_db");
    private final TextField userField = new TextField("postgres");
    private final TextField passField = new TextField("1610");

    private final TextField minPoolField = new TextField("2");
    private final TextField maxPoolField = new TextField("6");

    private final ComboBox<String> queryBox = new ComboBox<>();
    private final TextField queryParamsField = new TextField("10");

    private final Label adapterStatus = new Label("Adapter: -");
    private final Label poolStatus = new Label("Pool: total=0 | disponibles=0 | enUso=0");
    private final Label decoupleStatus = new Label("Desacople: DbComponent depende de la abstraccion IAdapter");

    private final TextArea output = new TextArea();

    @Override
    public void start(Stage stage) {
        stage.setTitle("Proyecto 3 - DbComponent (JavaFX)");

        adapterBox.getItems().addAll("PostgreSQL", "H2", "SQLite");
        adapterBox.valueProperty().addListener((obs, oldValue, newValue) -> applyAdapterDefaults(newValue));
        adapterBox.getSelectionModel().selectFirst();

        queryBox.setPromptText("Selecciona un queryId");
        queryParamsField.setPromptText("Parametros separados por coma. Ej: 50,10 o pendiente,10");

        Button connectBtn = new Button("Inicializar DbComponent");
        connectBtn.setOnAction(e -> initDbComponent());

        Button queryBtn = new Button("Ejecutar Query");
        queryBtn.setOnAction(e -> runQuery());

        Button txBtn = new Button("Demo Transaccion");
        txBtn.setOnAction(e -> runTransactionDemo());

        Button metricsBtn = new Button("Refrescar Pool");
        metricsBtn.setOnAction(e -> refreshPoolLabels());

        Button shutdownBtn = new Button("Cerrar Pool");
        shutdownBtn.setOnAction(e -> shutdownDb());

        GridPane config = new GridPane();
        config.setHgap(8);
        config.setVgap(8);
        config.addRow(0, new Label("Adapter"), adapterBox, new Label("Host"), hostField, new Label("Port"), portField);
        config.addRow(1, new Label("Database"), databaseField, new Label("User"), userField, new Label("Password"), passField);
        config.addRow(2, new Label("PoolMin"), minPoolField, new Label("PoolMax"), maxPoolField);

        HBox actions = new HBox(8, connectBtn, queryBtn, txBtn, metricsBtn, shutdownBtn);

        output.setEditable(false);
        output.setWrapText(true);

        VBox root = new VBox(10,
                new Label("Visualizador de DbComponent - Proyecto 3"),
                config,
                new HBox(8, new Label("QueryId"), queryBox),
            new HBox(8, new Label("Params"), queryParamsField),
                actions,
                adapterStatus,
                decoupleStatus,
                poolStatus,
                output);

        root.setPadding(new Insets(12));
        VBox.setVgrow(output, Priority.ALWAYS);

        stage.setScene(new Scene(root, 980, 620));
        stage.show();

        append("Listo. Inicializa DbComponent para ver el desacople y el estado del pool.");
        applyAdapterDefaults(adapterBox.getValue());
    }

    private void initDbComponent() {
        worker.submit(() -> {
            shutdownDbInternal();
            try {
                IAdapter adapter = buildAdapter(adapterBox.getValue());
                db = new DbComponent( 
                        adapter,
                        hostField.getText().trim(),
                        Integer.parseInt(portField.getText().trim()),
                        databaseField.getText().trim(),
                        userField.getText().trim(),
                        passField.getText().trim(),
                        Integer.parseInt(minPoolField.getText().trim()),
                        Integer.parseInt(maxPoolField.getText().trim()),
                        FIXED_SCALE_UP_MS,
                        FIXED_SCALE_DOWN_MS,
                        FIXED_ACQUIRE_TIMEOUT_MS,
                        "queries.properties");

                List<String> queryIds = db.getQueryIds();
                Platform.runLater(() -> {
                    queryBox.getItems().setAll(queryIds);
                    if (!queryIds.isEmpty()) {
                        queryBox.getSelectionModel().selectFirst();
                    }
                    adapterStatus.setText("Adapter: " + db.getAdapterName());
                    decoupleStatus.setText("Desacople OK: misma clase DbComponent con adapter=" + db.getAdapterName());
                    refreshPoolLabels();
                });
                append("DbComponent inicializado con adapter " + db.getAdapterName() + ".");
            } catch (Exception ex) {
                append("Error inicializando DbComponent: " + ex.getMessage());
                append(buildAdapterHelpMessage(adapterBox.getValue()));
            }
        });
    }

    private void runQuery() {
        if (db == null) {
            append("Primero inicializa DbComponent.");
            return;
        }
        String queryId = queryBox.getValue();
        if (queryId == null || queryId.isBlank()) {
            append("Selecciona un queryId.");
            return;
        }

        worker.submit(() -> {
            try {
                Object[] params = parseParams(queryParamsField.getText());
                DbQueryResult result = db.query(queryId, params);
                append("Query " + queryId + " ejecutada. Params=" + params.length + ", Rows=" + result.getRows().size());
                Platform.runLater(this::refreshPoolLabels);
            } catch (Exception ex) {
                append("Error ejecutando query: " + ex.getMessage());
            }
        });
    }

    private void runTransactionDemo() {
        if (db == null) {
            append("Primero inicializa DbComponent.");
            return;
        }

        worker.submit(() -> {
            try (DbComponent.DbTransaction tx = db.transaction()) {
                String first = pickQuery(0);
                String second = pickQuery(1);
            DbQueryResult r1 = tx.query(first, defaultParamsFor(first));
            DbQueryResult r2 = tx.query(second, defaultParamsFor(second));
                tx.commit();
                append("Transaccion OK: " + first + "(" + r1.getRows().size() + ") + "
                        + second + "(" + r2.getRows().size() + ")");
                Platform.runLater(this::refreshPoolLabels);
            } catch (Exception ex) {
                append("Error en transaccion: " + ex.getMessage());
            }
        });
    }

    private String pickQuery(int idx) {
        List<String> ids = db.getQueryIds();
        if (ids.isEmpty()) {
            throw new IllegalStateException("No hay queryIds cargados");
        }
        return ids.get(Math.min(idx, ids.size() - 1));
    }

    private Object[] parseParams(String raw) {
        if (raw == null || raw.isBlank()) {
            return new Object[0];
        }

        String[] tokens = raw.split(",");
        Object[] values = new Object[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            values[i] = parseToken(tokens[i]);
        }
        return values;
    }

    private Object parseToken(String token) {
        String t = token == null ? "" : token.trim();
        if (t.isEmpty()) {
            return "";
        }
        if ("null".equalsIgnoreCase(t)) {
            return null;
        }
        if ("true".equalsIgnoreCase(t) || "false".equalsIgnoreCase(t)) {
            return Boolean.parseBoolean(t);
        }
        if (t.matches("-?\\d+")) {
            try {
                return Integer.parseInt(t);
            } catch (NumberFormatException ignored) {
                return Long.parseLong(t);
            }
        }
        if (t.matches("-?\\d+\\.\\d+")) {
            return Double.parseDouble(t);
        }
        return t;
    }

    private Object[] defaultParamsFor(String queryId) {
        if (queryId == null || queryId.isBlank()) {
            return new Object[0];
        }

        if ("producto.listar".equals(queryId)) {
            return new Object[] { 10 };
        }
        if ("producto.stock_bajo".equals(queryId)) {
            return new Object[] { 50, 10 };
        }
        if ("pedido.pendientes".equals(queryId)) {
            return new Object[] { "pendiente", 10 };
        }
        if (queryId.startsWith("auto.") && queryId.endsWith(".listar")) {
            return new Object[] { 10 };
        }
        return new Object[0];
    }

    private void refreshPoolLabels() {
        if (db == null) {
            poolStatus.setText("Pool: total=0 | disponibles=0 | enUso=0");
            return;
        }
        poolStatus.setText("Pool: total=" + db.getPoolTotalConexiones()
                + " | disponibles=" + db.getPoolConexionesDisponibles()
                + " | enUso=" + db.getPoolConexionesEnUso());
    }

    private void shutdownDb() {
        worker.submit(() -> {
            shutdownDbInternal();
            append("Pool cerrado.");
            Platform.runLater(() -> {
                queryBox.getItems().clear();
                refreshPoolLabels();
                adapterStatus.setText("Adapter: -");
            });
        });
    }

    private void shutdownDbInternal() {
        if (db != null) {
            db.shutdown();
            db = null;
        }
    }

    private IAdapter buildAdapter(String selected) {
        if ("H2".equalsIgnoreCase(selected)) {
            return new H2Adapter();
        }
        if ("SQLite".equalsIgnoreCase(selected)) {
            return new SqliteAdapter();
        }
        return new PostgresAdapter();
    }

    private void applyAdapterDefaults(String selected) {
        if ("SQLite".equalsIgnoreCase(selected)) {
            hostField.setText("local-file");
            portField.setText("0");
            databaseField.setText("sqlite-demo.db");
            userField.setText("");
            passField.setText("");
            append("Adapter SQLite seleccionado. Valores sugeridos: db=sqlite-demo.db (host/port/user/password no aplican). ");
            return;
        }

        if ("H2".equalsIgnoreCase(selected)) {
            hostField.setText("localhost");
            portField.setText("9092");
            databaseField.setText("testdb");
            userField.setText("sa");
            passField.setText("");
            append("Adapter H2 seleccionado. Valores sugeridos: host=localhost, port=9092, db=testdb, user=sa, password=(vacio).");
            return;
        }

        hostField.setText("localhost");
        portField.setText("5432");
        databaseField.setText("pool_simulator_db");
        userField.setText("postgres");
        passField.setText("1610");
        append("Adapter PostgreSQL seleccionado. Valores sugeridos: host=localhost, port=5432, db=pool_simulator_db, user=postgres.");
    }

    private String buildAdapterHelpMessage(String selected) {
        if ("SQLite".equalsIgnoreCase(selected)) {
            return "SQLite usa archivo local (ejemplo: sqlite-demo.db). Si no existe, se crea automaticamente al conectar.";
        }
        if ("H2".equalsIgnoreCase(selected)) {
            return "H2 en este proyecto usa modo TCP. Verifica que el servidor H2 este activo (puerto 9092), y que la base/credenciales sean correctas.";
        }
        return "PostgreSQL seleccionado. Verifica host, puerto, base y usuario/password.";
    }

    private void append(String text) {
        Platform.runLater(() -> output.appendText(text + System.lineSeparator()));
    }

    @Override
    public void stop() {
        shutdownDbInternal();
        worker.shutdownNow();
    }
}
