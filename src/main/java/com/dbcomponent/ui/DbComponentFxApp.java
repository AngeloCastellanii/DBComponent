package com.dbcomponent.ui;

import com.dbcomponent.adapter.H2Adapter;
import com.dbcomponent.adapter.IAdapter;
import com.dbcomponent.adapter.PostgresAdapter;
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
    private final TextField upField = new TextField("300");
    private final TextField downField = new TextField("100");
    private final TextField acquireField = new TextField("3000");

    private final ComboBox<String> queryBox = new ComboBox<>();

    private final Label adapterStatus = new Label("Adapter: -");
    private final Label poolStatus = new Label("Pool: total=0 | disponibles=0 | enUso=0");
    private final Label decoupleStatus = new Label("Desacople: DbComponent depende de IAdapter");

    private final TextArea output = new TextArea();

    @Override
    public void start(Stage stage) {
        stage.setTitle("Proyecto 3 - DbComponent (JavaFX)");

        adapterBox.getItems().addAll("PostgreSQL", "H2");
        adapterBox.valueProperty().addListener((obs, oldValue, newValue) -> applyAdapterDefaults(newValue));
        adapterBox.getSelectionModel().selectFirst();

        queryBox.setPromptText("Selecciona un queryId");

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
        config.addRow(2, new Label("PoolMin"), minPoolField, new Label("PoolMax"), maxPoolField, new Label("Acquire(ms)"), acquireField);
        config.addRow(3, new Label("ScaleUp(ms)"), upField, new Label("ScaleDown(ms)"), downField);

        HBox actions = new HBox(8, connectBtn, queryBtn, txBtn, metricsBtn, shutdownBtn);

        output.setEditable(false);
        output.setWrapText(true);

        VBox root = new VBox(10,
                new Label("Visualizador de DbComponent - Proyecto 3"),
                config,
                new HBox(8, new Label("QueryId"), queryBox),
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
                        Long.parseLong(upField.getText().trim()),
                        Long.parseLong(downField.getText().trim()),
                        Long.parseLong(acquireField.getText().trim()),
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
                DbQueryResult result = db.query(queryId);
                append("Query " + queryId + " ejecutada. Rows=" + result.getRows().size());
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
                DbQueryResult r1 = tx.query(first);
                DbQueryResult r2 = tx.query(second);
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
        return new PostgresAdapter();
    }

    private void applyAdapterDefaults(String selected) {
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
