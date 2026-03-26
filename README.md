# Proyecto 3 - DbComponent

Este subproyecto contiene solo lo solicitado para la asignacion:

- DbComponent desacoplado por adapters
- Pool de conexiones propio
- Queries predefinidas parametrizadas (sin SQL crudo publico)
- Soporte para transacciones

## Parametros en queries

Los queries predefinidos usan placeholders posicionales `?` y se ejecutan con argumentos:

```java
db.query("producto.listar", 10);
db.query("producto.stock_bajo", 50, 10);

try (DbComponent.DbTransaction tx = db.transaction()) {
	tx.query("pedido.pendientes", "pendiente", 10);
	tx.commit();
}
```

## Ejecutar

```powershell
mvn -DskipTests clean compile
mvn exec:java
```

## Cambiar motor de BD

- PostgreSQL: usar `new PostgresAdapter()`
- H2: usar `new H2Adapter()`
- SQLite: usar `new SqliteAdapter()`

Sin tocar la logica de `DbComponent`.

## Catalogo de Queries (multi-formato)

`QueryCatalog` detecta el parser automaticamente segun la extension del archivo:

- `.properties`
- `.json`
- `.yaml` / `.yml`
- `.xml`
- `.sql` (plano por bloques)

Archivos de ejemplo en `src/main/resources/`:

- `queries.properties`
- `queries.json`
- `queries.yaml`
- `queries.xml`
- `queries.sql`

Para usar uno concreto, cambia el ultimo parametro del constructor `DbComponent`.
Ejemplo actual:

```java
"queries.properties"
```

Puedes reemplazarlo por:

```java
"queries.json"
```

Formato requerido por tipo:

1. `properties/json/yaml`: mapa `queryId -> sql`
2. `xml`: nodos `<query id="...">SQL</query>` o `<entry key="...">SQL</entry>`
3. `sql`: bloques con encabezado `-- id: query.id` (o `# id: query.id`) seguido del SQL
