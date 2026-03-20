# Proyecto 3 - DbComponent

Este subproyecto contiene solo lo solicitado para la asignacion:

- DbComponent desacoplado por adapters
- Pool de conexiones propio
- Queries predefinidas (sin SQL crudo publico)
- Soporte para transacciones

## Ejecutar

```powershell
mvn -DskipTests clean compile
mvn exec:java
```

## Cambiar motor de BD

- PostgreSQL: usar `new PostgresAdapter()`
- H2: usar `new H2Adapter()`

Sin tocar la logica de `DbComponent`.
