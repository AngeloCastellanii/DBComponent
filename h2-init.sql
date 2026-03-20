CREATE TABLE IF NOT EXISTS productos (
  id INT PRIMARY KEY,
  nombre VARCHAR(120),
  precio DECIMAL(10,2),
  stock INT
);

CREATE TABLE IF NOT EXISTS pedidos (
  id INT PRIMARY KEY,
  cliente_id INT,
  estado VARCHAR(50),
  creado_en TIMESTAMP
);

MERGE INTO productos KEY(id) VALUES
(1, 'Mate', 2500.00, 80),
(2, 'Termo', 12000.00, 20),
(3, 'Bombilla', 3000.00, 35);

MERGE INTO pedidos KEY(id) VALUES
(1, 10, 'pendiente', CURRENT_TIMESTAMP),
(2, 11, 'completado', CURRENT_TIMESTAMP);
