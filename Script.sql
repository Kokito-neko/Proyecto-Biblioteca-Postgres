/*
=============================================================================
PROYECTO FINAL: SISTEMA DE GESTIÓN DE BIBLIOTECA UNIVERSITARIA DISTRIBUIDA
ASIGNATURA: BASE DE DATOS AVANZADA
FECHA: DICIEMBRE 2025
=============================================================================
DESCRIPCIÓN:
Script integral que implementa un sistema de gestión bibliotecaria de alto 
rendimiento. Incluye:
1. Esquema Relacional Normalizado (DDL).
2. Lógica de Negocio Automatizada (PL/pgSQL: Triggers, Funciones, Procedimientos).
3. Configuración de Base de Datos Distribuida (postgres_fdw).
4. Simulación de Big Data (2 Millones de registros) para pruebas de indexación.
5. Gestión de Transacciones Financieras (Atomicidad).
=============================================================================
*/

-- ==========================================================================
-- 1. CONFIGURACIÓN DEL ENTORNO Y LIMPIEZA
-- ==========================================================================
DROP SCHEMA IF EXISTS biblioteca_universitaria CASCADE;
CREATE SCHEMA biblioteca_universitaria;
SET search_path TO biblioteca_universitaria;

-- ==========================================================================
-- 2. DDL - CREACIÓN DE ESTRUCTURA (TABLAS Y RELACIONES)
-- ==========================================================================

-- 2.1 ENTIDADES DE PERSONAS (Generalización/Especialización)
CREATE TABLE Persona (
    id_persona SERIAL PRIMARY KEY,
    tipo_persona VARCHAR(20) NOT NULL CHECK (tipo_persona IN ('Estudiante', 'Profesor', 'Administrativo')),
    identificacion VARCHAR(20) UNIQUE NOT NULL,
    nombre_completo VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    telefono VARCHAR(15),
    direccion TEXT,
    fecha_nacimiento DATE,
    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    estado VARCHAR(20) DEFAULT 'Activo'
);
CREATE INDEX idx_persona_mail ON Persona(email);

CREATE TABLE Estudiante (
    id_estudiante SERIAL PRIMARY KEY,
    id_persona INTEGER UNIQUE NOT NULL,
    codigo_estudiante VARCHAR(20) UNIQUE NOT NULL,
    carrera VARCHAR(100),
    semestre INTEGER,
    facultad VARCHAR(100),
    CONSTRAINT fk_est_pers FOREIGN KEY (id_persona) REFERENCES Persona(id_persona) ON DELETE CASCADE
);

CREATE TABLE Profesor (
    id_profesor SERIAL PRIMARY KEY,
    id_persona INTEGER UNIQUE NOT NULL,
    codigo_profesor VARCHAR(20) UNIQUE NOT NULL,
    departamento VARCHAR(100),
    titulo_academico VARCHAR(100),
    fecha_ingreso DATE,
    CONSTRAINT fk_prof_pers FOREIGN KEY (id_persona) REFERENCES Persona(id_persona) ON DELETE CASCADE
);

-- 2.2 CATÁLOGOS BIBLIOGRÁFICOS
CREATE TABLE Autor (
    id_autor SERIAL PRIMARY KEY,
    nombre_completo VARCHAR(100) NOT NULL,
    nacionalidad VARCHAR(50),
    fecha_nacimiento DATE,
    fecha_fallecimiento DATE,
    biografia TEXT
);
CREATE INDEX idx_autor_nombre ON Autor(nombre_completo);

CREATE TABLE Categoria (
    id_categoria SERIAL PRIMARY KEY,
    nombre VARCHAR(50) NOT NULL,
    descripcion TEXT,
    categoria_padre INTEGER,
    CONSTRAINT fk_cat_padre FOREIGN KEY (categoria_padre) REFERENCES Categoria(id_categoria)
);

CREATE TABLE PalabraClave (
    id_palabra_clave SERIAL PRIMARY KEY,
    palabra VARCHAR(50) UNIQUE NOT NULL
);

-- 2.3 INVENTARIO
CREATE TABLE Libro (
    id_libro SERIAL PRIMARY KEY,
    isbn VARCHAR(13) UNIQUE,
    titulo VARCHAR(200) NOT NULL,
    subtitulo VARCHAR(200),
    anio_publicacion INTEGER,
    edicion VARCHAR(20),
    idioma VARCHAR(30),
    num_paginas INTEGER,
    sinopsis TEXT,
    ubicacion_fisica VARCHAR(50),
    total_ejemplares INTEGER DEFAULT 0,
    ejemplares_disponibles INTEGER DEFAULT 0,
    estado_fisico VARCHAR(20) DEFAULT 'Bueno',
    fecha_ingreso DATE DEFAULT CURRENT_DATE
);
-- Índices para optimización de consultas
CREATE INDEX idx_libro_titulo ON Libro(titulo);
CREATE INDEX idx_libro_isbn ON Libro(isbn);

CREATE TABLE Ejemplar (
    id_ejemplar SERIAL PRIMARY KEY,
    codigo_barras VARCHAR(50) UNIQUE NOT NULL,
    id_libro INTEGER NOT NULL,
    numero_ejemplar INTEGER,
    estado VARCHAR(20) DEFAULT 'Disponible', -- Estados: Disponible, Prestado, Mantenimiento
    fecha_adquisicion DATE,
    observaciones TEXT,
    CONSTRAINT fk_ejemplar_libro FOREIGN KEY (id_libro) REFERENCES Libro(id_libro) ON DELETE CASCADE
);
CREATE INDEX idx_ejemplar_codigo ON Ejemplar(codigo_barras);

-- 2.4 TABLAS INTERMEDIAS (RELACIONES N:M)
CREATE TABLE LibroAutor (
    id_libro INTEGER NOT NULL,
    id_autor INTEGER NOT NULL,
    tipo_autor VARCHAR(20),
    PRIMARY KEY (id_libro, id_autor),
    CONSTRAINT fk_la_libro FOREIGN KEY (id_libro) REFERENCES Libro(id_libro),
    CONSTRAINT fk_la_autor FOREIGN KEY (id_autor) REFERENCES Autor(id_autor)
);

CREATE TABLE LibroCategoria (
    id_libro INTEGER NOT NULL,
    id_categoria INTEGER NOT NULL,
    PRIMARY KEY (id_libro, id_categoria),
    CONSTRAINT fk_lc_libro FOREIGN KEY (id_libro) REFERENCES Libro(id_libro),
    CONSTRAINT fk_lc_categoria FOREIGN KEY (id_categoria) REFERENCES Categoria(id_categoria)
);

CREATE TABLE LibroPalabraClave (
    id_libro INTEGER NOT NULL,
    id_palabra_clave INTEGER NOT NULL,
    PRIMARY KEY (id_libro, id_palabra_clave),
    CONSTRAINT fk_lp_libro FOREIGN KEY (id_libro) REFERENCES Libro(id_libro),
    CONSTRAINT fk_lp_palabra FOREIGN KEY (id_palabra_clave) REFERENCES PalabraClave(id_palabra_clave)
);

CREATE TABLE PersonaInteres (
    id_persona INTEGER NOT NULL,
    id_categoria INTEGER NOT NULL,
    nivel_interes INTEGER,
    PRIMARY KEY (id_persona, id_categoria),
    CONSTRAINT fk_pi_persona FOREIGN KEY (id_persona) REFERENCES Persona(id_persona),
    CONSTRAINT fk_pi_categoria FOREIGN KEY (id_categoria) REFERENCES Categoria(id_categoria)
);

-- 2.5 PROCESOS DE NEGOCIO (PRÉSTAMOS Y MULTAS)
CREATE TABLE Prestamo (
    id_prestamo SERIAL PRIMARY KEY,
    id_persona INTEGER NOT NULL,
    id_ejemplar INTEGER NOT NULL,
    fecha_prestamo TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_devolucion_estimada TIMESTAMP,
    fecha_devolucion_real TIMESTAMP,
    estado VARCHAR(20) DEFAULT 'Activo', -- Activo, Finalizado, Vencido
    renovaciones INTEGER DEFAULT 0,
    observaciones TEXT,
    CONSTRAINT fk_prestamo_pers FOREIGN KEY (id_persona) REFERENCES Persona(id_persona),
    CONSTRAINT fk_prestamo_ejem FOREIGN KEY (id_ejemplar) REFERENCES Ejemplar(id_ejemplar)
);
CREATE INDEX idx_prestamo_fecha ON Prestamo(fecha_prestamo);
CREATE INDEX idx_prestamo_estado ON Prestamo(estado);

CREATE TABLE Reserva (
    id_reserva SERIAL PRIMARY KEY,
    id_persona INTEGER NOT NULL,
    id_ejemplar INTEGER NOT NULL,
    fecha_reserva TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_vencimiento_reserva TIMESTAMP,
    estado VARCHAR(20) DEFAULT 'Pendiente',
    prioridad INTEGER DEFAULT 1,
    CONSTRAINT fk_reserva_pers FOREIGN KEY (id_persona) REFERENCES Persona(id_persona),
    CONSTRAINT fk_reserva_ejem FOREIGN KEY (id_ejemplar) REFERENCES Ejemplar(id_ejemplar)
);

CREATE TABLE Multa (
    id_multa SERIAL PRIMARY KEY,
    id_prestamo INTEGER UNIQUE NOT NULL,
    motivo VARCHAR(50),
    monto DECIMAL(10,2) NOT NULL,
    fecha_generacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_pago TIMESTAMP,
    estado VARCHAR(20) DEFAULT 'Pendiente', -- Pendiente, Pagada
    descripcion TEXT,
    CONSTRAINT fk_multa_prestamo FOREIGN KEY (id_prestamo) REFERENCES Prestamo(id_prestamo)
);

CREATE TABLE Pago (
    id_pago SERIAL PRIMARY KEY,
    id_multa INTEGER NOT NULL,
    monto_pagado DECIMAL(10,2) NOT NULL,
    metodo_pago VARCHAR(20),
    fecha_pago TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    comprobante VARCHAR(100),
    CONSTRAINT fk_pago_multa FOREIGN KEY (id_multa) REFERENCES Multa(id_multa)
);

CREATE TABLE Sancion (
    id_sancion SERIAL PRIMARY KEY,
    id_persona INTEGER NOT NULL,
    tipo_sancion VARCHAR(20),
    fecha_inicio DATE,
    fecha_fin DATE,
    motivo TEXT,
    estado VARCHAR(20),
    CONSTRAINT fk_sancion_pers FOREIGN KEY (id_persona) REFERENCES Persona(id_persona)
);

-- 2.6 AUDITORÍA Y SEGURIDAD
CREATE TABLE Auditoria (
    id_auditoria SERIAL PRIMARY KEY,
    tabla_afectada VARCHAR(50),
    accion VARCHAR(10),
    fecha_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    usuario VARCHAR(100),
    datos_anteriores JSON,
    datos_nuevos JSON,
    ip_origen VARCHAR(45)
);

-- ==========================================================================
-- 3. LÓGICA DE NEGOCIO (PL/pgSQL) - AUTOMATIZACIÓN
-- ==========================================================================

-- 3.1 FUNCIÓN ESCALAR: Cálculo de Mora
-- Calcula el monto a pagar basado en los días de retraso (50.00 por día)
CREATE OR REPLACE FUNCTION fn_calcular_multa(p_fecha_estimada TIMESTAMP, p_fecha_real TIMESTAMP)
RETURNS DECIMAL(10,2) AS $$
DECLARE
    v_dias INTEGER;
    v_costo_diario DECIMAL(10,2) := 50.00;
BEGIN
    IF p_fecha_real <= p_fecha_estimada THEN RETURN 0.00; END IF;
    
    v_dias := EXTRACT(DAY FROM (p_fecha_real - p_fecha_estimada));
    
    IF v_dias <= 0 THEN RETURN 0.00; END IF;
    
    RETURN v_dias * v_costo_diario;
END;
$$ LANGUAGE plpgsql;

-- 3.2 TRIGGER FUNCTION: Generador Automático de Multas
-- Se dispara al cerrar un préstamo y verifica si hubo retraso
CREATE OR REPLACE FUNCTION fn_trigger_generar_multa()
RETURNS TRIGGER AS $$
DECLARE
    v_monto DECIMAL(10,2);
BEGIN
    IF NEW.estado = 'Finalizado' AND NEW.fecha_devolucion_real IS NOT NULL THEN
        v_monto := fn_calcular_multa(OLD.fecha_devolucion_estimada, NEW.fecha_devolucion_real);
        
        IF v_monto > 0 THEN
            INSERT INTO Multa (id_prestamo, motivo, monto, estado, descripcion)
            VALUES (NEW.id_prestamo, 'Mora Automática', v_monto, 'Pendiente', 'Generada automáticamente por Sistema');
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_verificar_multa_al_devolver
AFTER UPDATE ON Prestamo
FOR EACH ROW
EXECUTE FUNCTION fn_trigger_generar_multa();

-- 3.3 PROCEDIMIENTO ALMACENADO: Gestión de Devoluciones
-- Encapsula la lógica de actualizar inventario y cerrar préstamo en una transacción
CREATE OR REPLACE PROCEDURE sp_registrar_devolucion(p_id_prestamo INTEGER)
LANGUAGE plpgsql AS $$
DECLARE
    v_id_ejemplar INTEGER;
    v_id_libro INTEGER;
BEGIN
    SELECT id_ejemplar INTO v_id_ejemplar FROM Prestamo WHERE id_prestamo = p_id_prestamo;
    SELECT id_libro INTO v_id_libro FROM Ejemplar WHERE id_ejemplar = v_id_ejemplar;

    -- Actualizar préstamo (Dispara Trigger)
    UPDATE Prestamo SET estado = 'Finalizado', fecha_devolucion_real = NOW() WHERE id_prestamo = p_id_prestamo;
    
    -- Liberar Ejemplar
    UPDATE Ejemplar SET estado = 'Disponible' WHERE id_ejemplar = v_id_ejemplar;
    
    -- Actualizar Stock Libro
    UPDATE Libro SET ejemplares_disponibles = ejemplares_disponibles + 1 WHERE id_libro = v_id_libro;
END;
$$;

-- ==========================================================================
-- 4. BASE DE DATOS DISTRIBUIDA (FEDERACIÓN)
-- ==========================================================================

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Limpieza preventiva
DROP FOREIGN TABLE IF EXISTS Autor_Remoto_Link;
DROP USER MAPPING IF EXISTS FOR postgres SERVER servidor_biblioteca_remota;
DROP SERVER IF EXISTS servidor_biblioteca_remota CASCADE;

-- Conexión al contenedor remoto (Docker)
CREATE SERVER servidor_biblioteca_remota
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'postgres-remoto', dbname 'biblioteca_db', port '5432');

-- Mapeo de credenciales
CREATE USER MAPPING FOR postgres
    SERVER servidor_biblioteca_remota
    OPTIONS (user 'postgres', password 'admin123');

-- Tabla Espejo (Foreign Table)
CREATE FOREIGN TABLE IF NOT EXISTS Autor_Remoto_Link (
    id_autor INTEGER,
    nombre_completo VARCHAR(100),
    nacionalidad VARCHAR(50)
)
SERVER servidor_biblioteca_remota
OPTIONS (schema_name 'public', table_name 'autor_remoto');

-- ==========================================================================
-- 5. CARGA DE DATOS DE PRUEBA (DATA SEEDING)
-- ==========================================================================

-- Personas
INSERT INTO Persona (tipo_persona, identificacion, nombre_completo, email, estado) VALUES
('Administrativo', '402-001', 'Roberto Cassá', 'rcassa@biblio.edu', 'Activo'),
('Estudiante', '402-002', 'Ana Julia Quezada', 'ana@est.edu', 'Activo');

INSERT INTO Estudiante (id_persona, codigo_estudiante, carrera) VALUES (2, '2023-001', 'Ingeniería');

-- Autores y Libros Base
INSERT INTO Autor (nombre_completo, nacionalidad) VALUES ('Stephen King', 'USA');
INSERT INTO Categoria (nombre) VALUES ('Tecnología'), ('Literatura');
INSERT INTO Libro (isbn, titulo, anio_publicacion, total_ejemplares, ejemplares_disponibles) VALUES 
('978-001', 'Database Concepts', 2023, 5, 5);
INSERT INTO Ejemplar (codigo_barras, id_libro, numero_ejemplar, estado) VALUES ('DB-001', 1, 1, 'Disponible');

-- CARGA MASIVA (BIG DATA SIMULATION) - 2 MILLONES DE REGISTROS
-- Necesario para demostrar la eficacia de los Índices y el Paralelismo
INSERT INTO Libro (isbn, titulo, anio_publicacion, total_ejemplares, ejemplares_disponibles)
SELECT 
    'GEN-' || generate_series, 
    'Registro Histórico de Archivo #' || generate_series, 
    2023, 5, 5
FROM generate_series(1, 2000000);

-- ==========================================================================
-- 6. CONSULTAS DE VALIDACIÓN (EVIDENCIAS DEL PROYECTO)
-- ==========================================================================

/* -- A. PRUEBA DE TRANSACCIONES (ACID)
   BEGIN; 
   INSERT INTO Pago VALUES (1, 1, 500.00, 'Efectivo', NOW(), 'ERR'); 
   ROLLBACK; -- Debe cancelar todo

-- B. PRUEBA DE INDEXACIÓN (OPTIMIZACIÓN)
   -- Sin Índice (Lento):
   EXPLAIN ANALYZE SELECT * FROM Libro WHERE titulo = 'Registro Histórico de Archivo #1999999';
   -- Con Índice (Rápido):
   CREATE INDEX idx_libro_titulo_demo ON Libro(titulo);
   EXPLAIN ANALYZE SELECT * FROM Libro WHERE titulo = 'Registro Histórico de Archivo #1999999';

-- C. PRUEBA DISTRIBUIDA
   SELECT * FROM Autor_Remoto_Link;

-- D. PRUEBA PARALELA (WORKERS)
   SET max_parallel_workers_per_gather = 4;
   EXPLAIN ANALYZE SELECT anio_publicacion, COUNT(*) FROM Libro GROUP BY anio_publicacion;
*/