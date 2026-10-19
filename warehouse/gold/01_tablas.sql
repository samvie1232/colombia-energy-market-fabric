-- =============================================================================
-- Gold: modelo dimensional del mercado eléctrico colombiano (Warehouse wh_energia)
-- Se corre una sola vez (o cuando cambie el modelo). Es idempotente: DROP IF EXISTS + CREATE.
--
-- Notas de Fabric Warehouse:
--   * Tipos: varchar (no nvarchar), datetime2(6) (no datetime), smallint (no tinyint).
--   * Las llaves primarias son NOT ENFORCED: sirven de pista al optimizador y a Power BI;
--     la unicidad real la comprueba gold.usp_validar_gold.
--   * Llaves "inteligentes": fecha_key = AAAAMMDD, hora_key = 0..23, recurso_key = código SIC de XM.
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

-- ---------------------------------------------------------------- dimensiones
DROP TABLE IF EXISTS gold.dim_fecha;
CREATE TABLE gold.dim_fecha (
    fecha_key        int          NOT NULL,   -- 20240131
    fecha            date         NOT NULL,
    anio             smallint     NOT NULL,
    trimestre        smallint     NOT NULL,
    mes              smallint     NOT NULL,
    nombre_mes       varchar(12)  NOT NULL,
    anio_mes         char(7)      NOT NULL,   -- 2024-01
    dia              smallint     NOT NULL,
    dia_semana       smallint     NOT NULL,   -- 1 = lunes … 7 = domingo
    nombre_dia       varchar(10)  NOT NULL,
    es_fin_de_semana bit          NOT NULL
);
ALTER TABLE gold.dim_fecha ADD CONSTRAINT pk_dim_fecha PRIMARY KEY NONCLUSTERED (fecha_key) NOT ENFORCED;

DROP TABLE IF EXISTS gold.dim_hora;
CREATE TABLE gold.dim_hora (
    hora_key      smallint    NOT NULL,   -- 0 = 00:00–01:00 (Hour01 de XM)
    hora_texto    char(5)     NOT NULL,   -- 00:00
    franja        varchar(10) NOT NULL,   -- madrugada, mañana, tarde, noche
    es_hora_punta bit         NOT NULL    -- 18:00–21:00, pico de demanda en Colombia
);
ALTER TABLE gold.dim_hora ADD CONSTRAINT pk_dim_hora PRIMARY KEY NONCLUSTERED (hora_key) NOT ENFORCED;

DROP TABLE IF EXISTS gold.dim_recurso;
CREATE TABLE gold.dim_recurso (
    recurso_key             varchar(20)  NOT NULL,   -- código SIC, p. ej. 2QBW
    nombre                  varchar(200) NOT NULL,
    tecnologia              varchar(20)  NOT NULL,   -- Hidráulica, Térmica, Solar, Eólica, Biomasa, Otra, Sin clasificar
    fuente_energia          varchar(100) NULL,       -- enersource tal como lo publica XM
    tipo_despacho           varchar(100) NULL,       -- disp
    tipo_recurso            varchar(100) NULL,       -- type
    agente                  varchar(100) NULL,       -- companycode
    estado                  varchar(100) NULL,
    fecha_inicio_operacion  date         NULL,
    en_listado_xm           bit          NOT NULL    -- 0 = aparece en Gene pero no en ListadoRecursos
);
ALTER TABLE gold.dim_recurso ADD CONSTRAINT pk_dim_recurso PRIMARY KEY NONCLUSTERED (recurso_key) NOT ENFORCED;

-- ---------------------------------------------------------------- hechos
-- Precio y demanda del sistema por hora (misma granularidad), con el precio de escasez del día
-- para el indicador de estrés.
DROP TABLE IF EXISTS gold.fct_precio_hora;
CREATE TABLE gold.fct_precio_hora (
    fecha_key              int            NOT NULL,
    hora_key               smallint       NOT NULL,
    ts_utc                 datetime2(6)   NOT NULL,
    precio_bolsa_cop_kwh   decimal(18,4)  NULL,
    demanda_kwh            decimal(18,4)  NULL,
    precio_escasez_cop_kwh decimal(18,4)  NULL,
    sobre_escasez          bit            NULL       -- 1 si el precio de bolsa supera el de escasez
);

-- Generación real por planta y hora. Las horas vacías de XM (planta sin generar) se cargan como 0.
DROP TABLE IF EXISTS gold.fct_generacion_hora;
CREATE TABLE gold.fct_generacion_hora (
    fecha_key       int            NOT NULL,
    hora_key        smallint       NOT NULL,
    recurso_key     varchar(20)    NOT NULL,
    generacion_kwh  decimal(18,4)  NOT NULL
);

-- Volumen útil por embalse y día.
DROP TABLE IF EXISTS gold.fct_embalse_dia;
CREATE TABLE gold.fct_embalse_dia (
    fecha_key         int            NOT NULL,
    embalse           varchar(100)   NOT NULL,
    volumen_util_kwh  decimal(18,4)  NULL
);

-- Resumen diario del sistema: hidrología y precio en una sola fila por día (para "precio vs. embalses").
DROP TABLE IF EXISTS gold.fct_sistema_dia;
CREATE TABLE gold.fct_sistema_dia (
    fecha_key                    int            NOT NULL,
    precio_bolsa_prom_cop_kwh    decimal(18,4)  NULL,
    precio_bolsa_max_cop_kwh     decimal(18,4)  NULL,
    precio_escasez_cop_kwh       decimal(18,4)  NULL,
    horas_sobre_escasez          int            NULL,
    demanda_kwh                  decimal(18,4)  NULL,
    generacion_kwh               decimal(18,4)  NULL,
    volumen_util_total_kwh       decimal(18,4)  NULL,
    aportes_kwh                  decimal(18,4)  NULL
);

-- ---------------------------------------------------------------- bitácoras
DROP TABLE IF EXISTS gold.carga_log;
CREATE TABLE gold.carga_log (
    run_id        varchar(36)  NOT NULL,
    tabla         varchar(50)  NOT NULL,
    rango_inicio  date         NULL,
    rango_fin     date         NULL,
    filas         bigint       NULL,
    cargado_utc   datetime2(6) NOT NULL
);

DROP TABLE IF EXISTS gold.calidad_resultados;
CREATE TABLE gold.calidad_resultados (
    run_id          varchar(36)  NOT NULL,
    tabla           varchar(50)  NOT NULL,
    regla           varchar(100) NOT NULL,
    severidad       varchar(10)  NOT NULL,   -- error | alerta
    filas_fallidas  bigint       NOT NULL,
    validado_utc    datetime2(6) NOT NULL
);
GO
