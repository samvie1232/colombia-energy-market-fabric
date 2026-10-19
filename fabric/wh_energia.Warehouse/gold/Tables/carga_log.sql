CREATE TABLE [gold].[carga_log] (
    [run_id]       VARCHAR (36)  NOT NULL,
    [tabla]        VARCHAR (50)  NOT NULL,
    [rango_inicio] DATE          NULL,
    [rango_fin]    DATE          NULL,
    [filas]        BIGINT        NULL,
    [cargado_utc]  DATETIME2 (6) NOT NULL
);


GO