CREATE TABLE [gold].[calidad_resultados] (
    [run_id]         VARCHAR (36)  NOT NULL,
    [tabla]          VARCHAR (50)  NOT NULL,
    [regla]          VARCHAR (100) NOT NULL,
    [severidad]      VARCHAR (10)  NOT NULL,
    [filas_fallidas] BIGINT        NOT NULL,
    [validado_utc]   DATETIME2 (6) NOT NULL
);


GO