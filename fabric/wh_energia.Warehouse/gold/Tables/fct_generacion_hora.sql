CREATE TABLE [gold].[fct_generacion_hora] (
    [fecha_key]      INT             NOT NULL,
    [hora_key]       SMALLINT        NOT NULL,
    [recurso_key]    VARCHAR (20)    NOT NULL,
    [generacion_kwh] DECIMAL (18, 4) NOT NULL
);


GO