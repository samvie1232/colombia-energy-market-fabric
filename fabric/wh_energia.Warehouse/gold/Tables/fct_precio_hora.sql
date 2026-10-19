CREATE TABLE [gold].[fct_precio_hora] (
    [fecha_key]              INT             NOT NULL,
    [hora_key]               SMALLINT        NOT NULL,
    [ts_utc]                 DATETIME2 (6)   NOT NULL,
    [precio_bolsa_cop_kwh]   DECIMAL (18, 4) NULL,
    [demanda_kwh]            DECIMAL (18, 4) NULL,
    [precio_escasez_cop_kwh] DECIMAL (18, 4) NULL,
    [sobre_escasez]          BIT             NULL
);


GO