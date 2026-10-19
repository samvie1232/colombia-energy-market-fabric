CREATE TABLE [gold].[fct_sistema_dia] (
    [fecha_key]                 INT             NOT NULL,
    [precio_bolsa_prom_cop_kwh] DECIMAL (18, 4) NULL,
    [precio_bolsa_max_cop_kwh]  DECIMAL (18, 4) NULL,
    [precio_escasez_cop_kwh]    DECIMAL (18, 4) NULL,
    [horas_sobre_escasez]       INT             NULL,
    [demanda_kwh]               DECIMAL (18, 4) NULL,
    [generacion_kwh]            DECIMAL (18, 4) NULL,
    [volumen_util_total_kwh]    DECIMAL (18, 4) NULL,
    [aportes_kwh]               DECIMAL (18, 4) NULL
);


GO