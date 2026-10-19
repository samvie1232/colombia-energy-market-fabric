CREATE TABLE [gold].[dim_fecha] (
    [fecha_key]        INT          NOT NULL,
    [fecha]            DATE         NOT NULL,
    [anio]             SMALLINT     NOT NULL,
    [trimestre]        SMALLINT     NOT NULL,
    [mes]              SMALLINT     NOT NULL,
    [nombre_mes]       VARCHAR (12) NOT NULL,
    [anio_mes]         CHAR (7)     NOT NULL,
    [dia]              SMALLINT     NOT NULL,
    [dia_semana]       SMALLINT     NOT NULL,
    [nombre_dia]       VARCHAR (10) NOT NULL,
    [es_fin_de_semana] BIT          NOT NULL
);


GO

ALTER TABLE [gold].[dim_fecha]
    ADD CONSTRAINT [pk_dim_fecha] PRIMARY KEY NONCLUSTERED ([fecha_key] ASC) NOT ENFORCED;


GO