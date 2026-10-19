CREATE TABLE [gold].[dim_recurso] (
    [recurso_key]            VARCHAR (20)  NOT NULL,
    [nombre]                 VARCHAR (200) NOT NULL,
    [tecnologia]             VARCHAR (20)  NOT NULL,
    [fuente_energia]         VARCHAR (100) NULL,
    [tipo_despacho]          VARCHAR (100) NULL,
    [tipo_recurso]           VARCHAR (100) NULL,
    [agente]                 VARCHAR (100) NULL,
    [estado]                 VARCHAR (100) NULL,
    [fecha_inicio_operacion] DATE          NULL,
    [en_listado_xm]          BIT           NOT NULL
);


GO

ALTER TABLE [gold].[dim_recurso]
    ADD CONSTRAINT [pk_dim_recurso] PRIMARY KEY NONCLUSTERED ([recurso_key] ASC) NOT ENFORCED;


GO