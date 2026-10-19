CREATE TABLE [gold].[dim_hora] (
    [hora_key]      SMALLINT     NOT NULL,
    [hora_texto]    CHAR (5)     NOT NULL,
    [franja]        VARCHAR (10) NOT NULL,
    [es_hora_punta] BIT          NOT NULL
);


GO

ALTER TABLE [gold].[dim_hora]
    ADD CONSTRAINT [pk_dim_hora] PRIMARY KEY NONCLUSTERED ([hora_key] ASC) NOT ENFORCED;


GO