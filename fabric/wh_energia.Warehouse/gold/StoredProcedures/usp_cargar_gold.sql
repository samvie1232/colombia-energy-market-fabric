-- =============================================================================
-- gold.usp_cargar_gold: carga Gold desde las tablas Silver del Lakehouse lh_energia
-- (consulta entre bases de datos del mismo workspace: lh_energia.silver.<tabla>).
--
--   EXEC gold.usp_cargar_gold;                                  -- incremental: últimos 10 días
--   EXEC gold.usp_cargar_gold @inicio = '2022-01-01';           -- carga completa
--   EXEC gold.usp_cargar_gold @inicio = '2024-03-01', @fin = '2024-03-31';
--
-- Idempotente: las dimensiones se reconstruyen completas (son pequeñas) y los hechos se
-- reemplazan en el rango (DELETE + INSERT dentro de una transacción). Al final llama a
-- gold.usp_validar_gold, que falla si alguna regla de error no se cumple.
-- =============================================================================
CREATE   PROCEDURE gold.usp_cargar_gold
    @inicio date = NULL,
    @fin    date = NULL
AS
BEGIN
    DECLARE @run_id varchar(36) = CAST(NEWID() AS varchar(36));
    DECLARE @hoy_co date = CAST(DATEADD(HOUR, -5, SYSUTCDATETIME()) AS date);   -- Colombia, UTC−5
    SET @fin = COALESCE(@fin, @hoy_co);
    SET @inicio = COALESCE(@inicio, DATEADD(DAY, -10, @fin));   -- 10 días: cubre los 7 de Silver más el retraso de XM
    IF @inicio > @fin
        THROW 50000, 'inicio es posterior a fin', 1;

    DECLARE @k_ini int = YEAR(@inicio) * 10000 + MONTH(@inicio) * 100 + DAY(@inicio);
    DECLARE @k_fin int = YEAR(@fin) * 10000 + MONTH(@fin) * 100 + DAY(@fin);

    -- ------------------------------------------------------------------ dim_fecha (2022–2030)
    TRUNCATE TABLE gold.dim_fecha;
    WITH d AS (SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)),
         numeros AS (SELECT a.n + 10 * b.n + 100 * c.n + 1000 * e.n AS n
                     FROM d a CROSS JOIN d b CROSS JOIN d c CROSS JOIN d e),
         fechas AS (SELECT DATEADD(DAY, n, CAST('2022-01-01' AS date)) AS fecha
                    FROM numeros
                    WHERE n <= DATEDIFF(DAY, '2022-01-01', '2030-12-31'))
    INSERT INTO gold.dim_fecha
    SELECT YEAR(fecha) * 10000 + MONTH(fecha) * 100 + DAY(fecha),
           fecha,
           YEAR(fecha),
           DATEPART(QUARTER, fecha),
           MONTH(fecha),
           CHOOSE(MONTH(fecha), 'enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio',
                  'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'),
           CONCAT(YEAR(fecha), '-', RIGHT(CONCAT('0', MONTH(fecha)), 2)),
           DAY(fecha),
           ds.dia_semana,
           CHOOSE(ds.dia_semana, 'lunes', 'martes', 'miércoles', 'jueves', 'viernes', 'sábado', 'domingo'),
           CASE WHEN ds.dia_semana >= 6 THEN 1 ELSE 0 END
    FROM fechas
    -- 1900-01-01 fue lunes: así el día de la semana no depende de SET DATEFIRST ni del idioma.
    CROSS APPLY (SELECT DATEDIFF(DAY, '1900-01-01', fecha) % 7 + 1 AS dia_semana) ds;

    -- ------------------------------------------------------------------ dim_hora
    TRUNCATE TABLE gold.dim_hora;
    WITH h AS (SELECT a.n + 10 * b.n AS hora
               FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) a(n)
               CROSS JOIN (VALUES (0),(1),(2)) b(n))
    INSERT INTO gold.dim_hora
    SELECT hora,
           CONCAT(RIGHT(CONCAT('0', hora), 2), ':00'),
           CASE WHEN hora < 6 THEN 'madrugada' WHEN hora < 12 THEN 'mañana'
                WHEN hora < 18 THEN 'tarde' ELSE 'noche' END,
           CASE WHEN hora BETWEEN 18 AND 20 THEN 1 ELSE 0 END
    FROM h
    WHERE hora < 24;

    -- ------------------------------------------------------------------ dim_recurso
    TRUNCATE TABLE gold.dim_recurso;
    WITH listado AS (
        SELECT UPPER(TRIM(code)) AS codigo, name, enersource, disp, [type], companycode, [state], operstartdate,
               ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(code)) ORDER BY name) AS rn
        FROM lh_energia.silver.recurso
        WHERE code IS NOT NULL
    )
    INSERT INTO gold.dim_recurso
    SELECT codigo,
           COALESCE(NULLIF(TRIM(name), ''), codigo),
           CASE
               WHEN UPPER(enersource) LIKE '%AGUA%' OR UPPER(enersource) LIKE '%HIDR%' THEN 'Hidráulica'
               WHEN UPPER(enersource) LIKE '%SOL%' OR UPPER(enersource) LIKE '%RADIACI%' THEN 'Solar'
               WHEN UPPER(enersource) LIKE '%VIENTO%' OR UPPER(enersource) LIKE '%E_LIC%' THEN 'Eólica'
               WHEN UPPER(enersource) LIKE '%BAGAZO%' OR UPPER(enersource) LIKE '%BIOMASA%'
                 OR UPPER(enersource) LIKE '%BIOG%' OR UPPER(enersource) LIKE '%CASCARILLA%' THEN 'Biomasa'
               WHEN UPPER(enersource) LIKE '%GAS%' OR UPPER(enersource) LIKE '%CARB%'
                 OR UPPER(enersource) LIKE '%ACPM%' OR UPPER(enersource) LIKE '%COMBUST%'
                 OR UPPER(enersource) LIKE '%JET%' OR UPPER(enersource) LIKE '%DIESEL%'
                 OR UPPER(enersource) LIKE '%FUEL%' OR UPPER(enersource) LIKE '%GLP%' THEN 'Térmica'
               WHEN enersource IS NULL OR TRIM(enersource) = '' THEN 'Sin clasificar'
               ELSE 'Otra'
           END,
           enersource, disp, [type], companycode, [state],
           TRY_CAST(LEFT(operstartdate, 10) AS date),
           1
    FROM listado
    WHERE rn = 1;

    -- Recursos que generan pero no están en el listado: se agregan para no perder filas de hechos.
    INSERT INTO gold.dim_recurso
    SELECT DISTINCT g.codigo, g.codigo, 'Sin clasificar', NULL, NULL, NULL, NULL, NULL, NULL, 0
    FROM (SELECT UPPER(TRIM(codigo_recurso)) AS codigo FROM lh_energia.silver.generacion_recurso_hora) g
    WHERE g.codigo IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM gold.dim_recurso r WHERE r.recurso_key = g.codigo);

    INSERT INTO gold.carga_log
    SELECT @run_id, t.tabla, NULL, NULL, t.filas, SYSUTCDATETIME()
    FROM (SELECT 'dim_fecha' AS tabla, COUNT_BIG(*) AS filas FROM gold.dim_fecha
          UNION ALL SELECT 'dim_hora', COUNT_BIG(*) FROM gold.dim_hora
          UNION ALL SELECT 'dim_recurso', COUNT_BIG(*) FROM gold.dim_recurso) t;

    -- ------------------------------------------------------------------ hechos
    BEGIN TRY
        BEGIN TRANSACTION;

        -- Precio + demanda por hora; FULL JOIN por si una de las dos métricas llega antes que la otra.
        DELETE FROM gold.fct_precio_hora WHERE fecha_key BETWEEN @k_ini AND @k_fin;
        INSERT INTO gold.fct_precio_hora
        SELECT YEAR(x.fecha) * 10000 + MONTH(x.fecha) * 100 + DAY(x.fecha),
               x.hora, x.ts_utc, x.precio_cop_kwh, x.demanda_kwh, e.precio_escasez_cop_kwh,
               CASE WHEN x.precio_cop_kwh IS NULL OR e.precio_escasez_cop_kwh IS NULL THEN NULL
                    WHEN x.precio_cop_kwh > e.precio_escasez_cop_kwh THEN 1 ELSE 0 END
        FROM (SELECT COALESCE(p.fecha, d.fecha) AS fecha, COALESCE(p.hora, d.hora) AS hora,
                     COALESCE(p.ts_utc, d.ts_utc) AS ts_utc, p.precio_cop_kwh, d.demanda_kwh
              FROM (SELECT * FROM lh_energia.silver.precio_bolsa_hora WHERE fecha BETWEEN @inicio AND @fin) p
              FULL JOIN (SELECT * FROM lh_energia.silver.demanda_hora WHERE fecha BETWEEN @inicio AND @fin) d
                ON d.fecha = p.fecha AND d.hora = p.hora) x
        LEFT JOIN lh_energia.silver.precio_escasez_dia e ON e.fecha = x.fecha;

        -- Generación: null de XM = hora sin generar = 0 kWh (ver docs/data_contract.md).
        DELETE FROM gold.fct_generacion_hora WHERE fecha_key BETWEEN @k_ini AND @k_fin;
        INSERT INTO gold.fct_generacion_hora
        SELECT YEAR(fecha) * 10000 + MONTH(fecha) * 100 + DAY(fecha), hora, UPPER(TRIM(codigo_recurso)),
               COALESCE(generacion_kwh, 0)
        FROM lh_energia.silver.generacion_recurso_hora
        WHERE fecha BETWEEN @inicio AND @fin AND codigo_recurso IS NOT NULL;

        DELETE FROM gold.fct_embalse_dia WHERE fecha_key BETWEEN @k_ini AND @k_fin;
        INSERT INTO gold.fct_embalse_dia
        SELECT YEAR(fecha) * 10000 + MONTH(fecha) * 100 + DAY(fecha), embalse, volumen_util_kwh
        FROM lh_energia.silver.embalse_dia
        WHERE fecha BETWEEN @inicio AND @fin AND embalse IS NOT NULL;

        -- Resumen diario, calculado sobre los hechos ya cargados.
        DELETE FROM gold.fct_sistema_dia WHERE fecha_key BETWEEN @k_ini AND @k_fin;
        INSERT INTO gold.fct_sistema_dia
        SELECT f.fecha_key, p.prom, p.maximo, es.precio_escasez_cop_kwh, p.horas_sobre, p.demanda,
               g.generacion, em.volumen, ap.aportes_kwh
        FROM gold.dim_fecha f
        LEFT JOIN (SELECT fecha_key, AVG(precio_bolsa_cop_kwh) AS prom, MAX(precio_bolsa_cop_kwh) AS maximo,
                          SUM(CAST(sobre_escasez AS int)) AS horas_sobre, SUM(demanda_kwh) AS demanda
                   FROM gold.fct_precio_hora WHERE fecha_key BETWEEN @k_ini AND @k_fin
                   GROUP BY fecha_key) p ON p.fecha_key = f.fecha_key
        LEFT JOIN (SELECT fecha_key, SUM(generacion_kwh) AS generacion
                   FROM gold.fct_generacion_hora WHERE fecha_key BETWEEN @k_ini AND @k_fin
                   GROUP BY fecha_key) g ON g.fecha_key = f.fecha_key
        LEFT JOIN (SELECT fecha_key, SUM(volumen_util_kwh) AS volumen
                   FROM gold.fct_embalse_dia WHERE fecha_key BETWEEN @k_ini AND @k_fin
                   GROUP BY fecha_key) em ON em.fecha_key = f.fecha_key
        LEFT JOIN lh_energia.silver.aportes_dia ap ON ap.fecha = f.fecha
        LEFT JOIN lh_energia.silver.precio_escasez_dia es ON es.fecha = f.fecha
        WHERE f.fecha_key BETWEEN @k_ini AND @k_fin
          AND COALESCE(p.prom, g.generacion, em.volumen, ap.aportes_kwh, es.precio_escasez_cop_kwh) IS NOT NULL;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;

    INSERT INTO gold.carga_log
    SELECT @run_id, t.tabla, @inicio, @fin, t.filas, SYSUTCDATETIME()
    FROM (SELECT 'fct_precio_hora' AS tabla, COUNT_BIG(*) AS filas FROM gold.fct_precio_hora WHERE fecha_key BETWEEN @k_ini AND @k_fin
          UNION ALL SELECT 'fct_generacion_hora', COUNT_BIG(*) FROM gold.fct_generacion_hora WHERE fecha_key BETWEEN @k_ini AND @k_fin
          UNION ALL SELECT 'fct_embalse_dia', COUNT_BIG(*) FROM gold.fct_embalse_dia WHERE fecha_key BETWEEN @k_ini AND @k_fin
          UNION ALL SELECT 'fct_sistema_dia', COUNT_BIG(*) FROM gold.fct_sistema_dia WHERE fecha_key BETWEEN @k_ini AND @k_fin) t;

    SELECT tabla, filas FROM gold.carga_log WHERE run_id = @run_id;

    EXEC gold.usp_validar_gold @run_id = @run_id, @inicio = @inicio, @fin = @fin;
END;

GO