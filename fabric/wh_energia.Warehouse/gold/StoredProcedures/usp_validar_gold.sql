-- =============================================================================
-- gold.usp_validar_gold: reglas de calidad sobre las tablas Gold en el rango cargado.
-- Guarda el resultado de cada regla en gold.calidad_resultados y falla (THROW) si alguna
-- regla de severidad "error" encuentra filas, para que el pipeline marque la corrida.
-- =============================================================================
CREATE   PROCEDURE gold.usp_validar_gold
    @run_id varchar(36),
    @inicio date,
    @fin    date
AS
BEGIN
    DECLARE @k_ini int = YEAR(@inicio) * 10000 + MONTH(@inicio) * 100 + DAY(@inicio);
    DECLARE @k_fin int = YEAR(@fin) * 10000 + MONTH(@fin) * 100 + DAY(@fin);
    DECLARE @ahora datetime2(6) = SYSUTCDATETIME();
    DECLARE @hoy_co date = CAST(DATEADD(HOUR, -5, @ahora) AS date);   -- Colombia, UTC−5 sin horario de verano

    -- ---------------------------------------------------------- fct_precio_hora
    INSERT INTO gold.calidad_resultados
    SELECT @run_id, 'fct_precio_hora', 'llave (fecha, hora) única', 'error', COUNT(*), @ahora
    FROM (SELECT fecha_key, hora_key FROM gold.fct_precio_hora
          WHERE fecha_key BETWEEN @k_ini AND @k_fin
          GROUP BY fecha_key, hora_key HAVING COUNT(*) > 1) d;

    INSERT INTO gold.calidad_resultados
    SELECT @run_id, 'fct_precio_hora', 'precio de bolsa no nulo', 'error', COUNT(*), @ahora
    FROM gold.fct_precio_hora
    WHERE fecha_key BETWEEN @k_ini AND @k_fin AND precio_bolsa_cop_kwh IS NULL;

    INSERT INTO gold.calidad_resultados
    SELECT @run_id, 'fct_precio_hora', 'precio de bolsa entre 0 y 5000 COP/kWh', 'alerta', COUNT(*), @ahora
    FROM gold.fct_precio_hora
    WHERE fecha_key BETWEEN @k_ini AND @k_fin
      AND (precio_bolsa_cop_kwh <= 0 OR precio_bolsa_cop_kwh >= 5000);

    INSERT INTO gold.calidad_resultados
    SELECT @run_id, 'fct_precio_hora', '24 horas por día', 'alerta', COUNT(*), @ahora
    FROM (SELECT fecha_key FROM gold.fct_precio_hora
          WHERE fecha_key BETWEEN @k_ini AND @k_fin
          GROUP BY fecha_key HAVING COUNT(DISTINCT hora_key) <> 24) d;

    INSERT INTO gold.calidad_resultados
    SELECT @run_id, 'fct_precio_hora', 'frescura: último día con precio <= 5 días', 'alerta',
           CASE WHEN MAX(f.fecha) IS NULL OR DATEDIFF(DAY, MAX(f.fecha), @hoy_co) > 5 THEN 1 ELSE 0 END, @ahora
    FROM gold.fct_precio_hora p
    JOIN gold.dim_fecha f ON f.fecha_key = p.fecha_key;

    -- ---------------------------------------------------------- fct_generacion_hora
    INSERT INTO gold.calidad_resultados
    SELECT @run_id, 'fct_generacion_hora', 'llave (fecha, hora, recurso) única', 'error', COUNT(*), @ahora
    FROM (SELECT fecha_key, hora_key, recurso_key FROM gold.fct_generacion_hora
          WHERE fecha_key BETWEEN @k_ini AND @k_fin
          GROUP BY fecha_key, hora_key, recurso_key HAVING COUNT(*) > 1) d;

    INSERT INTO gold.calidad_resultados
    SELECT @run_id, 'fct_generacion_hora', 'recurso existe en dim_recurso', 'error', COUNT(*), @ahora
    FROM gold.fct_generacion_hora g
    LEFT JOIN gold.dim_recurso r ON r.recurso_key = g.recurso_key
    WHERE g.fecha_key BETWEEN @k_ini AND @k_fin AND r.recurso_key IS NULL;

    INSERT INTO gold.calidad_resultados
    SELECT @run_id, 'fct_generacion_hora', 'generación >= 0', 'alerta', COUNT(*), @ahora
    FROM gold.fct_generacion_hora
    WHERE fecha_key BETWEEN @k_ini AND @k_fin AND generacion_kwh < 0;

    -- ---------------------------------------------------------- fct_embalse_dia
    INSERT INTO gold.calidad_resultados
    SELECT @run_id, 'fct_embalse_dia', 'llave (fecha, embalse) única', 'error', COUNT(*), @ahora
    FROM (SELECT fecha_key, embalse FROM gold.fct_embalse_dia
          WHERE fecha_key BETWEEN @k_ini AND @k_fin
          GROUP BY fecha_key, embalse HAVING COUNT(*) > 1) d;

    INSERT INTO gold.calidad_resultados
    SELECT @run_id, 'fct_embalse_dia', 'volumen útil >= 0', 'alerta', COUNT(*), @ahora
    FROM gold.fct_embalse_dia
    WHERE fecha_key BETWEEN @k_ini AND @k_fin AND volumen_util_kwh < 0;

    -- ---------------------------------------------------------- fechas huérfanas (todas las tablas de hechos)
    INSERT INTO gold.calidad_resultados
    SELECT @run_id, h.tabla, 'fecha existe en dim_fecha', 'error', COUNT(f.fecha_key) - COUNT(d.fecha_key), @ahora
    FROM (SELECT 'fct_precio_hora' AS tabla, fecha_key FROM gold.fct_precio_hora
          UNION ALL SELECT 'fct_generacion_hora', fecha_key FROM gold.fct_generacion_hora
          UNION ALL SELECT 'fct_embalse_dia', fecha_key FROM gold.fct_embalse_dia
          UNION ALL SELECT 'fct_sistema_dia', fecha_key FROM gold.fct_sistema_dia) f
    JOIN (VALUES ('fct_precio_hora'), ('fct_generacion_hora'), ('fct_embalse_dia'), ('fct_sistema_dia')) h(tabla)
      ON h.tabla = f.tabla
    LEFT JOIN gold.dim_fecha d ON d.fecha_key = f.fecha_key
    WHERE f.fecha_key BETWEEN @k_ini AND @k_fin
    GROUP BY h.tabla;

    -- ---------------------------------------------------------- resultado
    SELECT tabla, regla, severidad, filas_fallidas
    FROM gold.calidad_resultados
    WHERE run_id = @run_id
    ORDER BY CASE WHEN filas_fallidas > 0 THEN 0 ELSE 1 END, severidad, tabla;

    DECLARE @errores int = (SELECT COUNT(*) FROM gold.calidad_resultados
                            WHERE run_id = @run_id AND severidad = 'error' AND filas_fallidas > 0);
    IF @errores > 0
    BEGIN
        DECLARE @msg nvarchar(200) = CONCAT('Validación de Gold falló: ', @errores,
                                            ' reglas con errores. Ver gold.calidad_resultados, run_id ', @run_id);
        THROW 50001, @msg, 1;
    END
END;

GO