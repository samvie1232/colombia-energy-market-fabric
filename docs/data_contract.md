# Contrato de datos — Mercado eléctrico colombiano (XM)

> **Estado: v1, validado el 2026-10-19** con `scripts/explorar_xm.py` contra la API en vivo
> (193 combinaciones métrica/cruce en el inventario, guardado en `docs/xm_metricas.csv`).

## Fuente

| Campo | Valor |
| --- | --- |
| Proveedor | XM S.A. E.S.P., operador del mercado eléctrico colombiano |
| API | `https://servapibi.xm.com.co/{hourly,daily,monthly,annual,lists}` (POST, JSON, sin autenticación) |
| Librería de referencia | `pydataxm` 0.3.19 (`ReadDB` para la API XM, `pydatasimem` para SiMEM) |
| Alternativa | SiMEM: `https://www.simem.co/backend-files/api/PublicData` (por `datasetId`) |
| Licencia | Datos públicos del sector, sin restricción de uso (citar a XM como fuente) |
| Inventario de métricas | `POST /lists` con `{"MetricId": "ListadoMetricas"}` |

### Forma de la petición

```json
POST https://servapibi.xm.com.co/hourly
{"MetricId": "PrecBolsNaci", "Entity": "Sistema",
 "StartDate": "2024-01-01", "EndDate": "2024-01-30", "Filter": []}
```

La respuesta trae `Items[]`, cada uno con `Date` y una lista (`HourlyEntities`, `DailyEntities`…)
cuyos `Values` traen el código de la entidad y las columnas `Hour01`…`Hour24` en las métricas horarias.

### Límites de consulta

| Granularidad | Rango máximo por petición |
| --- | --- |
| Horaria / diaria | 31 días (`MaxDays` del inventario) |
| Mensual | 731 días |
| Anual | 366 días |

Consecuencia: la carga histórica desde 2022 se parte en ventanas de ≤ 31 días (se usa un mes calendario)
(~48 peticiones por métrica horaria). En SiMEM la ventana es de 1 día para datos diarios u horarios.

## Métricas del MVP

| Métrica | MetricId | Cruce usado | Otros cruces | Granularidad | Unidad | Retraso observado | Tabla Silver |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Precio de bolsa nacional | `PrecBolsNaci` | Sistema | — | horaria | COP/kWh | 3 días | `precio_bolsa_hora` |
| Demanda comercial | `DemaCome` | Sistema | Agente, MercadoComercializacion | horaria | kWh | 3 días | `demanda_hora` |
| Generación real | `Gene` | Recurso | Sistema | horaria | kWh | 3 días | `generacion_recurso_hora` |
| Volumen útil de embalses | `VoluUtilDiarEner` | Embalse | Sistema | diaria | kWh | 1 día | `embalse_dia` |
| Aportes hídricos | `AporEner` | Sistema | Rio | diaria | kWh | 1 día | `aportes_dia` |
| Precio de escasez | `PrecEsca` | Sistema | — | diaria | COP/kWh | 3 días | `precio_escasez_dia` |
| Maestro de recursos | `ListadoRecursos` | Sistema | — | listado | — | — | `dim_recurso` |

Retraso medido el 2026-10-19: última fecha publicada 2026-10-16 (horarias y precio de escasez) y 2026-10-18 (embalses y aportes).

### Forma de la respuesta (aplanada con `pd.json_normalize(Items, <Type>, "Date")`)

- **Horarias** (`HourlyEntities`): `Id` (nombre del cruce), `Values_code` (código: `Sistema` o el código SIC del recurso, p. ej. `2QBW`),
  `Values_Hour01` … `Values_Hour24`, `Date`. Una fila por entidad y día.
- **Diarias** (`DailyEntities`): `Id`, `Value`, `Date`; por Embalse además `Name` (p. ej. `AGREGADO BOGOTA`, `ALTOANCHICAYA`, `AMANI`).

### Volumen observado (muestra de 2 días)

- `Gene` por Recurso: 1.088 filas → ~544 recursos/día × 24 horas ≈ 13.000 filas/día despivotadas, ~23 M filas desde 2022.
- `VoluUtilDiarEner` por Embalse: 48 filas → 24 embalses/día.
- Primeros valores: precio de bolsa entre 432 y 1.093 COP/kWh el 2026-10-15; precio de escasez 763,11 COP/kWh.
  Ese mismo día hubo horas con el precio de bolsa por encima del de escasez, lo que alimenta el indicador de estrés.

## Reglas de transformación (Bronze → Silver)

- **Bronze:** JSON crudo tal como responde la API, en `Files/bronze/xm/{metric}/{entity}/fecha_carga=YYYY-MM-DD/{start}_{end}.json`.
  Volver a cargar la misma ventana sobrescribe el mismo archivo (idempotencia por ruta).
- **Despivotar** `Hour01..Hour24` a filas `(fecha, hora, valor)`. `Hour01` es la hora 00:00–01:00.
- **Zona horaria:** las fechas de XM están en hora de Colombia (UTC−5, sin horario de verano).
  Se guarda `ts_colombia` y `ts_utc`.
- **Llave natural:** `(fecha, hora, codigo_entidad)` para horarias y `(fecha, codigo_entidad)` para diarias.
  Escritura con `MERGE` en Delta sobre esa llave, de modo que las reliquidaciones de XM actualizan el valor.
- **Tipos:** valores a `decimal(18,4)`; vacíos a `null`, nunca a 0.

## Calidad (se valida en Silver y en Gold)

- Unicidad de la llave natural.
- Completitud: 24 horas por día y por entidad en métricas horarias del Sistema.
- No nulos en llave y valor del precio de bolsa.
- Rangos: precio de bolsa > 0 y < 5000 COP/kWh (alerta, no rechazo); volumen útil entre 0 y la capacidad del embalse.
- Frescura: la última fecha cargada no debe tener más de N días de retraso (N según el retraso medido).

## Carga incremental

- Diaria: se recarga una ventana móvil de los últimos **7 días** (el retraso máximo observado es de 3 días; el margen cubre reliquidaciones).
- Backfill: el mismo notebook o pipeline con parámetros `start_date` y `end_date`, partido en meses calendario.

## Pendiente

1. Revisar `ListadoRecursos` para ver si trae tecnología (hidráulica, térmica, solar, eólica), agente y tipo de despacho, o si hay que mapearlos.
2. Confirmar si XM publica reliquidaciones posteriores (comparar una misma fecha cargada en dos días distintos).
