# Pipeline diario `pl_diario_energia`

Orquesta la carga completa en Data Factory de Fabric (workspace `energia-dev`):

```
nb_bronze_ingesta_xm ──► nb_silver_xm ──► espera 2 min ──► gold.usp_cargar_gold
```

Cada flecha es "si se completa correctamente": si un paso falla, los siguientes no corren y la corrida queda
en rojo en **Supervisión** señalando la actividad que falló.

- **Incremental por defecto:** sin parámetros, Bronze y Silver recargan los últimos 7 días y Gold los últimos 10.
- **Backfill:** se lanza a mano con `start_date` (y opcionalmente `end_date`) en formato `AAAA-MM-DD`.
- **Reintentos:** cada actividad reintenta 2 veces con 5 minutos de espera (la API de XM a veces responde lento).
- **Espera de 2 minutos:** Gold lee Silver por el punto de conexión SQL del Lakehouse, que tarda un poco en ver
  los datos nuevos de Delta. Mejora futura: llamar a la API `refreshMetadata` del punto SQL en lugar de esperar.
- **Aviso de falla:** la corrida queda en rojo en *Supervisión*. El correo se conecta el Día 4 con Activator (eventos de trabajos de Fabric), porque el tenant de prueba
  no tiene buzón de Outlook para la actividad de Office 365.

## Paso a paso

### 1. Crear el pipeline
`energia-dev` → **Nuevo elemento** → **Canalización** (Data pipeline) → nombre `pl_diario_energia`.

### 2. Parámetros del pipeline
El lienzo aparece al agregar la primera actividad (la pantalla de inicio no lo muestra): agrega primero el
cuaderno de Bronze, luego clic en un espacio vacío del lienzo → pestaña **Parámetros** → **+ Nuevo** dos veces:

| Nombre | Tipo | Valor predeterminado |
| --- | --- | --- |
| `start_date` | String | *(vacío)* |
| `end_date` | String | *(vacío)* |

### 3. Actividad `Bronze` (Cuaderno / Notebook)
**Actividades** → **Cuaderno**. Nombre: `Bronze`.

- Pestaña **General**: Tiempo de espera `0.02:00:00`, Reintentar `2`, Intervalo de reintento `300`.
- Pestaña **Configuración**: Área de trabajo `energia-dev`, Cuaderno `nb_bronze_ingesta_xm`.
- **Parámetros base**: Fabric importa solo los parámetros del cuaderno. En *Valor* de estos dos usa
  **Agregar contenido dinámico** (el resto se deja con su valor por defecto):

| Nombre | Valor |
| --- | --- |
| `start_date` | `@pipeline().parameters.start_date` |
| `end_date` | `@pipeline().parameters.end_date` |

### 4. Actividad `Silver` (Cuaderno)
Igual que Bronze, con nombre `Silver` y cuaderno `nb_silver_xm` (mismos parámetros base y reintentos).
Conecta **Bronze → Silver** arrastrando la flecha verde (*Si se completa correctamente*) de Bronze a Silver.

### 5. Actividad `Esperar SQL` (Esperar / Wait)
**Actividades** → **…** (al final de la barra) → **Esperar**. Nombre `Esperar SQL`, tiempo de espera `120` segundos.
Conecta **Silver → Esperar SQL** con la flecha verde.

### 6. Actividad `Gold` (Script)
**Actividades** → **Script**. Nombre `Gold`.

- Pestaña **General**: Tiempo de espera `0.01:00:00`, Reintentar `1`, Intervalo `300`.
- Pestaña **Configuración**: Conexión → el Warehouse `wh_energia`. Tipo de script **Consulta** (Query).
  Conexión: **Examinar todo** → catálogo de OneLake → `wh_energia`.
  En el cuadro del script, **Agregar contenido dinámico** y pega (en una sola línea):

```
@concat('EXEC gold.usp_cargar_gold @inicio = ', if(empty(pipeline().parameters.start_date), 'NULL', concat('''', pipeline().parameters.start_date, '''')), ', @fin = ', if(empty(pipeline().parameters.end_date), 'NULL', concat('''', pipeline().parameters.end_date, '''')), ';')
```

Sin parámetros eso arma `EXEC gold.usp_cargar_gold @inicio = NULL, @fin = NULL;` (incremental de 10 días).
Conecta **Esperar SQL → Gold** con la flecha verde.

### 7. Probar
**Validar** → **Guardar** → **Ejecutar** (sin parámetros). Primera corrida, 2026-10-19: todo correcto en ~6 min
(Bronze 1 min 18 s, Silver 2 min 39 s, espera 2 min, Gold 46 s). El panel *Salida* deja de refrescarse solo a
los 5 minutos: usar el botón de actualizar junto al Id. de ejecución.

### 8. Programar
**Programar** → Activado → Repetir **Diario** → hora **07:00** → zona horaria **(UTC-05:00) Bogotá, Lima, Quito**.
XM publica con 1 a 3 días de retraso, así que la hora exacta no es crítica; la ventana de 7 días cubre los atrasos.

### 9. Versionar
**Control de código fuente** → marcar `pl_diario_energia` → **Confirmar**. Luego `git pull` en WSL.
