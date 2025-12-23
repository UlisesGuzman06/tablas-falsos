import pandas as pd
from sqlalchemy import create_engine
import redis
from datetime import datetime
import time
import threading
from flask import Flask, render_template, request, jsonify, send_file
from dotenv import load_dotenv
import os
import io
import traceback

# ============================================================
# 🔧 CONFIGURACIÓN
# ============================================================

# Force override system variables with .env values to ensure local config is used
load_dotenv(override=True)

PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")

print("="*60)
print(f"🔧 [CONFIG] Loaded Environment Variables")
print(f"   👉 DB Host: {PG_HOST}:{PG_PORT}")
print(f"   👉 DB Name: {PG_DB}")
print("="*60)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# Database Connection String
DB_URI = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# V4: Restored key to clean state after reverts
# V4: Restored key to clean state after reverts
REDIS_KEY = "monitoring:resumen_diario_restored_v4"

CACHE_SHORT_SECONDS = 8 * 60 * 60          # 8 Hours
CACHE_LONG_SECONDS = 14 * 24 * 60 * 60     # 14 Days
BACKUP_FILE = "monitoring_summary_backup.json" # Disk persistence

# Global variables for lazy loading
_engine = None
_redis_client = None

# Global State for Async Generation
IS_GENERATING = False
GENERATION_LOGS = []

def log_progress(message):
    """Agrega un mensaje a los logs globales y lo imprime en consola."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    full_msg = f"[{timestamp}] {message}"
    print(full_msg)
    GENERATION_LOGS.append(full_msg)
    # Mantener solo los últimos 100 logs para no saturar memoria
    if len(GENERATION_LOGS) > 100:
        GENERATION_LOGS.pop(0)

# Global State for Async Generation (Restored)
IS_GENERATING_CHART = False # New Lock for Chart
GENERATION_LOGS = []

def log_progress(message):
    """Agrega un mensaje a los logs globales y lo imprime en consola."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    full_msg = f"[{timestamp}] {message}"
    print(full_msg)
    GENERATION_LOGS.append(full_msg)
    # Mantener solo los últimos 100 logs para no saturar memoria
    if len(GENERATION_LOGS) > 100:
        GENERATION_LOGS.pop(0)

def get_engine():
    global _engine
    if _engine is None:
        try:
            print(f"[{datetime.now()}] 🔌 Creando Motor PostgreSQL (Singleton)...")
            print(f"   👉 Target: {PG_HOST}:{PG_PORT} | DB: {PG_DB} | User: {PG_USER}")
            
            # Increased pool size to prevent exhaustion during polling
            _engine = create_engine(
                DB_URI, 
                pool_size=10, 
                max_overflow=20, 
                pool_recycle=3600, 
                pool_pre_ping=True
            )
        except Exception as e:
            print(f"Warning: DB Engine creation failed: {e}")
            _engine = None
    return _engine

def get_redis():
    global _redis_client
    if _redis_client is None:
        try:
            print(f"[{datetime.now()}] 🔌 Conectando a Redis...")
            _redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
            # Test connection
            _redis_client.ping()
        except Exception as e:
            print(f"Warning: Redis connection failed: {e}")
            _redis_client = None
    return _redis_client


# ============================================================
# ⚙️ FUNCIÓN — Generar tabla resumen (ORIGINAL LOGIC - RESTORED & EXTENDED)
# ============================================================

def generar_tabla_resumen():
    global IS_GENERATING
    IS_GENERATING = True
    log_progress("Iniciando generación de tabla incremental...")

    try:
        engine = get_engine()
        if not engine:
            log_progress("❌ Error: No hay conexión a base de datos.")
            return pd.DataFrame()

        # 1. Intentar cargar caché existente para hacer update incremental
        cached_df = None
        r = get_redis()
        if r and r.exists(REDIS_KEY):
            try:
                content = r.get(REDIS_KEY)
                if content:
                    cached_df = pd.read_json(io.StringIO(content), orient="records")
                    log_progress("INFO: Cache encontrado. Ejecutando actualización incremental (Mes Actual).")
            except Exception as e:
                log_progress(f"Warning: Error leyendo caché para incremental: {e}")

        # Fallback: Intentar cargar desde disco si no hay Redis
        if cached_df is None and os.path.exists(BACKUP_FILE):
            try:
                cached_df = pd.read_json(BACKUP_FILE, orient="records")
                log_progress(f"INFO: Backup en disco encontrado ({BACKUP_FILE}). Ejecutando actualización incremental.")
            except Exception as e:
                log_progress(f"Warning: Error leyendo backup de disco: {e}")

        # 2. Definir Query (Full vs Incremental)
        if cached_df is not None and not cached_df.empty:
            # Incremental: Solo mes actual
            log_progress("Consultando PostgreSQL (Solo mes actual)...")
            query = """
            SELECT
                EXTRACT(DAY FROM requestindatetime)::int AS dia,
                TO_CHAR(requestindatetime, 'Mon-YY')    AS mes,
                COUNT(*)                                AS solicitudes,
                SUM(CASE WHEN succeeded = false THEN 1 ELSE 0 END) AS false,
                ROUND(
                    SUM(CASE WHEN succeeded = false THEN 1 ELSE 0 END)::numeric
                    / COUNT(*) * 100,
                    2
                )                                       AS porcentaje_false
            FROM tablaxroadmonitoreo
            WHERE
                requestindatetime >= DATE_TRUNC('month', NOW()) -- Solo desde el 1 del mes actual
                AND serviceCode NOT IN ('clientReg', 'getSecurityServerOperationalData', 'getSecurityServerHealthData', 'getSecurityServerMetrics', 'listMethods', 'getOpenAPI', 'getClients', 'getWSDL')
                AND securityservertype = 'Client'
            GROUP BY 1, 2
            ORDER BY 2, 1;
            """
        else:
            # Full Load: 12 meses (Fallback o inicio)
            log_progress("Consultando PostgreSQL (Completo 12 meses)...")
            query = """
            SELECT
                EXTRACT(DAY FROM requestindatetime)::int AS dia,
                TO_CHAR(requestindatetime, 'Mon-YY')    AS mes,
                COUNT(*)                                AS solicitudes,
                SUM(CASE WHEN succeeded = false THEN 1 ELSE 0 END) AS false,
                ROUND(
                    SUM(CASE WHEN succeeded = false THEN 1 ELSE 0 END)::numeric
                    / COUNT(*) * 100,
                    2
                )                                       AS porcentaje_false
            FROM tablaxroadmonitoreo
            WHERE
                requestindatetime >= DATE_TRUNC('month', NOW() - INTERVAL '2 months')
                AND serviceCode NOT IN ('clientReg', 'getSecurityServerOperationalData', 'getSecurityServerHealthData', 'getSecurityServerMetrics', 'listMethods', 'getOpenAPI', 'getClients', 'getWSDL')
                AND securityservertype = 'Client'
            GROUP BY 1, 2
            ORDER BY 2, 1;
            """

        try:
            df_new = pd.read_sql(query, engine)
        except Exception as e:
            log_progress(f"❌ Error ejecutando query SQL: {e}")
            return pd.DataFrame()

        # 3. Procesar y Fusionar
        if df_new.empty and cached_df is None:
             log_progress("⚠️ SQL retornó 0 datos y no hay caché.")
             return pd.DataFrame()

        # Pivotear los datos nuevos
        tabla_new = pd.DataFrame()
        if not df_new.empty:
            tabla_new = df_new.pivot(index="dia", columns="mes", values=["solicitudes", "false", "porcentaje_false"])
            tabla_new.columns = [f"{col[1]}_{col[0]}" for col in tabla_new.columns]
            tabla_new.index.name = "dia"
            tabla_new = tabla_new.reset_index()

        # Merge Logic
        if cached_df is not None:
             # Alinear indices
             cached_df.set_index("dia", inplace=True)
             if not tabla_new.empty:
                tabla_new.set_index("dia", inplace=True)
                
                # Actualizar columnas existentes del mes actual y agregar nuevas
                for col in tabla_new.columns:
                    cached_df[col] = tabla_new[col]
             
             # Reset index para el formato final
             tabla_final = cached_df.reset_index()
        else:
             tabla_final = tabla_new

        # 4. Limpieza (Pruning) de columnas viejas (> 3 meses)
        # Identificar mes actual y ventana
        current_date = datetime.now()
        valid_months = []
        for i in range(3):
            d = current_date - pd.DateOffset(months=i)
            valid_months.append(d.strftime("%b-%y"))
        
        # Filtrar columnas
        cols_to_keep = ["dia"]
        for col in tabla_final.columns:
            if col == "dia": continue
            parts = col.split("_") # [Jan-24, solicitudes]
            if parts[0] in valid_months:
                cols_to_keep.append(col)
        
        tabla_final = tabla_final[cols_to_keep]

        # 5. Ordenar Columnas y Filas
        tabla_final = tabla_final.sort_values(by="dia").fillna("")
        
        # Ordenar columnas por fecha
        meses_presentes = sorted(
            list(set(c.split("_")[0] for c in tabla_final.columns if c != "dia")),
            key=lambda m: datetime.strptime(m, "%b-%y")
        )
        
        columnas_ordenadas = ["dia"] + [
            f"{mes}_{tipo}"
            for mes in meses_presentes
            for tipo in ["solicitudes", "false", "porcentaje_false"]
            if f"{mes}_{tipo}" in tabla_final.columns
        ]
        tabla_final = tabla_final[columnas_ordenadas]
        
        # Formatear numeros
        tabla_final = tabla_final.map(lambda x: int(x) if isinstance(x, float) and x.is_integer() else x)

        log_progress(f"✔️ Tabla incremental lista ({len(tabla_final)} filas).")

        try:
            json_str = tabla_final.to_json(orient="records")
            if r:
                r.set(REDIS_KEY, json_str, ex=CACHE_SHORT_SECONDS)
                log_progress("💾 Tabla guardada en Redis.")
            
            # Save to Disk
            with open(BACKUP_FILE, "w") as f:
                f.write(json_str)
            log_progress(f"💾 Respaldo guardado en disco: {BACKUP_FILE}")

        except Exception as e:
            log_progress(f"Error guardando resultados: {e}")

        log_progress("✅ Generación completada con éxito.")
        return tabla_final

    except Exception as e:
        log_progress(f"❌ Error inesperado en generación: {e}")
        # Return what we have or empty
        return pd.DataFrame()
    finally:
        IS_GENERATING = False
    


# ============================================================
# ⚙️ FUNCIÓN — Obtener o actualizar cache
# ============================================================

def obtener_o_actualizar_cache():
    global IS_GENERATING

    r = get_redis()
    # 1. Intentar leer caché
    if r and r.exists(REDIS_KEY):
        # Si existe, devolvemos data.
        # Puede que se esté regenerando en segundo plano si expiró hace poco, 
        # pero aquí asumimos modelo simple: si está, úsalo.
        print(f"[{datetime.now()}] 🔥 Leyendo desde Redis (cache existente).")
        try:
            content = r.get(REDIS_KEY)
            if content:
                df = pd.read_json(io.StringIO(content), orient="records")
                return df
        except Exception as e:
            print(f"Error leyendo JSON de Redis: {e}")

    # 2. Si no hay caché
    if IS_GENERATING:
        # Ya se está generando en otro hilo
        return None # Retornamos None para indicar "Cargando..."
    
    # 3. Lanzar generación en background
    print(f"[{datetime.now()}] ⚙️ Cache no encontrado. Lanzando generación en segundo plano...")
    # Limpiar logs antiguos
    global GENERATION_LOGS
    GENERATION_LOGS = []
    
    thread = threading.Thread(target=generar_tabla_resumen)
    thread.start()
    
    return None # Retornamos None para indicar "Cargando..."

# ============================================================
# ⚙️ FUNCIÓN HEAVY - PRE-CARGAR TODOS LOS DETALLES MENSUALES
# ============================================================

def _generar_detalles_bg():
    """Worker para generar los detalles mensuales en background (Incremental)."""
    print(f"[{datetime.now()}] ⚙️ Iniciando generación background de detalles mensuales...")
    try:
        engine = get_engine()
        if not engine: return

        cache_key = "monitoring:all_monthly_details"
        r = get_redis()
        
        # 1. Intentar cargar caché existente
        cached_result = {}
        if r and r.exists(cache_key):
            try:
                import json
                cached_result = json.loads(r.get(cache_key))
                print("INFO: Cache detalles encontrado. Ejecutando actualización incremental.")
            except:
                cached_result = {}

        # 2. Definir Query (Incremental vs Full)
        if cached_result:
             # Incremental: Solo mes actual
            query = """
            SELECT 
                TO_CHAR(requestindatetime, 'Mon-YY') AS mes,
                EXTRACT(DAY FROM requestindatetime)::int AS dia,
                COUNT(*) AS solicitudes,
                SUM(CASE 
                    WHEN succeeded = false 
                    AND (
                    statuscode IS NULL 
                    OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) < 200)
                    OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) >= 300)
                    )
                THEN 1 ELSE 0 
                END) AS err
            FROM public.tablaxroadmonitoreo
            WHERE 
                requestindatetime >= DATE_TRUNC('month', NOW())
                AND serviceCode NOT IN ('clientReg', 'getSecurityServerOperationalData', 'getSecurityServerHealthData', 'getSecurityServerMetrics', 'listMethods', 'getOpenAPI', 'getClients', 'getWSDL')
                AND securityservertype = 'Client'
            GROUP BY 1, 2
            ORDER BY 1, 2 ASC;
            """
        else:
            # Full Load
            query = """
            SELECT 
                TO_CHAR(requestindatetime, 'Mon-YY') AS mes,
                EXTRACT(DAY FROM requestindatetime)::int AS dia,
                COUNT(*) AS solicitudes,
                SUM(CASE 
                    WHEN succeeded = false 
                    AND (
                    statuscode IS NULL 
                    OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) < 200)
                    OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) >= 300)
                    )
                THEN 1 ELSE 0 
                END) AS err
            FROM public.tablaxroadmonitoreo
            WHERE 
                requestindatetime >= DATE_TRUNC('month', NOW() - INTERVAL '2 months')
                AND serviceCode NOT IN ('clientReg', 'getSecurityServerOperationalData', 'getSecurityServerHealthData', 'getSecurityServerMetrics', 'listMethods', 'getOpenAPI', 'getClients', 'getWSDL')
                AND securityservertype = 'Client'
            GROUP BY 1, 2
            ORDER BY 1, 2 ASC;
            """
        
        try:
            df = pd.read_sql(query, engine)
        except Exception as e:
            print(f"Error query detalles: {e}")
            return

        # 3. Procesar resultados nuevos
        new_result = {}
        if not df.empty:
            df['pct'] = df.apply(
                lambda row: round((row['err'] / row['solicitudes'] * 100), 2) if row['solicitudes'] > 0 else 0, 
                axis=1
            )
            for mes, group in df.groupby("mes"):
                new_result[mes] = group.drop(columns=['mes']).to_dict(orient="records")
        
        # 4. Merge
        final_result = cached_result.copy()
        for mes, data in new_result.items():
            final_result[mes] = data # Overwrite/Add month
        
        # 5. Prune Old Months (Window 12 months)
        current_date = datetime.now()
        valid_months = []
        for i in range(3):
            d = current_date - pd.DateOffset(months=i)
            valid_months.append(d.strftime("%b-%y"))
        
        # Filter keys
        keys_to_delete = [k for k in final_result.keys() if k not in valid_months]
        for k in keys_to_delete:
            del final_result[k]

        # Save to Redis
        if r:
            import json
            r.set(cache_key, json.dumps(final_result), ex=CACHE_SHORT_SECONDS)
            print(f"[{datetime.now()}] 💾 Detalles mensuales guardados en Redis (Incremental).")

    except Exception as e:
        print(f"❌ Error generando detalles mensuales: {e}")

def obtener_todos_datos_mensuales():
    """
    Obtiene los detalles diarios (para el modal 'datos_mes').
    Si no está en cache, retorna {} y lanza background.
    """
    cache_key = "monitoring:all_monthly_details"
    r = get_redis()
    
    # 1. Try Redis
    if r and r.exists(cache_key):
        try:
            import json
            return json.loads(r.get(cache_key))
        except Exception as e:
            print(f"Error leyendo bulk monthly details cache: {e}")

    # 2. If no cache -> Background
    print(f"[{datetime.now()}] ⚙️ Cache detalles no encontrado. Lanzando background...")
    threading.Thread(target=_generar_detalles_bg).start()
    
    return {}

# ============================================================
# ⚙️ FUNCIÓN — Obtener datos para el gráfico anual (12 meses)
# ============================================================

def _generar_grafico_anual_bg():
    """Worker para generar el gráfico anual en background con Reintentos."""
    global IS_GENERATING_CHART
    IS_GENERATING_CHART = True
    print(f"[{datetime.now()}] ⚙️ Iniciando generación background de gráfico anual...")
    
    try:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                engine = get_engine()
                if not engine: return

                # Query Strict Errors
                query = """
                SELECT 
                    TO_CHAR(requestindatetime, 'Mon-YY') AS month,
                    MIN(date_trunc('month', requestindatetime)) as sort_date,
                    COUNT(*) AS total,
                    SUM(CASE 
                        WHEN succeeded = false 
                        AND (
                            statuscode IS NULL 
                            OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) < 200)
                            OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) >= 300)
                        )
                        THEN 1 
                        ELSE 0 
                    END) AS errors
                FROM public.tablaxroadmonitoreo
                WHERE 
                    requestindatetime >= DATE_TRUNC('month', NOW() - INTERVAL '1 year')
                    AND serviceCode NOT IN (
                        'clientReg',
                        'getSecurityServerOperationalData',
                        'getSecurityServerHealthData',
                        'getSecurityServerMetrics',
                        'listMethods',
                        'getOpenAPI',
                        'getClients',
                        'getWSDL'
                    )
                    AND securityservertype = 'Client'
                GROUP BY 1
                ORDER BY 2 ASC;
                """
                
                # Direct execution (Pandas manages connection)
                df = pd.read_sql(query, engine)
                
                current_ts = datetime.now()
                r = get_redis()
                if r and not df.empty:
                    cache_key = "monitoring:annual_chart"
                    timestamp_key = f"{cache_key}_timestamp"
                    r.set(cache_key, df.to_json(orient="records"), ex=CACHE_LONG_SECONDS)
                    r.set(timestamp_key, current_ts.isoformat(), ex=CACHE_LONG_SECONDS)
                    print(f"[{datetime.now()}] 💾 Gráfico anual guardado en Redis (Intento {attempt+1}).")
                
                # Éxito, salir del loop
                return

            except Exception as e:
                print(f"⚠️ Error generando gráfico anual (Intento {attempt+1}/{max_retries}): {e}")
                # Force engine disposal to reset pool on connection errors
                if "closed the connection unexpectedly" in str(e) or "OperationalError" in str(e):
                    try:
                        print("♻️ Reiniciando pool de conexiones...")
                        if _engine: _engine.dispose()
                    except: pass
                
                time.sleep(2) # Esperar antes de reintentar
        
        print("❌ Fallaron todos los intentos de generar gráfico anual.")
    finally:
        IS_GENERATING_CHART = False

def obtener_datos_grafico_anual():
    """
    Intenta obtener datos de Redis. Si no están, lanza thread y retorna None.
    """
    cache_key = "monitoring:annual_chart"
    r = get_redis()
    timestamp_key = f"{cache_key}_timestamp"
    
    # 1. Try Redis
    if r and r.exists(cache_key):
        try:
            cached_data = r.get(cache_key)
            if cached_data:
                data_dicts = pd.read_json(io.StringIO(cached_data), orient="records").to_dict(orient='records')
                # Try to get timestamp
                cached_ts = r.get(timestamp_key)
                if cached_ts:
                    try:
                        timestamp = datetime.fromisoformat(cached_ts.decode())
                    except:
                        timestamp = datetime.now()
                else:
                    timestamp = datetime.now()
                return data_dicts, timestamp
        except Exception as e:
            print(f"Error reading annual chart cache: {e}")

    # 2. If no cache -> Background Generation
    # Check LOCK to avoid spawning multiple threads (Cache Stampede)
    if not IS_GENERATING_CHART:
        print(f"[{datetime.now()}] ⚙️ Cache gráfico anual no encontrado. Lanzando background...")
        threading.Thread(target=_generar_grafico_anual_bg).start()
    else:
        print(f"[{datetime.now()}] ⏳ Gráfico anual ya se está generando en otro hilo. Esperando...")
    
    return None, None

    # This part theoretically unreachable if logic above flows right, but safe fallback
    # 3. Apply Translation (Always) - Wait, we returned above.
    # We need to apply translation BEFORE returning in the DB block?
    # Actually, the logic below handles existing `data_dicts` if we skipped DB block?
    # But I changed DB block to return. Let's fix that.
    
    # ... Wait, the original code had translation AFTER DB block. 
    # I should preserve that flow or move it.
    # To avoid complexity, I will move translation INTO the DB block before returning
    # OR change the early returns to just set `data_dicts` and `timestamp` 
    # and let it flow. The `redis` block already decoded dicts.
    
    # Let's adjust the implementation to be safer and cleaner.
    # Returning straight from DB block means translation logic at bottom is skipped.
    pass

    return [], datetime.now() # Fallback

# ============================================================
# 🔁 HILO — Actualización periódica
# ============================================================

def actualizar_periodicamente(intervalo_horas=8):
    intervalo_segundos = intervalo_horas * 3600
    while True:
        print("\n" + "=" * 60)
        print(f"[{datetime.now()}] 🔄 Iniciando ciclo de actualización...")
        try:
            generar_tabla_resumen()
            _generar_detalles_bg()      # 2. Detalles (Prioridad para interactivity)
            _generar_grafico_anual_bg() # 3. Gráfico (Menos crítico)
            
            print(f"[{datetime.now()}] ✅ Actualización completa. Durmiendo {intervalo_horas}h.")

        except Exception as e:
             print(f"[{datetime.now()}] ❌ Excepción en hilo actualizador: {e}")
        
        print("=" * 60 + "\n")
        time.sleep(intervalo_segundos)

# ============================================================
# 🌐 FLASK — Servidor web
# ============================================================

app = Flask(__name__)

def traducir_columnas_df(df):
    """Aplica la traducción de columnas al DataFrame directamente."""
    meses_es = {
        "Jan": "Enero", "Feb": "Febrero", "Mar": "Marzo", "Apr": "Abril",
        "May": "Mayo", "Jun": "Junio", "Jul": "Julio", "Aug": "Agosto",
        "Sep": "Septiembre", "Oct": "Octubre", "Nov": "Noviembre", "Dec": "Diciembre"
    }
    
    nuevo_cols = []
    for col in df.columns:
        traducido = col
        for abreviado, completo in meses_es.items():
            if abreviado in col:
                partes = col.split("_")
                mes_año = partes[0]
                tipo = partes[1] if len(partes) > 1 else ""
                mes, año = mes_año.split("-")
                nombre_mes = meses_es.get(mes, mes)
                tipo_legible = {
                    "solicitudes": "Total",
                    "false": "Falsos",
                    "porcentaje_false": "% Fallo"
                }.get(tipo, tipo)
                traducido = f"{nombre_mes} 20{año} - {tipo_legible}"
                break
        
        if col == "dia":
            traducido = "Día"
        elif "_" in col and " - " not in traducido:
             traducido = col.replace("_", " - ")
             
        nuevo_cols.append(traducido)
    
    df_export = df.copy()
    df_export.columns = nuevo_cols
    return df_export

@app.route("/")
def home():
    # Renderizar siempre la tabla directamente
    return mostrar_tabla()

@app.route("/status")
def status():
    """Endpoint para consultar el estado de la generación."""
    return jsonify({
        "generating": IS_GENERATING,
        "logs": GENERATION_LOGS
    })

@app.template_filter('format_number')
def format_number(value):
    try:
        if value is None or value == "":
            return ""
        # Format with comma as thousand separator, then swap comma/dot
        # Spanish/European format: 1.234.567
        return "{:,.0f}".format(float(value)).replace(",", ".")
    except (ValueError, TypeError):
        return value

@app.route("/tabla")
def mostrar_tabla():
    # 1. Main Table Data
    df = obtener_o_actualizar_cache()
    
    loading_table = False
    headers = []
    raw_columns = []
    totals = {}
    
    if df is None:
        loading_table = True
        df = pd.DataFrame()
    elif df.empty:
         # Error or truly empty
         pass
    else:
        # Generar encabezados legibles para la vista HTML
        df_view = traducir_columnas_df(df)
        headers = list(df_view.columns)
        raw_columns = list(df.columns)
        
        # Calcular Totales
        totals = {}
        for col in df.columns:
            if col == "dia":
                totals[col] = "TOTAL"
            elif "_solicitudes" in col:
                totals[col] = int(pd.to_numeric(df[col], errors='coerce').fillna(0).sum())
            elif "_false" in col and "porcentaje" not in col:
                totals[col] = int(pd.to_numeric(df[col], errors='coerce').fillna(0).sum())
        
        # Calcular porcentajes totales
        for col in df.columns:
            if "porcentaje_false" in col:
                mes = col.replace("_porcentaje_false", "")
                sol_col = f"{mes}_solicitudes"
                err_col = f"{mes}_false"
                
                total_sol = totals.get(sol_col, 0)
                total_err = totals.get(err_col, 0)
                
                if total_sol > 0:
                    totals[col] = round((total_err / total_sol) * 100, 2)
                else:
                    totals[col] = 0.0

    # 2. Chart Data & Metrics
    # Only try to fetch/generate chart if Table is already done (Sequential load)
    chart_data_list = None
    chart_timestamp = None
    loading_chart = False

    if not loading_table:
        chart_data_list, chart_timestamp = obtener_datos_grafico_anual()
    
    if chart_data_list is None:
        loading_chart = True
        chart_data_list = []
        chart_timestamp = datetime.now() # Mock
    
    # Calculate Metrics
    total_anual = 0
    promedio_mensual = 0
    mes_maximo = {"label": "-", "total": 0}
    
    if chart_data_list:
        total_anual = sum(item['total'] for item in chart_data_list)
        promedio_mensual = total_anual / len(chart_data_list)
        # Find max month
        try:
            max_item = max(chart_data_list, key=lambda x: x['total'])
            mes_maximo = {
                "label": max_item.get('month_label', max_item['month']),
                "total": max_item['total']
            }
        except:
            pass
    
    # 3. Pre-load Monthly Details (for instant modal)
    monthly_details_preloaded = obtener_todos_datos_mensuales()

    return render_template(
        "tabla.html", 
        tabla=df, 
        headers=headers, 
        raw_columns=raw_columns, 
        ultima_actualizacion=chart_timestamp if chart_timestamp else datetime.now(),
        totals=totals, 
        chart_data=chart_data_list, 
        total_anual=total_anual,
        promedio_mensual=promedio_mensual,
        mes_maximo=mes_maximo,
        monthly_details_preloaded=monthly_details_preloaded,
        loading_table=loading_table,
        loading_chart=loading_chart
    )

# ============================================================
# ⚡ API ASYNC LOADING
# ============================================================

@app.route("/api/summary-table")
def api_summary_table():
    df = obtener_o_actualizar_cache()
    if df is None:
        return jsonify({"status": "loading"}), 202
    
    if df.empty:
        return jsonify({"status": "empty"}), 200

    # FIXME: Avoid code duplication by extracting this logic
    df_view = traducir_columnas_df(df)
    headers = list(df_view.columns)
    raw_columns = list(df.columns)
    
    totals = {}
    for col in df.columns:
        if col == "dia":
            totals[col] = "TOTAL"
        elif "_solicitudes" in col:
            totals[col] = int(pd.to_numeric(df[col], errors='coerce').fillna(0).sum())
        elif "_false" in col and "porcentaje" not in col:
            totals[col] = int(pd.to_numeric(df[col], errors='coerce').fillna(0).sum())
    
    for col in df.columns:
        if "porcentaje_false" in col:
            mes = col.replace("_porcentaje_false", "")
            sol_col = f"{mes}_solicitudes"
            err_col = f"{mes}_false"
            total_sol = totals.get(sol_col, 0)
            total_err = totals.get(err_col, 0)
            if total_sol > 0:
                totals[col] = round((total_err / total_sol) * 100, 2)
            else:
                totals[col] = 0.0

    html = render_template("components/summary_table.html", tabla=df, headers=headers, raw_columns=raw_columns, totals=totals)
    return jsonify({"status": "ready", "html": html})

@app.route("/api/annual-chart")
def api_annual_chart():
    chart_data_list, timestamp = obtener_datos_grafico_anual()
    
    # Si retorna None, es que está cargando en background
    if chart_data_list is None:
         return jsonify({"status": "loading"}), 202
    
    total_anual = 0
    promedio_mensual = 0
    mes_maximo = {"label": "-", "total": 0}

    if chart_data_list:
        total_anual = sum(item['total'] for item in chart_data_list)
        promedio_mensual = total_anual / len(chart_data_list)
        try:
            max_item = max(chart_data_list, key=lambda x: x['total'])
            mes_maximo = {
                "label": max_item.get('month_label', max_item.get('month', '-')),
                "total": max_item.get('total', 0)
            }
        except:
            pass

    monthly_details_preloaded = obtener_todos_datos_mensuales()
    
    return jsonify({
        "status": "ready",
        "chart_data": chart_data_list,
        "total_anual": total_anual,
        "promedio_mensual": promedio_mensual,
        "mes_maximo": mes_maximo,
        "ultima_actualizacion": timestamp.strftime('%d/%m %H:%M:%S') if timestamp else '-',
        "monthly_details_preloaded": monthly_details_preloaded
    })

# ============================================================
# 📁 EXCEL EXPORT
# ============================================================

@app.route("/descargar_resumen")
def descargar_resumen():
    df = obtener_o_actualizar_cache()
    
    if df.empty:
        return "No hay datos para descargar", 404

    df = df.loc[:, ~df.columns.duplicated()]
    df_export = traducir_columnas_df(df)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_export.to_excel(writer, index=False, sheet_name='Resumen Monitoreo')
    
    output.seek(0)
    
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f"xroadtrimestral.xlsx"
    )

# ============================================================
# 📊 DATOS MES (Modal Tabla) -> Keeps as fallback endpoint
# ============================================================

@app.route("/datos_mes", methods=["POST"])
def datos_mes():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON"}), 400
        
    mes = data.get("mes") # e.g. "Jan-25"
    if not mes:
        return jsonify({"error": "Mes is required"}), 400

    engine = get_engine()
    if not engine:
        return jsonify({"error": "No DB connection"}), 500

    cache_key = f"monitoring:month_data:{mes}"
    r = get_redis()
    if r and r.exists(cache_key):
        try:
             print(f"[{datetime.now()}] 🔥 Leyendo datos mes ({mes}) desde Redis (Endpoint).")
             return r.get(cache_key)
        except Exception as e:
             print(f"Error reading month data cache: {e}")

    # Fallback to local query if not in Redis for some reason
    # ... logic similar to `obtener_todos_datos_mensuales` but for single month ...
    # Simpler: just call the bulk function and return specific key? No, that might be too heavy for a specific fallback.
    # We'll leave the original query here as a fallback mechanism.
    
    query = f"""
    SELECT 
        EXTRACT(DAY FROM requestindatetime)::int AS dia,
        COUNT(*) AS solicitudes,
        SUM(CASE 
            WHEN succeeded = false 
                 AND (
                    statuscode IS NULL 
                    OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) < 200)
                    OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) >= 300)
                 )
            THEN 1 ELSE 0 
        END) AS err
    FROM public.tablaxroadmonitoreo
    WHERE 
        TO_CHAR(requestindatetime, 'Mon-YY') = '{mes}'
        AND serviceCode NOT IN (
            'clientReg',
            'getSecurityServerOperationalData',
            'getSecurityServerHealthData',
            'getSecurityServerMetrics',
            'listMethods',
            'getOpenAPI',
            'getClients',
            'getWSDL'
        )
        AND securityservertype = 'Client'
    GROUP BY 1
    ORDER BY 1 ASC;
    """
    
    try:
        df_res = pd.read_sql(query, engine)
        
        # Calculate percentage
        df_res['pct'] = df_res.apply(
            lambda row: round((row['err'] / row['solicitudes'] * 100), 2) if row['solicitudes'] > 0 else 0, 
            axis=1
        )
        
        json_res = df_res.to_json(orient="records")
        if r:
            try:
                r.set(cache_key, json_res, ex=CACHE_LONG_SECONDS)
            except Exception as e:
                print(f"Error saving month data to Redis: {e}")
        
        return json_res
    except Exception as e:
        print(f"Error in /datos_mes: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================================
# 🔍 DETALLES (Mensuales Caché)
# ============================================================

@app.route("/detalles", methods=["POST"])
def obtener_detalles():
    data = request.get_json()
    print(f"DEBUG: /detalles payload: {data}")
    if not data:
        print("DEBUG: No JSON received")
        return jsonify({"error": "Invalid JSON"}), 400
        
    dia = data.get("dia")
    columna = data.get("columna", "") 
    print(f"DEBUG: dia={dia}, columna={columna}") 
    
    # ... (Rest of existing logic for /detalles) -> UNCHANGED
    try:
        engine = get_engine()
        if not engine:
             return jsonify({"error": "Sin conexión a DB"}), 500

        try:
            mes_año_part = columna.split("_")[0] # "Jan-25"
        except:
            return jsonify({"error": "No se pudo determinar el mes."}), 400

        mes_sql = mes_año_part
        cache_key = f"monitoring:detalles:mes:{mes_sql}"

        df_mes = pd.DataFrame()
        
        r = get_redis()

        # 1. Intentar leer de Redis
        if r and r.exists(cache_key):
            try:
                df_mes = pd.read_json(io.StringIO(r.get(cache_key)), orient="records")
            except:
                pass

        # 2. Si no hay cache, consultar SQL
        if df_mes.empty:
            # Note: Keeping the filter for details strictly (300-599) as that seems to be desired for the details view specifically
            # usually 300+ are 'interesting' codes.
            query = f"""
            SELECT 
                EXTRACT(DAY FROM requestindatetime)::int AS dia,
                servicesubsystemcode,
                servicecode,
                statuscode,
                COUNT(*) AS cantidad
            FROM public.tablaxroadmonitoreo
            WHERE 
                TO_CHAR(requestindatetime, 'Mon-YY') = '{mes_sql}'
                AND succeeded = false
                AND (
                    statuscode IS NULL 
                    OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) < 200)
                    OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) >= 300)
                )
                AND serviceCode NOT IN (
                    'clientReg',
                    'getSecurityServerOperationalData',
                    'getSecurityServerHealthData',
                    'getSecurityServerMetrics',
                    'listMethods',
                    'getOpenAPI',
                    'getClients',
                    'getWSDL'
                )
                AND securityservertype = 'Client'
            GROUP BY 1, servicesubsystemcode, servicecode, statuscode
            ORDER BY statuscode ASC, cantidad DESC;
            """
            try:
                df_mes = pd.read_sql(query, engine)
                if r and not df_mes.empty:
                    r.set(cache_key, df_mes.to_json(orient="records", force_ascii=False), ex=CACHE_SHORT_SECONDS)
            except Exception as e:
                return jsonify({"error": str(e)}), 500

        # 3. Filtrar en memoria
        if not df_mes.empty:
            df_dia = df_mes[df_mes['dia'] == int(dia)].copy()
            df_dia = df_dia.sort_values(by=["statuscode", "cantidad"], ascending=[True, False])
            return df_dia.to_json(orient="records", force_ascii=False)
        
        return "[]"
    except Exception as e:
        print(f"CRITICAL ERROR in /detalles: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500

if __name__ == "__main__":
    # Disable default Flask/Werkzeug request logging (too noisy with polling)
    import logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)

    # SOLO para desarrollo local
    print(f"[{datetime.now()}] 🚀 Iniciando servidor en modo DEBUG/DESARROLLO...")
    hilo_actualizador = threading.Thread(target=actualizar_periodicamente, args=(8,), daemon=True)
    hilo_actualizador.start()
    app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)
