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
import json
from zoneinfo import ZoneInfo

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

print("=" * 60)
print(f"🔧 [CONFIG] Loaded Environment Variables")
print(f"   👉 DB Host: {PG_HOST}:{PG_PORT}")
print(f"   👉 DB Name: {PG_DB}")
print("=" * 60)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# Database Connection String
DB_URI = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# Tabla catálogo de X-Road (la tuya)
CATALOG_TABLE = "public.xroadmiembros"

# V4: Restored key to clean state after reverts
REDIS_KEY = "monitoring:resumen_diario_restored_v5"

CACHE_SHORT_SECONDS = 1 * 60 * 60          # 1 Hour
CACHE_LONG_SECONDS = 3 * 24 * 60 * 60      # 3 Days
BACKUP_FILE = "monitoring_summary_backup.json"  # Disk persistence

# Global variables for lazy loading
_engine = None
_redis_client = None

# Global State for Async Generation
IS_GENERATING = False
IS_GENERATING_CHART = False  # New Lock for Chart
IS_GENERATING_DETAILS = False # Lock for Bulk Details
GENERATING_DETAILS_MONTHS = {}  # Track active details generation per month {month_key: bool}
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
            _redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                decode_responses=True  # IMPORTANTE: devuelve strings
            )
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
                SUM(CASE 
                    WHEN succeeded = false 
                    AND (statuscode IS NULL OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) > 499))
                    THEN 1 ELSE 0 END) AS false,
                ROUND(
                    SUM(CASE 
                        WHEN succeeded = false 
                        AND (statuscode IS NULL OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) > 499))
                        THEN 1 ELSE 0 END)::numeric
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
            # Full Load: fallback/inicio
            log_progress("Consultando PostgreSQL (Completo 12 meses)...")
            query = """
            SELECT
                EXTRACT(DAY FROM requestindatetime)::int AS dia,
                TO_CHAR(requestindatetime, 'Mon-YY')    AS mes,
                COUNT(*)                                AS solicitudes,
                SUM(CASE 
                    WHEN succeeded = false 
                    AND (statuscode IS NULL OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) > 499))
                    THEN 1 ELSE 0 END) AS false,
                ROUND(
                    SUM(CASE 
                        WHEN succeeded = false 
                        AND (statuscode IS NULL OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) > 499))
                        THEN 1 ELSE 0 END)::numeric
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
            cached_df.set_index("dia", inplace=True)
            if not tabla_new.empty:
                tabla_new.set_index("dia", inplace=True)
                for col in tabla_new.columns:
                    cached_df[col] = tabla_new[col]
            tabla_final = cached_df.reset_index()
        else:
            tabla_final = tabla_new

        # 4. Limpieza (Pruning) de columnas viejas (> 3 meses)
        current_date = datetime.now()
        valid_months = []
        for i in range(3):
            d = current_date - pd.DateOffset(months=i)
            valid_months.append(d.strftime("%b-%y"))

        cols_to_keep = ["dia"]
        for col in tabla_final.columns:
            if col == "dia":
                continue
            parts = col.split("_")
            if parts[0] in valid_months:
                cols_to_keep.append(col)

        tabla_final = tabla_final[cols_to_keep]

        # 5. Ordenar Columnas y Filas
        tabla_final = tabla_final.sort_values(by="dia").fillna("")

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

            with open(BACKUP_FILE, "w") as f:
                f.write(json_str)
            log_progress(f"💾 Respaldo guardado en disco: {BACKUP_FILE}")

        except Exception as e:
            log_progress(f"Error guardando resultados: {e}")

        log_progress("✅ Generación completada con éxito.")
        return tabla_final

    except Exception as e:
        log_progress(f"❌ Error inesperado en generación: {e}")
        return pd.DataFrame()
    finally:
        IS_GENERATING = False


# ============================================================
# ⚙️ FUNCIÓN — Obtener o actualizar cache
# ============================================================

def obtener_o_actualizar_cache():
    global IS_GENERATING

    r = get_redis()
    if r and r.exists(REDIS_KEY):
        print(f"[{datetime.now()}] 🔥 Leyendo desde Redis (cache existente).")
        try:
            content = r.get(REDIS_KEY)
            if content:
                df = pd.read_json(io.StringIO(content), orient="records")
                return df
        except Exception as e:
            print(f"Error leyendo JSON de Redis: {e}")

    if IS_GENERATING:
        return None

    print(f"[{datetime.now()}] ⚙️ Cache no encontrado. Lanzando generación en segundo plano...")
    global GENERATION_LOGS
    GENERATION_LOGS = []

    thread = threading.Thread(target=generar_tabla_resumen)
    thread.start()

    return None


# ============================================================
# ⚙️ FUNCIÓN HEAVY - PRE-CARGAR TODOS LOS DETALLES MENSUALES
# ============================================================

def _generar_detalles_bg():
    global IS_GENERATING_DETAILS
    IS_GENERATING_DETAILS = True
    print(f"[{datetime.now()}] ⚙️ Iniciando generación background de detalles mensuales...")
    try:
        engine = get_engine()
        if not engine:
            return

        cache_key = "monitoring:all_monthly_details"
        r = get_redis()

        cached_result = {}
        if r and r.exists(cache_key):
            try:
                import json
                cached_result = json.loads(r.get(cache_key))
                print("INFO: Cache detalles encontrado. Ejecutando actualización incremental.")
            except:
                cached_result = {}

        if cached_result:
            query = """
            SELECT 
                TO_CHAR(requestindatetime, 'Mon-YY') AS mes,
                EXTRACT(DAY FROM requestindatetime)::int AS dia,
                COUNT(*) AS solicitudes,
                SUM(CASE 
                    WHEN succeeded = false 
                    AND (
                    statuscode IS NULL 
                    OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) > 499)
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
            query = """
            SELECT 
                TO_CHAR(requestindatetime, 'Mon-YY') AS mes,
                EXTRACT(DAY FROM requestindatetime)::int AS dia,
                COUNT(*) AS solicitudes,
                SUM(CASE 
                    WHEN succeeded = false 
                    AND (
                    statuscode IS NULL 
                    OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) > 499)
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

        new_result = {}
        if not df.empty:
            df["pct"] = df.apply(
                lambda row: round((row["err"] / row["solicitudes"] * 100), 2) if row["solicitudes"] > 0 else 0,
                axis=1
            )
            for mes, group in df.groupby("mes"):
                new_result[mes] = group.drop(columns=["mes"]).to_dict(orient="records")

        final_result = cached_result.copy()
        for mes, data in new_result.items():
            final_result[mes] = data

        current_date = datetime.now()
        valid_months = []
        for i in range(3):
            d = current_date - pd.DateOffset(months=i)
            valid_months.append(d.strftime("%b-%y"))

        keys_to_delete = [k for k in final_result.keys() if k not in valid_months]
        for k in keys_to_delete:
            del final_result[k]

        if r:
            import json
            r.set(cache_key, json.dumps(final_result), ex=CACHE_SHORT_SECONDS)
            print(f"[{datetime.now()}] 💾 Detalles mensuales guardados en Redis (Incremental).")

    except Exception as e:
        print(f"❌ Error generando detalles mensuales: {e}")
    finally:
        IS_GENERATING_DETAILS = False

def obtener_todos_datos_mensuales():
    cache_key = "monitoring:all_monthly_details"
    r = get_redis()

    if r and r.exists(cache_key):
        try:
            import json
            return json.loads(r.get(cache_key))
        except Exception as e:
            print(f"Error leyendo bulk monthly details cache: {e}")

    if IS_GENERATING_DETAILS:
        return {}

    print(f"[{datetime.now()}] ⚙️ Cache detalles no encontrado. Lanzando background...")
    threading.Thread(target=_generar_detalles_bg).start()
    return {}


# ============================================================
# ⚙️ FUNCIÓN — Obtener datos para el gráfico anual (12 meses)
# ============================================================

def _generar_grafico_anual_bg():
    global IS_GENERATING_CHART
    IS_GENERATING_CHART = True
    print(f"[{datetime.now()}] ⚙️ Iniciando generación background de gráfico anual...")

    try:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                engine = get_engine()
                if not engine:
                    return

                query = """
                SELECT 
                    TO_CHAR(requestindatetime, 'Mon-YY') AS month,
                    MIN(date_trunc('month', requestindatetime)) as sort_date,
                    COUNT(*) AS total,
                    SUM(CASE 
                        WHEN succeeded = false 
                        AND (
                            statuscode IS NULL 
                            OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) > 499)
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

                df = pd.read_sql(query, engine)

                current_ts = datetime.now()
                r = get_redis()
                if r and not df.empty:
                    cache_key = "monitoring:annual_chart"
                    timestamp_key = f"{cache_key}_timestamp"
                    r.set(cache_key, df.to_json(orient="records"), ex=CACHE_LONG_SECONDS)
                    r.set(timestamp_key, current_ts.isoformat(), ex=CACHE_LONG_SECONDS)
                    print(f"[{datetime.now()}] 💾 Gráfico anual guardado en Redis (Intento {attempt+1}).")

                return

            except Exception as e:
                print(f"⚠️ Error generando gráfico anual (Intento {attempt+1}/{max_retries}): {e}")
                if "closed the connection unexpectedly" in str(e) or "OperationalError" in str(e):
                    try:
                        print("♻️ Reiniciando pool de conexiones...")
                        if _engine:
                            _engine.dispose()
                    except:
                        pass
                time.sleep(2)

        print("❌ Fallaron todos los intentos de generar gráfico anual.")
    finally:
        IS_GENERATING_CHART = False

def obtener_datos_grafico_anual():
    """
    Intenta obtener datos de Redis. Si no están, lanza thread y retorna (None, None).
    """
    cache_key = "monitoring:annual_chart"
    timestamp_key = f"{cache_key}_timestamp"
    r = get_redis()

    if r and r.exists(cache_key):
        try:
            cached_data = r.get(cache_key)
            if cached_data:
                data_dicts = pd.read_json(io.StringIO(cached_data), orient="records").to_dict(orient="records")

                cached_ts = r.get(timestamp_key)  # str (decode_responses=True)
                if cached_ts:
                    try:
                        timestamp = datetime.fromisoformat(cached_ts)
                    except:
                        timestamp = datetime.now()
                else:
                    timestamp = datetime.now()

                # Translate months
                meses_es = {
                    "Jan": "Ene", "Feb": "Feb", "Mar": "Mar", "Apr": "Abr",
                    "May": "May", "Jun": "Jun", "Jul": "Jul", "Aug": "Ago",
                    "Sep": "Sep", "Oct": "Oct", "Nov": "Nov", "Dec": "Dic"
                }
                for item in data_dicts:
                     parts = item.get("month", "").split("-")
                     if len(parts) == 2:
                         en_mon = parts[0]
                         year = parts[1]
                         es_mon = meses_es.get(en_mon, en_mon)
                         # Set label
                         item["month_label"] = f"{es_mon}-{year}"
                         # Update month field too if desired, but label is safer for UI
                         # item["month"] = f"{es_mon}-{year}" # Optional: keep original for logic?
                         # Keeping original month field for filteringlogic (if it depends on English names)
                         # But wait, app.py filtering uses "25" in month string, so that's fine.
                         # User said "dice jan-25", so we must return something with Spanish.
                         # If I update item["month"], I must ensure other logic (filtering) still works.
                         # Filtering uses: if "25" in item.get("month", "") -> works with "Jan-25" or "Ene-25"
                         # But let's check if anything relies on "Jan".
                         # Database query returns 'Mon-YY'.
                         # Safest is to set 'month_label' and use that in UI.
                         # But the UI might strictly use 'month'.
                         # Let's check tabla.html Chart JS.
                         # It uses item.month usually.
                         # Let's override "month" as well to be sure it appears in chart x-axis.
                         item["month"] = f"{es_mon}-{year}"

                return data_dicts, timestamp
        except Exception as e:
            print(f"Error reading annual chart cache: {e}")

    if not IS_GENERATING_CHART:
        print(f"[{datetime.now()}] ⚙️ Cache gráfico anual no encontrado. Lanzando background...")
        threading.Thread(target=_generar_grafico_anual_bg).start()
    
    # Silently ignored if already generating
    
    return None, None


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
            _generar_detalles_bg()
            _generar_grafico_anual_bg()
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
    return mostrar_tabla()

@app.route("/status")
def status():
    return jsonify({
        "generating": IS_GENERATING,
        "logs": GENERATION_LOGS
    })

@app.template_filter('format_number')
def format_number(value):
    try:
        if value is None or value == "":
            return ""
        return "{:,.0f}".format(float(value)).replace(",", ".")
    except (ValueError, TypeError):
        return value


# ============================================================
# ✅ NUEVOS ENDPOINTS XROAD (members / subsystems)
# ============================================================




@app.route("/api/kpi/unified", methods=["GET"])
def kpi_unified():
    """
    Unified KPI endpoint returning:
    1. Previous month transactions count.
    2. Total members count.
    3. Total subsystems count.
    """
    engine = get_engine()
    if not engine:
        return jsonify({"error": "No DB connection"}), 500

    r = get_redis()
    cache_key = "monitoring:kpi:unified"
    
    if r and r.exists(cache_key):
        try:
            cached = r.get(cache_key)
            if cached:
                return cached, 200, {"Content-Type": "application/json"}
        except Exception as e:
            print(f"Error reading unified KPI cache: {e}")

    try:
        # 1. Transactions Previous Month
        query_trans = """
            SELECT COUNT(*) AS total
            FROM public.tablaxroadmonitoreo
            WHERE
                requestindatetime >= DATE_TRUNC('month', NOW()) - INTERVAL '1 month'
                AND requestindatetime <  DATE_TRUNC('month', NOW())
                AND serviceCode NOT IN (
                    'clientReg', 'getSecurityServerOperationalData', 'getSecurityServerHealthData', 
                    'getSecurityServerMetrics', 'listMethods', 'getOpenAPI', 'getClients', 'getWSDL'
                )
                AND securityservertype = 'Client';
        """
        df_trans = pd.read_sql(query_trans, engine)
        trans_count = int(df_trans.iloc[0]["total"]) if not df_trans.empty else 0

        # 2. Members Count
        query_members = f"""
            SELECT COUNT(*) AS total
            FROM (
                SELECT DISTINCT xroadinstance, memberclass, membercode
                FROM {CATALOG_TABLE}
                WHERE objecttype = 'MEMBER'
            ) t;
        """
        df_members = pd.read_sql(query_members, engine)
        members_count = int(df_members.iloc[0]["total"]) if not df_members.empty else 0

        # 3. Subsystems Count
        query_subsystems = f"""
            SELECT COUNT(*) AS total
            FROM (
                SELECT DISTINCT xroadinstance, memberclass, membercode, subsystemcode
                FROM {CATALOG_TABLE}
                WHERE objecttype = 'SUBSYSTEM'
            ) t;
        """
        df_subsystems = pd.read_sql(query_subsystems, engine)
        subsystems_count = int(df_subsystems.iloc[0]["total"]) if not df_subsystems.empty else 0

        # Name of previous month
        today = datetime.now()
        first_day_current_month = today.replace(day=1)
        last_month_date = first_day_current_month - pd.DateOffset(days=1)
        meses_es = {
            1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
            5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
            9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
        }
        mes_nombre = meses_es[last_month_date.month]

        payload = {
            "previous_month": {
                "month_name": mes_nombre,
                "year": last_month_date.year,
                "transactions": trans_count
            },
            "members_count": members_count,
            "subsystems_count": subsystems_count,
            "generated_at": datetime.now().isoformat()
        }

        json_str = json.dumps(payload, ensure_ascii=False)

        if r:
            try:
                r.set(cache_key, json_str, ex=10 * 60) # 10 min cache
            except Exception as e:
                print(f"Error saving unified KPI cache: {e}")
        
        return json_str, 200, {"Content-Type": "application/json"}

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": "kpi_unified_failed", "detail": str(e)}), 500


# ============================================================
# Resto de tu app (tal cual lo tenías)
# ============================================================

@app.route("/tabla")
def mostrar_tabla():
    df = obtener_o_actualizar_cache()

    loading_table = False
    headers = []
    raw_columns = []
    totals = {}

    if df is None:
        loading_table = True
        df = pd.DataFrame()
    elif df.empty:
        pass
    else:
        df_view = traducir_columnas_df(df)
        headers = list(df_view.columns)
        raw_columns = list(df.columns)

        totals = {}
        for col in df.columns:
            if col == "dia":
                totals[col] = "TOTAL"
            elif "_solicitudes" in col:
                totals[col] = int(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())
            elif "_false" in col and "porcentaje" not in col:
                totals[col] = int(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())

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

    chart_data_list = None
    chart_timestamp = None
    loading_chart = False

    if not loading_table:
        chart_data_list, chart_timestamp = obtener_datos_grafico_anual()

    if chart_data_list is None:
        loading_chart = True
        chart_data_list = []
        chart_timestamp = datetime.now()

    total_anual = 0
    total_2025 = 0
    total_2026 = 0
    promedio_mensual = 0
    mes_maximo = {"label": "-", "total": 0}

    if chart_data_list:
        total_anual = sum(item["total"] for item in chart_data_list)
        
        # Calculate split totals
        total_2025 = sum(item["total"] for item in chart_data_list if "25" in item.get("month", ""))
        total_2026 = sum(item["total"] for item in chart_data_list if "26" in item.get("month", ""))
        
        promedio_mensual = total_anual / len(chart_data_list)
        try:
            max_item = max(chart_data_list, key=lambda x: x["total"])
            mes_maximo = {
                "label": max_item.get("month_label", max_item.get("month", "-")),
                "total": max_item.get("total", 0)
            }
        except:
            pass

    monthly_details_preloaded = obtener_todos_datos_mensuales()

    return render_template(
        "tabla.html",
        tabla=df,
        headers=headers,
        raw_columns=raw_columns,
        ultima_actualizacion=chart_timestamp if chart_timestamp else datetime.now(),
        totals=totals,
        chart_data=chart_data_list,
        total_anual=total_anual, # Keep for backward compat if needed, or replace usage
        total_2025=total_2025,
        total_2026=total_2026,
        promedio_mensual=promedio_mensual,
        mes_maximo=mes_maximo,
        monthly_details_preloaded=monthly_details_preloaded,
        loading_table=loading_table,
        loading_chart=loading_chart
    )


@app.route("/api/summary-table")
def api_summary_table():
    df = obtener_o_actualizar_cache()
    if df is None:
        return jsonify({"status": "loading"}), 202

    if df.empty:
        return jsonify({"status": "empty"}), 200

    df_view = traducir_columnas_df(df)
    headers = list(df_view.columns)
    raw_columns = list(df.columns)

    totals = {}
    for col in df.columns:
        if col == "dia":
            totals[col] = "TOTAL"
        elif "_solicitudes" in col:
            totals[col] = int(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())
        elif "_false" in col and "porcentaje" not in col:
            totals[col] = int(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())

    for col in df.columns:
        if "porcentaje_false" in col:
            mes = col.replace("_porcentaje_false", "")
            sol_col = f"{mes}_solicitudes"
            err_col = f"{mes}_false"
            total_sol = totals.get(sol_col, 0)
            total_err = totals.get(err_col, 0)
            totals[col] = round((total_err / total_sol) * 100, 2) if total_sol > 0 else 0.0

    html = render_template(
        "components/summary_table.html",
        tabla=df,
        headers=headers,
        raw_columns=raw_columns,
        totals=totals
    )
    return jsonify({"status": "ready", "html": html})


@app.route("/api/annual-chart")
def api_annual_chart():
    chart_data_list, timestamp = obtener_datos_grafico_anual()

    if chart_data_list is None:
        return jsonify({"status": "loading"}), 202

    total_anual = 0
    total_2025 = 0
    total_2026 = 0
    promedio_mensual = 0
    mes_maximo = {"label": "-", "total": 0}

    if chart_data_list:
        total_anual = sum(item["total"] for item in chart_data_list)
        
        # Calculate split totals
        total_2025 = sum(item["total"] for item in chart_data_list if "25" in item.get("month", ""))
        total_2026 = sum(item["total"] for item in chart_data_list if "26" in item.get("month", ""))

        promedio_mensual = total_anual / len(chart_data_list)
        try:
            max_item = max(chart_data_list, key=lambda x: x["total"])
            mes_maximo = {
                "label": max_item.get("month_label", max_item.get("month", "-")),
                "total": max_item.get("total", 0)
            }
        except:
            pass

    monthly_details_preloaded = obtener_todos_datos_mensuales()

    return jsonify({
        "status": "ready",
        "chart_data": chart_data_list,
        "total_anual": total_anual,
        "total_2025": total_2025,
        "total_2026": total_2026,
        "promedio_mensual": promedio_mensual,
        "mes_maximo": mes_maximo,
        "ultima_actualizacion": timestamp.strftime("%d/%m %H:%M:%S") if timestamp else "-",
        "monthly_details_preloaded": monthly_details_preloaded
    })


@app.route("/descargar_resumen")
def descargar_resumen():
    df = obtener_o_actualizar_cache()

    if df is None or df.empty:
        return "No hay datos para descargar", 404

    # Hoy en Argentina (Buenos Aires)
    today_day = datetime.now(ZoneInfo("America/Argentina/Buenos_Aires")).day

    if "dia" in df.columns:
        df["dia_numeric"] = pd.to_numeric(df["dia"], errors="coerce").astype("Int64")
        df = df[df["dia_numeric"].fillna(-1) != today_day]
        df = df.drop(columns=["dia_numeric"])

    df = df.loc[:, ~df.columns.duplicated()]
    df_export = traducir_columnas_df(df)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_export.to_excel(writer, index=False, sheet_name="Resumen Monitoreo")

    output.seek(0)

    return send_file(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name="xroadtrimestral.xlsx"
    )


# ============================================================
# 📊 DATOS MES (Modal Tabla) -> Keeps as fallback endpoint
# ============================================================

@app.route("/datos_mes", methods=["POST"])
def datos_mes():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON"}), 400

    mes = data.get("mes")  # e.g. "Jan-25"
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

    query = f"""
    SELECT 
        EXTRACT(DAY FROM requestindatetime)::int AS dia,
        COUNT(*) AS solicitudes,
        SUM(CASE 
            WHEN succeeded = false 
                 AND (
                    statuscode IS NULL 
                    OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) > 499)
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

        df_res["pct"] = df_res.apply(
            lambda row: round((row["err"] / row["solicitudes"] * 100), 2) if row["solicitudes"] > 0 else 0,
            axis=1
        )

        json_res = df_res.to_json(orient="records", force_ascii=False)
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
# 🔍 DETALLES (Mensuales Caché) - ASYNC UPDATE
# ============================================================

def _generar_detalle_mes_bg(mes_sql, mode="error"):
    """
    Worker para generar detalles de un mes específico en background.
    mode='error'|'success'
    """
    global GENERATING_DETAILS_MONTHS

    gen_key = f"{mes_sql}:{mode}"
    print(f"[{datetime.now()}] ⚙️ Iniciando generación background de detalles ({mode}) para {mes_sql}...")

    try:
        engine = get_engine()
        if not engine:
            return

        # Cache key diferenciada por modo
        if mode == "success":
            cache_key = f"monitoring:detalles:mes:{mes_sql}:success_v2"
        else:
            cache_key = f"monitoring:detalles:mes:{mes_sql}"

        r = get_redis()

        if mode == "success":
            # Éxitos: 200-299
            where_clause = f"""
                TO_CHAR(requestindatetime, 'Mon-YY') = '{mes_sql}'
                AND (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) BETWEEN 200 AND 299)
            """
        else:
            # Errores: mantiene tu lógica amplia (no solo 500)
            where_clause = f"""
                TO_CHAR(requestindatetime, 'Mon-YY') = '{mes_sql}'
                AND succeeded = false
                AND (
                    statuscode IS NULL 
                    OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) < 200)
                    OR (statuscode ~ '^[0-9]+$' AND cast(statuscode as integer) >= 300)
                )
            """

        query = f"""
            SELECT 
                EXTRACT(DAY FROM requestindatetime)::int AS dia,
                servicesubsystemcode,
                servicecode,
                statuscode,
                COUNT(*) AS cantidad
            FROM public.tablaxroadmonitoreo
            WHERE 
                {where_clause}
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
            json_res = df_mes.to_json(orient="records", force_ascii=False) if not df_mes.empty else "[]"

            if r:
                r.set(cache_key, json_res, ex=CACHE_SHORT_SECONDS)
                print(f"[{datetime.now()}] 💾 Detalles ({mode}) mes {mes_sql} guardados en Redis.")
        except Exception as e:
            print(f"❌ Error SQL detalle ({mode}) mes {mes_sql}: {e}")

    except Exception as e:
        print(f"❌ Error worker detalle ({mode}) mes {mes_sql}: {e}")

    finally:
        if gen_key in GENERATING_DETAILS_MONTHS:
            del GENERATING_DETAILS_MONTHS[gen_key]


@app.route("/detalles", methods=["POST"])
def obtener_detalles():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON"}), 400

    dia = data.get("dia")
    columna = data.get("columna", "")

    if dia is None:
        return jsonify({"error": "dia is required"}), 400

    try:
        mes_año_part = columna.split("_")[0]  # "Jan-25"
    except:
        return jsonify({"error": "No se pudo determinar el mes."}), 400

    mes_sql = mes_año_part

    # Mode: si clickean solicitudes => queremos detalles "success"
    mode = "success" if "_solicitudes" in columna else "error"

    if mode == "success":
        cache_key = f"monitoring:detalles:mes:{mes_sql}:success_v2"
        gen_key = f"{mes_sql}:success"
    else:
        cache_key = f"monitoring:detalles:mes:{mes_sql}"
        gen_key = f"{mes_sql}:error"

    r = get_redis()

    # 1) Leer cache
    if r and r.exists(cache_key):
        try:
            content = r.get(cache_key)
            if content:
                df_mes = pd.read_json(io.StringIO(content), orient="records")

                if not df_mes.empty:
                    df_dia = df_mes[df_mes["dia"] == int(dia)].copy()
                    # ordenar: statuscode ASC, cantidad DESC
                    # (si hay NULL statuscode, pandas puede romper; lo normal es que venga como None)
                    if "statuscode" in df_dia.columns:
                        df_dia["statuscode_sort"] = pd.to_numeric(df_dia["statuscode"], errors="coerce")
                        df_dia = df_dia.sort_values(by=["statuscode_sort", "cantidad"], ascending=[True, False])
                        df_dia = df_dia.drop(columns=["statuscode_sort"])
                    else:
                        df_dia = df_dia.sort_values(by=["cantidad"], ascending=[False])

                    return df_dia.to_json(orient="records", force_ascii=False)

                return "[]"
        except Exception as e:
            print(f"Error reading details cache: {e}")

    # 2) Si ya se está generando
    if gen_key in GENERATING_DETAILS_MONTHS:
        return jsonify({"status": "loading", "message": "Generando datos..."}), 202

    # 3) Lanzar bg
    GENERATING_DETAILS_MONTHS[gen_key] = True
    threading.Thread(target=_generar_detalle_mes_bg, args=(mes_sql, mode)).start()

    return jsonify({"status": "loading", "message": "Iniciando generación..."}), 202


# ============================================================
# ▶️ MAIN
# ============================================================


if __name__ == "__main__":
    import logging
    log = logging.getLogger("werkzeug")
    log.setLevel(logging.ERROR)

    print(f"[{datetime.now()}] 🚀 Iniciando servidor en modo DEBUG/DESARROLLO...")
    
    # Imprimir endpoints disponibles
    print("\n🔗 Endpoints Disponibles:")
    with app.app_context():
        for rule in app.url_map.iter_rules():
            if "GET" in rule.methods and "static" not in rule.endpoint:
                url = f"http://localhost:5000{rule.rule}"
                print(f"   👉 {url}")
    print("\n")

    hilo_actualizador = threading.Thread(target=actualizar_periodicamente, args=(8,), daemon=True)
    hilo_actualizador.start()
    app.run(debug=True, host="0.0.0.0", port=5000, use_reloader=False)

