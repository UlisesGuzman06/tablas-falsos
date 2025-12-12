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

load_dotenv()

PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# V4: Restored key to clean state after reverts
REDIS_KEY = "monitoring:resumen_diario_restored_v4"

# Handle potential connection errors
try:
    engine = create_engine(f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")
except Exception as e:
    print(f"Warning: DB Engine creation failed (check .env): {e}")
    engine = None

try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
except Exception as e:
    print(f"Warning: Redis connection failed: {e}")
    r = None

# ============================================================
# ⚙️ FUNCIÓN — Generar tabla resumen (ORIGINAL LOGIC)
# ============================================================

def generar_tabla_resumen():
    if not engine:
        print("❌ Error: No hay conexión a base de datos.")
        return pd.DataFrame()

    print(f"[{datetime.now()}] Consultando datos agregados de PostgreSQL...")

    # RESTORED ORIGINAL QUERY: Counts everything, no filtering 300-599
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
    AND requestindatetime <  DATE_TRUNC('month', NOW() + INTERVAL '1 month')
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
GROUP BY 1, 2
ORDER BY 2, 1;

    """

    try:
        df = pd.read_sql(query, engine)
        print(f"✔️ Datos agregados cargados: {len(df)} registros.")
    except Exception as e:
        print(f"❌ Error ejecutando query SQL: {e}")
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    tabla = df.pivot(index="dia", columns="mes", values=["solicitudes", "false", "porcentaje_false"])
    tabla.columns = [f"{col[1]}_{col[0]}" for col in tabla.columns]
    tabla.index.name = "dia"
    tabla = tabla.reset_index().sort_values(by="dia").fillna("")
    tabla = tabla.loc[:, ~tabla.columns.duplicated()]

    meses = sorted(
        {c.split("_")[0] for c in tabla.columns if c != "dia"},
        key=lambda m: datetime.strptime(m, "%b-%y")
    )

    columnas_ordenadas = ["dia"] + [
        f"{mes}_{tipo}"
        for mes in meses
        for tipo in ["solicitudes", "false", "porcentaje_false"]
        if f"{mes}_{tipo}" in tabla.columns
    ]

    tabla = tabla[columnas_ordenadas]
    tabla = tabla.applymap(lambda x: int(x) if isinstance(x, float) and x.is_integer() else x)

    print(f"✔️ Tabla generada y ordenada por mes ({', '.join(meses)}).")
    return tabla

# ============================================================
# ⚙️ FUNCIÓN — Obtener o actualizar cache
# ============================================================

def obtener_o_actualizar_cache():
    if r and r.exists(REDIS_KEY):
        print(f"[{datetime.now()}] 🔥 Leyendo desde Redis (cache existente).")
        try:
            content = r.get(REDIS_KEY)
            if content:
                df = pd.read_json(io.StringIO(content), orient="records")
                return df
        except Exception as e:
            print(f"Error leyendo JSON de Redis: {e}")

    print(f"[{datetime.now()}] ⚙️ Cache no encontrado o expirado. Regenerando...")
    df = generar_tabla_resumen()
    
    if r and not df.empty:
        try:
            r.set(REDIS_KEY, df.reset_index(drop=True).to_json(orient="records"), ex=8 * 60 * 60)
            print(f"[{datetime.now()}] 💾 Datos guardados en Redis (expiran en 8 horas).")
        except Exception as e:
            print(f"Error escribiendo en Redis: {e}")
            
    return df

# ============================================================
# ⚙️ FUNCIÓN — Obtener datos para el gráfico anual (12 meses)
# ============================================================

def obtener_datos_grafico_anual():
    """
    Consulta la base de datos para obtener el conteo de errores 'estrictos'
    de los últimos 12 meses.
    """
    if not engine:
        return []

    cache_key = "monitoring:annual_chart"
    
    # 1. Try Redis
    if r and r.exists(cache_key):
        try:
            print(f"[{datetime.now()}] 🔥 Leyendo gráfico anual desde Redis.")
            return pd.read_json(io.StringIO(r.get(cache_key)), orient="records").to_dict(orient='records')
        except Exception as e:
            print(f"Error reading annual chart cache: {e}")

    # Using the STRICT definition of errors provided by user:
    # 1. succeeded = false
    # 2. statuscode IS NULL OR < 200 OR >= 300
    # 3. serviceCode NOT IN exclusions
    # 4. securityservertype = 'Client'
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
    
    try:
        df = pd.read_sql(query, engine)
        
        # 2. Save to Redis (8h)
        if r and not df.empty:
            try:
                r.set(cache_key, df.to_json(orient="records"), ex=8 * 60 * 60)
            except Exception as e:
                print(f"Error saving annual chart to Redis: {e}")
                
        return df[['month', 'total', 'errors']].to_dict(orient='records')
    except Exception as e:
        print(f"Error fetching annual chart data: {e}")
        return []

# ============================================================
# 🔁 HILO — Actualización periódica
# ============================================================

def actualizar_periodicamente(intervalo_horas=8):
    intervalo_segundos = intervalo_horas * 3600
    while True:
        print("\n" + "=" * 60)
        print(f"[{datetime.now()}] 🔄 Iniciando ciclo de actualización...")
        try:
            df = obtener_o_actualizar_cache()
            if not df.empty:
                print(df.head())
                print(f"[{datetime.now()}] ✅ Actualización completa. Próxima en {intervalo_horas} horas.")
            else:
                print(f"[{datetime.now()}] ⚠️ Actualización vacía o fallida.")
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
                    "false": "Errores",
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
    return """
    <div style="font-family: sans-serif; text-align: center; padding: 50px;">
        <h1 style="color: #4ade80;">Servidor Monitor Activo 🟢</h1>
        <p>El backend de monitoreo está ejecutándose correctamente (Versión Restaurada).</p>
        <a href='/tabla' style="display: inline-block; padding: 10px 20px; background-color: #3b82f6; color: white; text-decoration: none; border-radius: 5px;">Ver Tabla Resumen</a>
    </div>
    """

@app.route("/tabla")
def mostrar_tabla():
    df = obtener_o_actualizar_cache()
    if df.empty:
         return render_template("tabla.html", tabla=None, mensaje="Esperando datos... Verifica la conexión.", ultima_actualizacion=datetime.now())

    # Generar encabezados legibles para la vista HTML
    df_view = traducir_columnas_df(df)
    headers = list(df_view.columns)
    
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

    # Calcular datos para el gráfico (ANUAL - 12 Meses)
    # chart_data_list = []
    # meses_cols = sorted ... (REMOVED: Old logic)
    
    # NEW LOGIC: Fetch directly from DB using strict annual query
    chart_data_list = obtener_datos_grafico_anual()

    return render_template("tabla.html", tabla=df, headers=headers, raw_columns=list(df.columns), ultima_actualizacion=datetime.now(), totals=totals, chart_data=chart_data_list)

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
        download_name=f"resumen_monitoreo_{datetime.now().strftime('%Y%m%d')}.xlsx"
    )

# ============================================================
# 📊 DATOS MES (Modal Tabla)
# ============================================================

@app.route("/datos_mes", methods=["POST"])
def datos_mes():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON"}), 400
        
    mes = data.get("mes") # e.g. "Jan-25"
    if not mes:
        return jsonify({"error": "Mes is required"}), 400

    if not engine:
        return jsonify({"error": "No DB connection"}), 500

    cache_key = f"monitoring:month_data:{mes}"
    if r and r.exists(cache_key):
        try:
             print(f"[{datetime.now()}] 🔥 Leyendo datos mes ({mes}) desde Redis.")
             return r.get(cache_key)
        except Exception as e:
             print(f"Error reading month data cache: {e}")

    # Query for specific month details
    # We want: 
    # 1. Day
    # 2. Total Requests (excluding ignored services)
    # 3. Strict Errors (excluding ignored services AND strict status filters)
    
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
                r.set(cache_key, json_res, ex=8 * 60 * 60)
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

    try:
        if not engine:
             return jsonify({"error": "Sin conexión a DB"}), 500

        try:
            mes_año_part = columna.split("_")[0] # "Jan-25"
        except:
            return jsonify({"error": "No se pudo determinar el mes."}), 400

        mes_sql = mes_año_part
        cache_key = f"monitoring:detalles:mes:{mes_sql}"

        df_mes = pd.DataFrame()

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
                    r.set(cache_key, df_mes.to_json(orient="records", force_ascii=False), ex=8 * 60 * 60)
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
    hilo_actualizador = threading.Thread(target=actualizar_periodicamente, daemon=True)
    hilo_actualizador.start()
    app.run(debug=True)
