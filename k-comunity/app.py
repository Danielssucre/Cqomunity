import streamlit as st
import sqlite3
import pandas as pd
import datetime
import os
import time
import json
import io
import altair as alt
import random
import math
import plotly.express as px
from passlib.context import CryptContext  # Para hashing de contraseñas
import numpy as np
import shutil

# --- CONFIGURACIÓN DE PÁGINA Y SEGURIDAD ---
st.set_page_config(
    page_title="ResidentClubMD",  # Nuevo nombre de marca
    page_icon="🎓",               # Icono de Residente (Birrete)
    layout="wide",
    initial_sidebar_state="expanded"
)

# Contexto para hashear contraseñas
pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")
# --- CONFIGURACIÓN DE RUTAS (Local vs Render) ---
# Si existe la carpeta de Render, úsala. Si no, usa la carpeta local.
if os.path.exists("/opt/render/data"):
    DB_FILE = "/opt/render/data/prisma_srs.db"
else:
    DB_FILE = "prisma_srs.db"

BACKUP_DIR = os.path.join(os.path.dirname(DB_FILE) or '.', 'backups')

# --- FUNCIONES DE RESILIENCIA Y BASE DE DATOS ---

def run_auto_backup():
    """Crea un respaldo de la BD con timestamp y rota los 5 más recientes."""
    try:
        if not os.path.exists(BACKUP_DIR):
            os.makedirs(BACKUP_DIR)
            st.toast("Directorio de respaldos creado.")

        # Generar nombre de archivo con timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        backup_file = os.path.join(BACKUP_DIR, f"backup_{timestamp}.db")

        # Copiar el archivo de la base de datos
        shutil.copy2(DB_FILE, backup_file)

        # Gestión de espacio: rotación de respaldos
        backups = sorted(
            [os.path.join(BACKUP_DIR, f) for f in os.listdir(BACKUP_DIR) if f.endswith('.db')],
            key=os.path.getmtime
        )
        
        if len(backups) > 5:
            os.remove(backups[0]) # Borrar el más antiguo
            print(f"INFO: Respaldo antiguo '{backups[0]}' eliminado.")

        print(f"INFO: Respaldo de BD creado en '{backup_file}'.")

    except Exception as e:
        print(f"ERROR en Auto-Backup: {e}")


def get_db_conn():
    """Establece conexión con la BD SQLite, asegurando que el directorio exista."""
    # Obtener el directorio de la base de datos
    db_dir = os.path.dirname(DB_FILE)
    
    # CORRECCIÓN: Solo intentar crear el directorio si db_dir NO está vacío
    if db_dir and not os.path.exists(db_dir):
        try:
            os.makedirs(db_dir)
        except OSError as e:
            st.error(f"Error al crear directorio de base de datos: {e}")
            st.stop()
            
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row 
        return conn
    except sqlite3.Error as e:
        st.error(f"Error de conexión a SQLite: {e}")
        st.stop()

def get_ghost_profile():
    # Devuelve el diccionario del Usuario Fantasma (Referencia) o None.
    conn = get_db_conn()
    # Buscamos al usuario marcado como modelo (1)
    try:
        row = conn.execute("SELECT * FROM users WHERE is_reference_model = 1 LIMIT 1").fetchone()
        if row:
            # Convertimos el objeto sqlite3.Row a un diccionario normal
            return dict(row)
    except Exception as e:
        print(f"Error buscando fantasma: {e}")
    return None

def setup_database():
    """
    Crea y migra la base de datos de forma segura. Verifica la existencia de todas
    las tablas y columnas necesarias y las añade si no existen.
    """
    conn = get_db_conn()
    cursor = conn.cursor()
    
    # --- Creación de Tablas (si no existen) ---
    cursor.execute("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password_hash TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'user');")
    cursor.execute("CREATE TABLE IF NOT EXISTS questions (id INTEGER PRIMARY KEY AUTOINCREMENT, owner_username TEXT NOT NULL REFERENCES users(username), enunciado TEXT NOT NULL, opciones TEXT NOT NULL, correcta TEXT NOT NULL, retroalimentacion TEXT NOT NULL, tag_categoria TEXT, tag_tema TEXT);")
    cursor.execute("CREATE TABLE IF NOT EXISTS progress (username TEXT NOT NULL REFERENCES users(username), question_id INTEGER NOT NULL REFERENCES questions(id) ON DELETE CASCADE, due_date DATE NOT NULL, interval INTEGER NOT NULL DEFAULT 1, aciertos INTEGER NOT NULL DEFAULT 0, fallos INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (username, question_id));")
    cursor.execute("CREATE TABLE IF NOT EXISTS duels (id INTEGER PRIMARY KEY AUTOINCREMENT, challenger_username TEXT NOT NULL REFERENCES users(username), opponent_username TEXT NOT NULL REFERENCES users(username), question_ids TEXT NOT NULL, challenger_score INTEGER, opponent_score INTEGER, status TEXT NOT NULL, winner TEXT, created_at DATETIME NOT NULL);")
    cursor.execute("CREATE TABLE IF NOT EXISTS activity_log (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, action_type TEXT NOT NULL, timestamp DATETIME NOT NULL);")
    cursor.execute("CREATE TABLE IF NOT EXISTS deleted_users_log (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL, deletion_date DATETIME NOT NULL, reason TEXT);")
    cursor.execute("CREATE TABLE IF NOT EXISTS question_votes (id INTEGER PRIMARY KEY AUTOINCREMENT, user_username TEXT NOT NULL REFERENCES users(username), question_id INTEGER NOT NULL REFERENCES questions(id), vote_type INTEGER NOT NULL, timestamp DATETIME NOT NULL);")
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_user_question_vote ON question_votes (user_username, question_id);")

    # --- Migraciones Seguras de Columnas ---
    
    def add_column_if_not_exists(table, column_name, column_def):
        """Función auxiliar para añadir una columna de forma idempotente."""
        cursor.execute(f"PRAGMA table_info({table})")
        existing_columns = [col[1] for col in cursor.fetchall()]
        if column_name not in existing_columns:
            st.warning(f"Migrando BD: Añadiendo columna '{column_name}' a tabla '{table}'...")
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column_name} {column_def}")

    # Migraciones para la tabla 'users'
    add_column_if_not_exists('users', 'is_approved', 'INTEGER NOT NULL DEFAULT 0')
    add_column_if_not_exists('users', 'is_intensive', 'INTEGER NOT NULL DEFAULT 0')
    add_column_if_not_exists('users', 'max_inactivity_days', 'INTEGER NOT NULL DEFAULT 3')
    add_column_if_not_exists('users', 'status', "TEXT NOT NULL DEFAULT 'active'")
    add_column_if_not_exists('users', 'is_resident', 'INTEGER NOT NULL DEFAULT 0')
    add_column_if_not_exists('users', 'intensive_start_date', 'DATE')
    add_column_if_not_exists('users', 'total_active_days', 'INTEGER NOT NULL DEFAULT 0')
    add_column_if_not_exists('users', 'current_streak', 'INTEGER NOT NULL DEFAULT 0')
    add_column_if_not_exists('users', 'last_active_date', 'DATE')
    add_column_if_not_exists('users', 'last_streak_date', 'DATE')
    add_column_if_not_exists('users', 'is_reference_model', 'INTEGER DEFAULT 0')
    add_column_if_not_exists('users', 'final_exam_score', 'INTEGER DEFAULT NULL')
    add_column_if_not_exists('users', 'cohort_year', 'TEXT DEFAULT NULL')
    add_column_if_not_exists('users', 'target_exam_date', 'DATE DEFAULT NULL')
    add_column_if_not_exists('users', 'admitted_status', "TEXT DEFAULT 'Pending'")
    add_column_if_not_exists('users', 'admitted_specialty', 'TEXT DEFAULT NULL')
    add_column_if_not_exists('users', 'final_accuracy_snapshot', 'REAL DEFAULT 0.0')
    add_column_if_not_exists('users', 'avg_daily_questions', 'REAL DEFAULT 0.0')
    add_column_if_not_exists('users', 'avg_seconds_per_question', 'REAL DEFAULT 0.0')
    add_column_if_not_exists('users', 'total_questions_snapshot', 'INTEGER DEFAULT 0')

    # --- INICIO: Migraciones de Seguridad (Anti-Fuerza Bruta) ---
    add_column_if_not_exists('users', 'failed_attempts', 'INTEGER NOT NULL DEFAULT 0')
    add_column_if_not_exists('users', 'lockout_until', 'DATETIME DEFAULT NULL')
    # --- FIN: Migraciones de Seguridad ---

    # Migraciones para la tabla 'questions'
    add_column_if_not_exists('questions', 'status', "TEXT NOT NULL DEFAULT 'active'")
    add_column_if_not_exists('questions', 'karma', 'INTEGER NOT NULL DEFAULT 0') # Columna para Karma/Votos

    # Migraciones para la tabla 'progress' (FSRS)
    add_column_if_not_exists('progress', 'stability', 'REAL NOT NULL DEFAULT 0.0')
    add_column_if_not_exists('progress', 'difficulty', 'REAL NOT NULL DEFAULT 0.0')
    add_column_if_not_exists('progress', 'retrievability', 'REAL NOT NULL DEFAULT 0.0')
    add_column_if_not_exists('progress', 'last_review', 'DATE')

    # Migración para la tabla 'activity_log'
    add_column_if_not_exists('activity_log', 'metadata', 'TEXT')

    # --- Configuración del Admin por Defecto ---
    try:
        ADMIN_USER_DEFAULT = st.secrets["ADMIN_USER"]
        ADMIN_PASS_DEFAULT = st.secrets["ADMIN_PASS"]
    except (KeyError, FileNotFoundError):
        st.error("Error crítico: Faltan ADMIN_USER o ADMIN_PASS en los secretos de Streamlit (secrets.toml).")
        st.stop()

    cursor.execute("SELECT * FROM users WHERE username = ?", (ADMIN_USER_DEFAULT,))
    admin = cursor.fetchone()
    
    if not admin:
        admin_pass_bytes = ADMIN_PASS_DEFAULT.encode('utf-8')[:72]
        admin_pass_hash = pwd_context.hash(admin_pass_bytes)
        cursor.execute(
            "INSERT INTO users (username, password_hash, role, is_approved) VALUES (?, ?, 'admin', 1)",
            (ADMIN_USER_DEFAULT, admin_pass_hash)
        )
    else:
        cursor.execute("UPDATE users SET is_approved = 1, role = 'admin' WHERE username = ?", (ADMIN_USER_DEFAULT,))

    conn.commit()
    conn.close()

# --- FUNCIONES DE AUTENTICACIÓN Y HASHING ---

def verify_password(plain_password, hashed_password):
    """Verifica la contraseña plana contra el hash."""
    return pwd_context.verify(plain_password, hashed_password)

def get_user_role(username):
    """Obtiene el rol (admin/user) de un usuario."""
    conn = get_db_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT role FROM users WHERE username = ?", (username,))
    result = cursor.fetchone()
    conn.close()
    return result['role'] if result else None

def delete_user_from_db(username):
    """
    Elimina un usuario, transfiere sus preguntas al admin y limpia datos asociados.
    Sigue una lógica de expropiación para no perder contenido comunitario valioso.
    """
    try:
        # 1. Identificar al Admin
        admin_user = st.secrets["ADMIN_USER"]
    except KeyError:
        # Fallback para entorno local donde los secrets no están definidos
        admin_user = "admin"

    # 2. Validación: No eliminar al admin
    if username == admin_user:
        st.error(f"No se puede eliminar al usuario administrador principal ('{admin_user}').")
        return

    conn = None
    try:
        conn = get_db_conn()
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON")
        # Iniciar transacción
        cursor.execute("BEGIN TRANSACTION")

        # 3. Limpieza de Datos Personales (Borrar)
        # Eliminar participaciones en duelos
        cursor.execute("DELETE FROM duels WHERE challenger_username = ? OR opponent_username = ?", (username, username))
        # Eliminar todo el progreso de estudio
        cursor.execute("DELETE FROM progress WHERE username = ?", (username,))

        # 4. Preservación de Contenido (Transferir)
        # Actualizar el propietario de las preguntas para que pertenezcan al admin
        cursor.execute("UPDATE questions SET owner_username = ? WHERE owner_username = ?", (admin_user, username))

        # 5. Eliminación de Cuenta
        # Finalmente, eliminar el registro del usuario
        cursor.execute("DELETE FROM users WHERE username = ?", (username,))

        # Confirmar la transacción si todo fue exitoso
        conn.commit()

        st.success(f"Usuario '{username}' eliminado. Sus preguntas han sido transferidas al admin '{admin_user}'.")

    except sqlite3.Error as e:
        if conn:
            conn.rollback()  # Revertir cambios en caso de error de base de datos
        st.error(f"Error de base de datos al eliminar usuario: {e}")
    except Exception as e:
        if conn:
            conn.rollback()  # Revertir también en caso de otro tipo de error
        st.error(f"Ocurrió un error inesperado durante la eliminación: {e}")
    finally:
        if conn:
            conn.close()


def log_event(user_id, event_type, metadata_dict=None):
    """
    Registra un evento genérico en el activity_log con metadatos JSON.
    """
    conn = None
    try:
        # Asegura que los metadatos sean un diccionario antes de procesar
        if metadata_dict is None:
            metadata_dict = {}
        
        # Convierte el diccionario a un string JSON.
        meta_json = json.dumps(metadata_dict)
        
        conn = get_db_conn()
        cursor = conn.cursor()
        
        # Inserta el nuevo evento incluyendo los metadatos.
        cursor.execute(
            "INSERT INTO activity_log (username, action_type, timestamp, metadata) VALUES (?, ?, ?, ?)",
            (user_id, event_type, datetime.datetime.now(), meta_json)
        )
        conn.commit()
    
    except sqlite3.Error as e:
        # Error específico de la base de datos
        print(f"Error de base de datos al registrar evento: {e}")
    except TypeError as e:
        # Error durante la serialización a JSON (ej. un objeto no serializable)
        print(f"Error de serialización JSON al registrar evento: {e}")
    except Exception as e:
        # Cualquier otro error inesperado
        print(f"Error inesperado al registrar evento: {e}")
    finally:
        if conn:
            conn.close()

# --- PÁGINAS DE LA APLICACIÓN ---

# --- INICIO SECCIÓN DE FEATURES: Votos y Modo Intensivo ---

def cast_vote(conn, username, question_id, vote_type):
    """Registra o actualiza el voto de un usuario y activa la guillotina si es necesario."""
    cursor = conn.cursor()

    # Usamos INSERT OR REPLACE para manejar el UPSERT basado en el índice UNIQUE
    cursor.execute("""
        INSERT OR REPLACE INTO question_votes (user_username, question_id, vote_type, timestamp)
        VALUES (?, ?, ?, ?)
    """, (username, question_id, vote_type, datetime.datetime.now()))

    # --- Lógica del Gatillo (La Guillotina) ---
    if vote_type == -1:
        # Contar los votos negativos para esta pregunta
        unlike_count = cursor.execute(
            "SELECT COUNT(*) FROM question_votes WHERE question_id = ? AND vote_type = -1",
            (question_id,)
        ).fetchone()[0]
        
        # La Regla: Si hay 3 o más 'unlikes', la pregunta necesita revisión
        if unlike_count >= 3:
            cursor.execute("UPDATE questions SET status = 'needs_revision' WHERE id = ?", (question_id,))
            st.toast(f"Pregunta {question_id} enviada a revisión por votos negativos.")

def update_karma(conn, username, question_id, vote_type):
    """
    Gestiona el voto de un usuario y actualiza el contador de karma denormalizado
    en la tabla de preguntas dentro de una única transacción.
    """
    # 1. Registrar el voto individual
    cast_vote(conn, username, question_id, vote_type)
    
    # 2. Recalcular el karma total
    # Contamos los likes (1) y restamos los unlikes (-1)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT SUM(vote_type) FROM question_votes WHERE question_id = ?",
        (question_id,)
    )
    new_karma = cursor.fetchone()[0] or 0
    
    # 3. Actualizar el contador denormalizado en la tabla de preguntas
    cursor.execute(
        "UPDATE questions SET karma = ? WHERE id = ?",
        (new_karma, question_id)
    )

def get_question_votes(question_id):
    """Obtiene el conteo de likes y unlikes para una pregunta."""
    conn = get_db_conn()
    # Usamos COALESCE para asegurar que devolvemos 0 si no hay votos de un tipo
    query = """
        SELECT 
            COALESCE(SUM(CASE WHEN vote_type = 1 THEN 1 ELSE 0 END), 0) as likes,
            COALESCE(SUM(CASE WHEN vote_type = -1 THEN 1 ELSE 0 END), 0) as unlikes
        FROM question_votes
        WHERE question_id = ?
    """
    votes = conn.execute(query, (question_id,)).fetchone()
    conn.close()
    
    return votes['likes'], votes['unlikes']

def has_user_voted(username, question_id):
    """Verifica si un usuario ya ha votado por una pregunta específica."""
    conn = get_db_conn()
    vote = conn.execute(
        "SELECT 1 FROM question_votes WHERE user_username = ? AND question_id = ?",
        (username, question_id)
    ).fetchone()
    conn.close()
    return vote is not None

def calculate_user_score(username, days_limit=3):
    """Calcula el puntaje de actividad de un usuario, respetando la fecha de inicio del modo intensivo."""
    conn = get_db_conn()
    
    # 1. Obtener fecha de inicio del desafío
    user = conn.execute("SELECT intensive_start_date FROM users WHERE username = ?", (username,)).fetchone()
    
    # Calculamos el inicio de la ventana deslizante estándar (hace X días)
    window_start = datetime.datetime.now() - datetime.timedelta(days=days_limit)
    
    # Por defecto, filtramos por la ventana deslizante
    start_date_filter = window_start

    # 2. Lógica de Justicia: Si hay fecha de inicio, la respetamos.
    if user and user['intensive_start_date']:
        start_str = user['intensive_start_date']
        
        try:
            # El formato guardado es YYYY-MM-DD
            intensive_start = datetime.datetime.strptime(start_str, '%Y-%m-%d')
            # EL PARCHE: Usamos la fecha más reciente.
            # Si activó el modo hace 1 hora, start_date_filter será hace 1 hora (0 puntos previos).
            # Si activó hace 1 mes, start_date_filter será hace 3 días.
            start_date_filter = max(intensive_start, window_start)
        except (ValueError, TypeError) as e:
             # Fallback por si la fecha tuviera un formato con hora
            try:
                intensive_start = datetime.datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
                start_date_filter = max(intensive_start, window_start)
            except Exception as e2:
                print(f"⚠️ Error parseando fecha intensiva (formatos '%Y-%m-%d' y '%Y-%m-%d %H:%M:%S'): {e} / {e2}")

    # 3. Contar puntos (Answer=1, Create=2)
    query = """
        SELECT action_type 
        FROM activity_log 
        WHERE username = ? 
          AND timestamp >= ?
    """
    logs = conn.execute(query, (username, start_date_filter)).fetchall()
    conn.close()

    score = 0
    num_creadas = 0
    num_respuestas = 0
    for log in logs:
        action = log['action_type']
        if action in ['answer', 'answer_submitted']:
            score += 1
            num_respuestas += 1
        elif action == 'create':
            score += 2
            num_creadas += 1

    return score, num_creadas, num_respuestas

def show_productivity_widget():
    """Muestra un widget de productividad mejorado, visualmente consistente para todos los usuarios en modo intensivo."""
    conn = get_db_conn()
    user_settings = conn.execute(
        "SELECT is_intensive, max_inactivity_days, intensive_start_date FROM users WHERE username = ?",
        (st.session_state.current_user,)
    ).fetchone()
    conn.close()

    if not (user_settings and user_settings['is_intensive']):
        return

    days_limit = user_settings['max_inactivity_days']
    score, _, _ = calculate_user_score(st.session_state.current_user, days_limit)

    st.sidebar.markdown("---")
    st.sidebar.subheader("🔥 Modo Intensivo Activo")

    # --- LÓGICA DE RENDERIZADO CONDICIONAL ---
    is_in_grace_period = False
    days_active = 0
    if user_settings['intensive_start_date']:
        start_date = datetime.datetime.strptime(user_settings['intensive_start_date'], '%Y-%m-%d').date()
        days_active = (datetime.date.today() - start_date).days
        if days_active < days_limit:
            is_in_grace_period = True

    # Renderizado visual (se muestra siempre la barra y el puntaje)
    progress_value = min(score, 30) / 30.0
    st.sidebar.progress(progress_value)
    st.sidebar.metric(label=f"Cuota ({days_limit} días)", value=f"{score} / 30 Pts")

    if is_in_grace_period:
        st.sidebar.success(f"🛡️ Periodo de Gracia (Día {days_active + 1}/{days_limit})")
        if score < 30:
            faltante = 30 - score
            crear_nec = math.ceil(faltante / 2)
            st.sidebar.caption(f"Te faltan {faltante} pts. Puedes responder {faltante} preguntas o crear {crear_nec} nuevas.")
        else:
            st.sidebar.caption("¡Ya cumpliste la cuota! Sigue así.")

    else: # Modo Normal (sin gracia)
        if score >= 30:
            st.sidebar.success("✅ Cuota Cubierta")
        else:
            st.sidebar.warning("⚠️ En riesgo de eliminación")
        
        st.sidebar.caption("Responde (1pt) o Crea (2pts) para sumar.")

# --- FIN SECCIÓN DE FEATURES ---


def show_rules_page():
    """Crea una página visual para explicar las reglas, métricas y rangos."""
    st.header("📜 Reglamento y Guía de Supervivencia")
    st.markdown("¡Bienvenido a la arena de conocimiento! Aquí te explicamos cómo funciona todo.")

    tab1, tab2, tab3 = st.tabs(["📊 El Tablero de Control (Métricas)", "🔥 La Constitución del Modo Intensivo", "🏆 Rangos y Medallas"])

    # --- Pestaña 1: Métricas ---
    with tab1:
        st.subheader("📊 El Tablero de Control (Métricas)")
        
        st.markdown("""
        #### Tasa de Aprendizaje
        Esta métrica es clave. Mide tu **conocimiento a largo plazo**. Se calcula sobre las preguntas que has respondido correctamente y cuyo próximo repaso está programado para **más de 7 días** en el futuro. Un porcentaje alto aquí significa que estás reteniendo la información de verdad.
        """)
        
        # Gráfico de Torta para Tasa de Aprendizaje
        df_aprendizaje = pd.DataFrame({
            'Estado': ['Aprendido (Largo Plazo)', 'Por Aprender'],
            'Cantidad': [20, 80]
        })
        chart_aprendizaje = alt.Chart(df_aprendizaje).mark_arc(innerRadius=50).encode(
            theta=alt.Theta(field="Cantidad", type="quantitative"),
            color=alt.Color(field="Estado", type="nominal", scale=alt.Scale(scheme='greens')),
            tooltip=['Estado', 'Cantidad']
        ).properties(
            title='Ej: Tasa de Aprendizaje del 20%'
        )
        st.altair_chart(chart_aprendizaje, use_container_width=True)

        st.markdown("""
        ---
        #### Precisión
        Mide la **calidad de tus respuestas inmediatas**. Es la simple pero poderosa relación entre tus aciertos y tus fallos. Una precisión alta indica que entiendes los conceptos al momento de estudiarlos.
        
        ---
        #### Progreso vs. Experto
        Esto no es solo una carrera contra ti mismo, es una **competencia contra el estándar de los mejores**. Tu progreso se compara con el rendimiento promedio de los **Residentes 🎓**, los usuarios más experimentados que ya han aprobado el examen real. ¡Aspira a superar su marca!
        """)

    # --- Pestaña 2: Modo Intensivo ---
    with tab2:
        st.subheader("🔥 Cómo sobrevivir a la guillotina")
        st.error("**La Regla de Oro:** Debes sumar **30 Puntos** en cada ciclo (normalmente 3 días). Si no cumples, tu cuenta será marcada para eliminación.")

        st.markdown("#### Tabla de Puntuación:")
        st.markdown("""
        | Acción | Puntos | Descripción |
        |---|---|---|
        | 📝 **Crear Pregunta** | **2 Puntos** | El mayor valor. Aportar conocimiento a la comunidad es la acción más recompensada. |
        | 🧠 **Responder Pregunta**| **1 Punto** | Estudiar y contestar preguntas del sistema te mantiene en forma y suma puntos. |
        """)
        
        st.markdown("---")
        st.subheader("Ejemplos de Estrategias de Supervivencia")

        # Gráfico de Barras para Estrategias
        df_estrategias = pd.DataFrame({
            'Estrategia': ['Solo Responder', 'Solo Crear', 'Mix Equilibrado'],
            'Acciones Necesarias': [30, 15, 20], # 30 respuestas, 15 creadas, 10 creadas + 10 respondidas = 20 acciones
            'Detalle': ['30 Respuestas', '15 Preguntas Creadas', '10 Creadas + 10 Respuestas']
        })
        
        chart_estrategias = alt.Chart(df_estrategias).mark_bar().encode(
            x=alt.X('Estrategia', sort=None, title=''),
            y=alt.Y('Acciones Necesarias', title='Cantidad de Acciones para llegar a 30 Pts'),
            color=alt.Color('Estrategia', legend=None),
            tooltip=['Estrategia', 'Detalle']
        ).properties(
            title='Cómo Acumular 30 Puntos'
        )
        st.altair_chart(chart_estrategias, use_container_width=True)
        st.caption("El gráfico muestra cuántas acciones de cada tipo necesitas para cumplir la cuota. Un 'Mix' es a menudo la estrategia más sostenible.")

    # --- Pestaña 3: Rangos y Medallas ---
    with tab3:
        st.subheader("🏆 Jerarquía de la Comunidad")
        
        st.markdown("""
        Tu rango refleja tu pericia, consistencia y contribución.
        
        # 🎓 Residente
        El 'Sensei'. Un usuario que **ha aprobado el examen real** y cuya cuenta ha sido verificada por un administrador. Son la fuente de sabiduría y el estándar a seguir.
        
        # ⭐ Experto
        El 'Alumno Estrella'. Un usuario con una **Precisión superior al 95%** y un volumen de estudio muy alto. Demuestra un dominio casi total del material.
        
        # 🦁 Avanzado
        El pilar de la comunidad. Un usuario **constante y con buen rendimiento**. Sigue las reglas y progresa adecuadamente.
        
        # 🚑 En Riesgo
        Una señal de alerta. Este usuario tiene una **precisión muy baja** o parece estar haciendo 'spam' (responde mucho pero no retiene, indicando falta de aprendizaje real). Necesita mejorar para no ser purgado.
        
        ---
        #### El Poder del Karma (Votos)
        En cada pregunta que respondas, podrás votar si es de buena calidad (👍) o si tiene errores (👎).
        - **Votos Positivos (👍):** Aumentan la reputación de la pregunta y de su creador.
        - **Votos Negativos (👎):** ¡Cuidado! Si una pregunta acumula 3 o más votos negativos, es marcada para revisión por un administrador. Abusar de preguntas de baja calidad puede afectar tu estatus.
        """)

def check_rate_limit():
    """Previene abuso por acciones demasiado rápidas (spam/scraping)."""
    now = datetime.datetime.now()
    last_action = st.session_state.get("last_action_time", None)

    if last_action and (now - last_action).total_seconds() < 2:
        st.warning("⏳ Vas muy rápido. Tómate un respiro.")
        st.stop()
    
    # Actualiza el tiempo de la acción actual para la próxima verificación.
    st.session_state.last_action_time = now


def update_user_activity(conn, username):
    """
    Actualiza la racha y los días de actividad de un usuario de forma segura,
    utilizando una conexión de BD existente.
    """
    # Aseguramos traer todas las columnas necesarias para la lógica
    user = conn.execute("SELECT last_active_date, current_streak, total_active_days, last_streak_date FROM users WHERE username = ?", (username,)).fetchone()
    
    if not user:
        return

    today = datetime.date.today()
    last_active_str = user['last_active_date']
    
    # Si el usuario ya estudió hoy, no hacemos nada.
    if last_active_str == today.strftime('%Y-%m-%d'):
        return
        
    current_streak = user['current_streak'] or 0
    total_active_days = user['total_active_days'] or 0

    if last_active_str is None:
        # Primer día de actividad
        new_streak = 1
        new_total_days = 1
    else:
        last_active_date = datetime.datetime.strptime(last_active_str, '%Y-%m-%d').date()
        yesterday = today - datetime.timedelta(days=1)
        
        if last_active_date == yesterday:
            # La racha continúa
            new_streak = current_streak + 1
            new_total_days = total_active_days + 1
        else:
            # La racha se rompió
            new_streak = 1
            new_total_days = total_active_days + 1
            
    conn.execute(
        "UPDATE users SET last_active_date = ?, current_streak = ?, total_active_days = ? WHERE username = ?",
        (today, new_streak, new_total_days, username)
    )

def show_login_page():
    """Muestra un dashboard de bienvenida con métricas y gestiona el login/registro."""
    # --- 1. SECCIÓN MOTIVACIONAL Y MÉTRICAS ---
    # Conexión solo para métricas
    conn_metrics = get_db_conn()
    try:
        q_count = conn_metrics.execute("SELECT COUNT(*) FROM questions").fetchone()[0]
        u_count = conn_metrics.execute("SELECT COUNT(*) FROM users WHERE role != 'admin' AND status = 'active'").fetchone()[0]
        try:
            del_count = conn_metrics.execute("SELECT COUNT(*) FROM deleted_users_log").fetchone()[0]
        except sqlite3.OperationalError:
            del_count = 0 # Fallback si la tabla no existe
    except Exception as e:
        q_count, u_count, del_count = "N/A", "N/A", "N/A"
        print(f"DEBUG: Error cargando métricas del login: {e}")
    finally:
        if conn_metrics:
            conn_metrics.close()

    # Frase Central
    st.markdown("""
        <div style='text-align: center; padding: 20px 0;'>
            <h2 style='font-size: 24px; font-weight: 600; color: #E0E0E0;'>
                "La única diferencia entre el que se queja y el que mejora es que el segundo no se rinde."
            </h2>
            <hr style='margin-top: 20px; margin-bottom: 20px; border-color: #333;'>
        </div>
    """, unsafe_allow_html=True)

    # Métricas Sociales
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        st.metric("📚 Preguntas en Banco", f"{q_count}")
    with col2:
        st.metric("👥 Estudiantes Activos", f"{u_count}")
    with col3:
        st.markdown(f"<br><p style='font-size: 12px; color: #666; text-align: center;'>☠️ {del_count} Estudiantes Eliminados</p>", unsafe_allow_html=True)
    
    st.markdown("---")

    # --- 2. LOGIN (Mantenemos la lógica existente pero limpia) ---
    with st.form("login_form"):
        st.markdown("### Ingreso")
        username = st.text_input("Nombre de usuario")
        password = st.text_input("Contraseña", type="password")
        login_submitted = st.form_submit_button("Ingresar")

        if login_submitted:
            check_rate_limit()
            # Higiene de datos: eliminar espacios y forzar minúsculas
            clean_username = username.strip().lower()
            conn = get_db_conn()
            
            # --- INICIO: Lógica Anti-Fuerza Bruta ---
            user = conn.execute("SELECT * FROM users WHERE username = ?", (clean_username,)).fetchone()

            if not user:
                st.error("Usuario o contraseña incorrectos.")
                if conn: conn.close()
                return

            # 1. Chequeo de Bloqueo
            if user['lockout_until']:
                try:
                    lockout_time = datetime.datetime.fromisoformat(user['lockout_until'])
                    if lockout_time > datetime.datetime.now():
                        remaining_time = lockout_time - datetime.datetime.now()
                        minutes = math.ceil(remaining_time.total_seconds() / 60)
                        st.error(f"Cuenta bloqueada temporalmente. Intenta de nuevo en {minutes} minutos.")
                        if conn: conn.close()
                        return
                except (ValueError, TypeError):
                    # Ignorar si el formato de fecha es inválido y proceder
                    pass

            # 2. Verificación de Contraseña
            if verify_password(password, user['password_hash']):
                # ACIERTO: Resetear contadores y proceder al login
                if user['failed_attempts'] > 0 or user['lockout_until'] is not None:
                    conn.execute("UPDATE users SET failed_attempts = 0, lockout_until = NULL WHERE username = ?", (clean_username,))
                    conn.commit()
                
                # --- Lógica de login existente ---
                if user['status'] == 'pending_delete':
                    st.error("Cuenta bloqueada por incumplimiento. Contacta al administrador.")
                    conn.close()
                    return

                if user['is_intensive']:
                    is_in_grace_period = False
                    start_date_str = user['intensive_start_date']

                    if start_date_str is None:
                        today = datetime.date.today()
                        conn.execute("UPDATE users SET intensive_start_date = ? WHERE username = ?", (today, clean_username))
                        conn.commit()
                        st.success(f"🛡️ Periodo de Gracia activado. Tienes {user['max_inactivity_days']} días para cumplir tu cuota.")
                        is_in_grace_period = True
                    else:
                        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
                        days_active = (datetime.date.today() - start_date).days
                        if days_active < user['max_inactivity_days']:
                            is_in_grace_period = True

                    if not is_in_grace_period:
                        score, _, _ = calculate_user_score(clean_username, user['max_inactivity_days'])
                        last_activity_row = conn.execute("SELECT MAX(timestamp) as last_ts FROM activity_log WHERE username = ?", (clean_username,)).fetchone()
                        is_inactive = False
                        if last_activity_row and last_activity_row['last_ts']:
                            last_activity_date = datetime.datetime.fromisoformat(last_activity_row['last_ts'])
                            if (datetime.datetime.now() - last_activity_date).days > user['max_inactivity_days']:
                                is_inactive = True
                        else:
                            is_inactive = True
                        if score < 30 or is_inactive:
                            conn.execute("UPDATE users SET status = 'pending_delete' WHERE username = ?", (clean_username,))
                            conn.commit()
                            st.error("Cuenta bloqueada por incumplimiento del Modo Intensivo. Contacta al administrador.")
                            conn.close()
                            return
                
                if user['is_approved'] == 1:
                    st.session_state.logged_in = True
                    st.session_state.current_user = user['username']
                    st.session_state.user_role = user['role']
                    st.session_state.current_page = "evaluacion"
                    conn.close()
                    st.rerun()
                else:
                    st.error("Tu cuenta está registrada, pero aún no ha sido aprobada por un administrador.")
            else:
                # FALLO: Incrementar contador y potencialmente bloquear
                new_attempts = user['failed_attempts'] + 1
                if new_attempts >= 5:
                    lockout_time = datetime.datetime.now() + datetime.timedelta(minutes=15)
                    conn.execute("UPDATE users SET failed_attempts = 0, lockout_until = ? WHERE username = ?", (lockout_time.isoformat(), clean_username))
                    st.error("Contraseña incorrecta. Has superado el límite de intentos. Cuenta bloqueada por 15 minutos.")
                else:
                    conn.execute("UPDATE users SET failed_attempts = ? WHERE username = ?", (new_attempts, clean_username))
                    st.error(f"Usuario o contraseña incorrectos. Intento {new_attempts} de 5.")
                
                conn.commit()
            
            if conn:
                conn.close()

    # --- 3. REGISTRO (ENCAPSULADO) ---
    st.markdown("<br>", unsafe_allow_html=True)
    with st.expander("📝 Registro de Usuario Nuevo", expanded=False):
        with st.form("register_form", clear_on_submit=True):
            new_username = st.text_input("Nuevo nombre de usuario")
            new_password = st.text_input("Nueva contraseña", type="password")
            reg_submitted = st.form_submit_button("Registrarse")

            if reg_submitted:
                # Higiene de datos: guardar siempre limpio
                clean_new_username = new_username.strip().lower()

                if not clean_new_username or not new_password:
                    st.warning("Usuario y contraseña no pueden estar vacíos.")
                elif clean_new_username == st.secrets["ADMIN_USER"].lower():
                     st.error("Nombre de usuario no disponible.")
                else:
                    try:
                        password_new_bytes = new_password.encode('utf-8')[:72]
                        hashed_pass = pwd_context.hash(password_new_bytes)
                        conn = get_db_conn()
                        cursor = conn.cursor()
                        cursor.execute(
                            "INSERT INTO users (username, password_hash, role) VALUES (?, ?, 'user')",
                            (clean_new_username, hashed_pass)
                        )
                        conn.commit()
                        conn.close()
                        st.success("¡Usuario registrado! Tu cuenta está pendiente de aprobación por un administrador.")
                    except sqlite3.IntegrityError:
                        st.error("Ese nombre de usuario ya existe.")
                    except Exception as e:
                        st.error(f"Error al registrar: {e}")

def show_create_page():
    """Muestra el formulario para crear nuevas preguntas (con etiquetas)."""
    st.subheader("🖊️ Crear Nueva Pregunta")
    
    CATEGORIAS_MEDICAS = [
        "Medicina Interna", "Cirugía General", "Ortopedia", "Urología", 
        "ORL", "Urgencia", "Psiquiatría", "Neurología", "Neurocirugía", 
        "Epidemiología", "Pediatría", "Ginecología", "Oftalmología", "Otra"
    ]
    
    with st.form("create_question_form", clear_on_submit=True):
        enunciado = st.text_area("Enunciado de la pregunta")
        opciones = []
        opciones.append(st.text_input("Opción A"))
        opciones.append(st.text_input("Opción B"))
        opciones.append(st.text_input("Opción C"))
        opciones.append(st.text_input("Opción D"))
        
        correcta_idx = st.radio("Respuesta Correcta", (0, 1, 2, 3), format_func=lambda x: f"Opción {chr(65+x)}")
        retroalimentacion = st.text_area("Retroalimentación (Explicación)")
        
        st.markdown("---")
        tag_categoria = st.selectbox("Etiqueta 1: Categoría", options=CATEGORIAS_MEDICAS, index=None)
        tag_tema = st.text_input("Etiqueta 2: Tema")
        
        submitted = st.form_submit_button("Guardar Pregunta")
        
        if submitted:
            check_rate_limit()
            if not all([enunciado, opciones[0], opciones[1], opciones[2], opciones[3], retroalimentacion, tag_categoria, tag_tema]):
                st.warning("Por favor, completa todos los campos.")
            else:
                conn = get_db_conn()
                cursor = conn.cursor()
                opciones_str = "|".join(opciones) 
                correcta = opciones[correcta_idx]
                owner = st.session_state.current_user
                
                cursor.execute(
                    "INSERT INTO questions (owner_username, enunciado, opciones, correcta, retroalimentacion, tag_categoria, tag_tema) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (owner, enunciado, opciones_str, correcta, retroalimentacion, tag_categoria, tag_tema)
                )

                # --- INICIO SECCIÓN MODO INTENSIVO: Registrar actividad ---
                cursor.execute(
                    "INSERT INTO activity_log (username, action_type, timestamp) VALUES (?, 'create', ?)",
                    (owner, datetime.datetime.now())
                )
                # --- FIN SECCIÓN MODO INTENSIVO ---
                
                # --- INICIO ACTUALIZACIÓN DE RACHA ---
                update_user_activity(conn, owner)
                # --- FIN ACTUALIZACIÓN DE RACHA ---

                conn.commit()
                conn.close()
                st.success("¡Pregunta guardada con éxito!")

def get_next_question_for_user(username, practice_mode=False): # practice_mode es ahora ignorado
    """
    Obtiene la próxima pregunta para el usuario, fusionando Evaluación y Práctica en un Flujo Infinito.
    También soporta el modo de práctica por temas de la Biblioteca como una entrada prioritaria.
    
    Jerarquía del Flujo Infinito:
    1. Vencidas/Nuevas -> 2. Adelantos Futuros -> 3. Aleatorio (Respaldo).
    
    Devuelve un diccionario {'id': question_id, 'is_advance': bool} o None si no hay preguntas.
    """
    conn = get_db_conn()
    cursor = conn.cursor()
    today = datetime.date.today()

    # --- MODO PRIORITARIO: Práctica por Tema (de la Biblioteca) ---
    # Se mantiene esta funcionalidad ya que es una selección explícita del usuario
    if st.session_state.get('practice_mode') and st.session_state.get('selected_tag'):
        tag = st.session_state.selected_tag
        cursor.execute(
            "SELECT id FROM questions WHERE tag_tema = ? AND status = 'active' ORDER BY RANDOM() LIMIT 1",
            (tag,)
        )
        practice_question = cursor.fetchone()
        conn.close()
        if not practice_question:
            return None
        # Se retorna con la nueva estructura, asumiendo que no es un adelanto.
        return {'id': practice_question['id'], 'is_advance': False}

    # --- MODO PRINCIPAL: Flujo de Estudio Infinito ---
    
    # Intento 1: Preguntas Vencidas (due) y Nuevas (new)
    # Se priorizan las vencidas sobre las nuevas gracias al ORDER BY
    query_priority = """
        SELECT q.id
        FROM questions q
        LEFT JOIN progress p ON q.id = p.question_id AND p.username = ?
        WHERE
            q.status = 'active' AND (p.due_date <= ? OR p.question_id IS NULL)
        ORDER BY
            CASE WHEN p.due_date <= ? THEN 0 ELSE 1 END, -- Vencidas (0) antes que Nuevas (1)
            p.due_date ASC -- Las más vencidas primero
        LIMIT 1
    """
    cursor.execute(query_priority, (username, today, today))
    question = cursor.fetchone()
    if question:
        conn.close()
        return {'id': question['id'], 'is_advance': False}

    # Intento 2: Adelantos Inteligentes (preguntas futuras)
    query_advance = """
        SELECT q.id
        FROM questions q
        JOIN progress p ON q.id = p.question_id
        WHERE
            p.username = ? AND q.status = 'active' AND p.due_date > ?
            AND (p.last_review IS NULL OR p.last_review != ?)
        ORDER BY p.due_date ASC -- Las que vencen más pronto primero
        LIMIT 1
    """
    cursor.execute(query_advance, (username, today, today))
    question = cursor.fetchone()
    if question:
        conn.close()
        return {'id': question['id'], 'is_advance': True}

    # Intento 3: Respaldo Final (Cualquier pregunta activa)
    # Solo se llega aquí si no hay vencidas, ni nuevas, ni futuras (ej. todo se repasó hoy).
    query_fallback = "SELECT id FROM questions WHERE status = 'active' ORDER BY RANDOM() LIMIT 1"
    cursor.execute(query_fallback)
    question = cursor.fetchone()
    conn.close()
    
    if question:
        # Se considera un adelanto forzado, ya que no estaba en la cola prioritaria.
        return {'id': question['id'], 'is_advance': True}

    # Si no hay absolutamente ninguna pregunta activa en el sistema.
    return None

def update_srs(conn, username, question_id, difficulty_rating):
    """
    Actualiza el SRS en la BD usando la lógica FSRS v4 simplificada y registra la actividad.
    Rating mapping: 'difícil'->1 (Olvido), 'medio'->3 (Costoso), 'fácil'->5 (Bien).
    """
    cursor = conn.cursor()
    today = datetime.date.today()

    # 1. Mapeo del rating de entrada a un grado numérico
    if difficulty_rating == "difícil":
        grade = 1
    elif difficulty_rating == "medio":
        grade = 3
    else: # "fácil"
        grade = 5
    
    # 2. Obtener el estado SRS actual de la pregunta para el usuario
    cursor.execute(
        "SELECT stability, difficulty FROM progress WHERE username = ? AND question_id = ?",
        (username, question_id)
    )
    progress = cursor.fetchone()

    s_prev = progress['stability'] if progress and progress['stability'] is not None else 0.0
    d_prev = progress['difficulty'] if progress and progress['difficulty'] is not None else 0.0

    # 3. Cálculo de Dificultad (D)
    if d_prev == 0.0:
        d_new = 5.0  # Valor inicial si es la primera vez
    else:
        # El 'costo' en la fórmula se deriva del 'grade'
        d_new = d_prev - 0.32 + (0.18 * (grade - 3.0))
    
    d_new = max(1.0, min(10.0, d_new))  # Se asegura que D esté entre 1.0 y 10.0

    # 4. Cálculo de Estabilidad (S)
    if s_prev == 0.0:  # Si la tarjeta es nueva
        if grade == 1: s_new = 0.4
        elif grade == 3: s_new = 2.0
        else: s_new = 5.0
    else:  # Si la tarjeta está en repaso
        if grade == 1:  # Olvido
            s_new = s_prev * 0.4  # Penalización a la estabilidad
        else:  # Recordado (Medio o Fácil)
            factor_crecimiento = 1 + (1.5 / (d_new * 0.3))
            s_new = s_prev * factor_crecimiento

    # 5. Cálculo del nuevo Intervalo (I)
    # El intervalo busca una probabilidad de recuerdo (retrievability) del 90%
    new_interval = max(1, round(s_new * 0.9))

    # 6. Cálculo de la nueva fecha de repaso
    new_due_date = today + datetime.timedelta(days=int(new_interval))

    # 7. Actualización de contadores de aciertos/fallos (lógica heredada)
    cursor.execute("SELECT aciertos, fallos FROM progress WHERE username = ? AND question_id = ?", (username, question_id))
    aciertos_fallos = cursor.fetchone()
    aciertos = aciertos_fallos['aciertos'] if aciertos_fallos else 0
    fallos = aciertos_fallos['fallos'] if aciertos_fallos else 0
    
    if grade == 1:
        fallos += 1
    else:
        aciertos += 1

    # 8. Actualizar la base de datos con todos los nuevos valores (UPSERT)
    cursor.execute("""
        INSERT INTO progress (username, question_id, due_date, interval, aciertos, fallos, stability, difficulty, last_review)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(username, question_id) DO UPDATE SET
            due_date = excluded.due_date,
            interval = excluded.interval,
            aciertos = excluded.aciertos,
            fallos = excluded.fallos,
            stability = excluded.stability,
            difficulty = excluded.difficulty,
            last_review = excluded.last_review
    """, (username, question_id, new_due_date, new_interval, aciertos, fallos, s_new, d_new, today))
    
    # --- Registrar actividad para Modo Intensivo y Rachas ---
    cursor.execute(
        "INSERT INTO activity_log (username, action_type, timestamp) VALUES (?, 'answer', ?)",
        (username, datetime.datetime.now())
    )
    update_user_activity(conn, username)

def reset_evaluation_state():
    """Resetea el estado para mostrar la siguiente pregunta."""
    st.session_state.eval_state = "showing_question"
    st.session_state.user_answer = None
    if 'current_eval_question_data' in st.session_state:
        del st.session_state['current_eval_question_data']
    # También es buena idea limpiar el estado de avance previo al resetear
    if 'previous_is_advance' in st.session_state:
        del st.session_state['previous_is_advance']

def render_question_card(question_id):
    # --- SENSOR DE INICIO (CRONÓMETRO) ---
    # Usamos el ID de la pregunta para crear un timer único
    start_key = f"timer_start_{question_id}"
    if start_key not in st.session_state:
        st.session_state[start_key] = datetime.datetime.now()
    
    # --- LÓGICA AUTO-CURABLE (ANTI-ZOMBIE) ---
    # Detectamos si la tarjeta cree que ya terminó ('done') pero se le ha pedido renderizar de nuevo.
    key_state = f"card_state_{question_id}"
    current_state = st.session_state.get(key_state)

    # Si el estado es 'done', significa que es un residuo de una sesión anterior.
    # Debemos reiniciarlo obligatoriamente para que el usuario pueda responder.
    if current_state == 'done':
        # Borramos variables clave para forzar un reinicio limpio
        keys_to_purge = [
            key_state,
            f"user_answer_{question_id}",
            f"feedback_shown_{question_id}",
            f"shuffled_options_{question_id}"
        ]
        for k in keys_to_purge:
            if k in st.session_state:
                del st.session_state[k]
        
        # Forzamos el estado inicial
        st.session_state[key_state] = "showing_question"
    # ----------------------------------------
    # --- 1. Inicialización y Carga de Datos ---
    next_question_requested = False
    card_state_key = f"card_state_{question_id}"
    user_answer_key = f"user_answer_{question_id}"
    
    # Inicializar el estado de la tarjeta si no existe
    if card_state_key not in st.session_state:
        st.session_state[card_state_key] = "showing_question"

    conn = get_db_conn()
    pregunta_row = conn.execute("SELECT * FROM questions WHERE id = ?", (question_id,)).fetchone()
    conn.close()
    
    if not pregunta_row:
        st.error("Error: La pregunta no se encontró en la base de datos.")
        return True # Solicitar pasar a la siguiente para evitar un bucle

    pregunta = dict(pregunta_row)

    # --- BLINDAJE CONTRA DATOS CORRUPTOS ---
    try:
        # Asegurarse de que el campo 'opciones' no es None, no está vacío y contiene el separador.
        if not pregunta['opciones'] or '|' not in pregunta['opciones']:
            raise ValueError("Formato de opciones inválido o ausente.")
        
        parsed_options = pregunta['opciones'].split('|')
        
        # Validar que después del split, no haya quedado una lista de strings vacíos
        if len(parsed_options) < 2 or not all(op.strip() for op in parsed_options):
             raise ValueError("Al menos una de las opciones está vacía.")
             
    except (ValueError, TypeError, KeyError) as e:
        st.error(f"Datos de pregunta corruptos (ID: {question_id}). Un administrador debería revisarla. Saltando pregunta.")
        st.caption(f"Detalle técnico: {e}")
        return True # Devuelve True para que show_evaluation_page pida la siguiente.

    # --- LÓGICA DE SHUFFLE PERSISTENTE ---
    # Generamos una key única para guardar el orden de esta pregunta específica
    shuffle_key = f"shuffled_options_{question_id}"
    # Si ya tenemos un orden guardado para esta pregunta, lo usamos (para evitar rebajar al interactuar)
    if shuffle_key in st.session_state:
        display_options = st.session_state[shuffle_key]
    else:
        # Si es nueva, barajamos y guardamos
        display_options = parsed_options.copy() # Copia para no alterar original si hiciera falta
        random.shuffle(display_options)
        st.session_state[shuffle_key] = display_options
    # -------------------------------------

    # --- 2. Lógica de Renderizado y Estado ---
    st.markdown(f"### {pregunta['enunciado']}")

    # --- ESTADO: MOSTRANDO PREGUNTA Y OPCIONES ---
    if st.session_state.get(card_state_key) == "showing_question":
        with st.form(f"form_{question_id}"):
            user_choice = st.radio(
                "Selecciona tu respuesta:", 
                options=display_options,
                key=f"radio_{question_id}"
            )
            if st.form_submit_button("Responder"):
                st.session_state[user_answer_key] = user_choice
                st.session_state[card_state_key] = "showing_feedback"
                st.rerun()

    # --- ESTADO: MOSTRANDO FEEDBACK, KARMA Y SRS ---
    elif st.session_state.get(card_state_key) == "showing_feedback":
        respuesta_usuario = st.session_state.get(user_answer_key)
        
        # Mostrar opciones con feedback visual
        for op in display_options:
            if op == pregunta['correcta']:
                st.success(f"**{op} (Correcta)**")
            elif op == respuesta_usuario:
                st.error(f"**{op} (Tu respuesta)**")
            else:
                st.write(op)
        
        st.info(f"**Retroalimentación:**\n{pregunta['retroalimentacion']}")
        st.markdown("---")

        # --- Sub-componente: Botones de Karma ---
        col_karma, col_srs = st.columns([1, 2])
        with col_karma:
            st.write("**Calidad:**")
            
            user_has_voted = has_user_voted(st.session_state.current_user, question_id)
            
            if user_has_voted:
                st.caption("✅ Ya has votado.")
            else:
                k_col1, k_col2 = st.columns(2)
                
                def handle_karma_update(vote_type):
                    conn = None
                    try:
                        conn = get_db_conn()
                        update_karma(conn, st.session_state.current_user, question_id, vote_type)
                        conn.commit()
                    finally:
                        if conn: conn.close()
                    st.rerun()

                if k_col1.button(f"👍 {pregunta['karma']}", key=f"karma_up_{question_id}"):
                    handle_karma_update(1)
                if k_col2.button("👎", key=f"karma_down_{question_id}"):
                    handle_karma_update(-1)

        # --- Sub-componente: Botones SRS ---
        with col_srs:
            st.write("**¿Qué tan difícil fue?**")
            srs_cols = st.columns(3)
            
            def handle_srs_update(difficulty):
                check_rate_limit()
                # --- LOG DE ÉXITO Y SRS ---
                start_time = st.session_state.get(f"timer_start_{question_id}")
                duration = (datetime.datetime.now() - start_time).total_seconds() if start_time else 0
                is_correct = st.session_state.get(user_answer_key) == pregunta['correcta']

                log_event(st.session_state.current_user, "answer_submitted", {
                    "question_id": question_id,
                    "result": "correct" if is_correct else "incorrect",
                    "difficulty_rating": difficulty,
                    "time_seconds": round(duration, 2),
                    "topic": pregunta.get('tag_categoria', 'Unknown')
                })
                
                # Limpiamos el timer para ahorrar memoria
                if f"timer_start_{question_id}" in st.session_state:
                    del st.session_state[f"timer_start_{question_id}"]
                
                # --- FIN DEL BLOQUE DE LOGGING ---

                conn = None
                try:
                    conn = get_db_conn()
                    update_srs(conn, st.session_state.current_user, question_id, difficulty)
                    conn.commit()
                finally:
                    if conn: conn.close()
                st.session_state[card_state_key] = "done"
                
            if srs_cols[0].button("Difícil", key=f"srs_hard_{question_id}"):
                handle_srs_update("difícil")
                next_question_requested = True
            if srs_cols[1].button("Medio", key=f"srs_mid_{question_id}"):
                handle_srs_update("medio")
                next_question_requested = True
            if srs_cols[2].button("Fácil", key=f"srs_easy_{question_id}"):
                handle_srs_update("fácil")
                next_question_requested = True

    return next_question_requested

def show_evaluation_page():
    """
    Página principal de evaluación en Flujo Infinito. Muestra siempre una pregunta
    y utiliza render_question_card para la interacción.
    """
    # --- 1. Lógica de Cabeceras de Modo (Solo para práctica por tema) ---
    if st.session_state.get('practice_mode') and st.session_state.get('selected_tag'):
        st.info(f"📚 Practicando el tema: '{st.session_state.selected_tag}'")
        if st.button("⬅️ Cambiar de Tema"):
            st.session_state.practice_mode = False
            del st.session_state.selected_tag
            st.session_state.current_page = "topics"
            if 'current_eval_question_data' in st.session_state:
                del st.session_state['current_eval_question_data']
            if 'last_displayed_id' in st.session_state:
                del st.session_state['last_displayed_id']
            st.rerun()
        st.markdown("---")

    # --- 2. Gestión de la Pregunta Actual (Flujo Infinito) ---
    if 'current_eval_question_data' not in st.session_state:
        st.session_state.current_eval_question_data = get_next_question_for_user(st.session_state.current_user)

    q_data = st.session_state.current_eval_question_data

    if q_data is None:
        st.warning("No hay preguntas en el sistema. ¡Crea algunas para empezar a estudiar!")
        return

    q_id = q_data['id']
    is_advance = q_data['is_advance']

    # --- 3. Notificación de Transición y Feedback Visual ---
    if is_advance and not st.session_state.get('previous_is_advance', False):
        st.toast('🎉 ¡Meta diaria cumplida! Entrando en Modo Infinito...', icon='🚀')

    if is_advance:
        st.caption("🔵 Modo Adelanto (Bonus FSRS)")
    else:
        st.caption("🔴 Repaso Prioritario / Nuevo")

    # --- 4. Renderizado de la Pregunta ---
    next_requested = render_question_card(q_id)
    
    if next_requested:
        st.session_state.previous_is_advance = is_advance
        del st.session_state.current_eval_question_data
        st.rerun()

def show_topics_page():
    """
    Página de la Biblioteca por Temas. Permite al usuario elegir una CATEGORÍA
    y luego le presenta preguntas de esa categoría una por una.
    """
    st.header("📚 Biblioteca por Categorías")

    conn = get_db_conn()
    # CORRECCIÓN: Se consulta tag_categoria, que tiene los datos limpios.
    query = """
        SELECT tag_categoria as tag, COUNT(*) as total 
        FROM questions 
        WHERE status='active' AND tag_categoria IS NOT NULL AND tag_categoria != '' 
        GROUP BY tag_categoria 
        ORDER BY tag_categoria ASC
    """
    try:
        topics_df = pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Error al consultar las categorías: {e}")
        return
    finally:
        conn.close()

    # CORRECCIÓN: Filtro de basura para eliminar etiquetas cortas/inválidas.
    if not topics_df.empty:
        topics_df = topics_df[topics_df['tag'].str.len() >= 3]

    if topics_df.empty:
        st.info("No hay categorías con preguntas activas para mostrar. ¡Crea algunas preguntas con etiquetas!")
        return

    # --- 1. VISTA DE SELECCIÓN DE CATEGORÍA ---
    
    # Crear un diccionario para el mapeo de formato fácil
    tag_counts = pd.Series(topics_df.total.values, index=topics_df.tag).to_dict()
    topic_list = topics_df['tag'].tolist()

    if 'selected_topic' not in st.session_state:
        st.session_state.selected_topic = None
    
    # --- NUEVO SELECTOR DE CATEGORÍAS (DROPDOWN) ---
    st.markdown("##### 📚 Selecciona tu área de estudio:")
    
    # Usamos selectbox para ahorrar espacio y permitir búsqueda
    selected_tag = st.selectbox(
        label="Categoría", # Ocultamos el label visualmente si usamos el markdown arriba
        options=topic_list,
        # Mantenemos la lógica de índice para recordar la selección previa
        index=topic_list.index(st.session_state.selected_topic) if st.session_state.selected_topic in topic_list else None,
        # Reutilizamos la lambda que ya tenías para mostrar el conteo
        format_func=lambda tag: f"{tag} ({tag_counts.get(tag, 0)} preguntas)",
        key="category_selector",
        label_visibility="collapsed", # Para que se vea más limpio junto al título H5
        placeholder="🔍 Buscar especialidad..."
    )
    
    # Sincronizar estado (Igual que antes)
    if selected_tag != st.session_state.selected_topic:
        st.session_state.selected_topic = selected_tag
        st.rerun() # Recargamos para mostrar las preguntas de la nueva categoría
    # -----------------------------------------------

    st.markdown("---")

    # --- 2. VISTA DE PRÁCTICA ---
    if selected_tag:
        if st.button("⬅️ Cambiar de Categoría"):
            st.session_state.selected_topic = None
            if 'topic_question_id' in st.session_state:
                del st.session_state['topic_question_id']
            st.rerun()

        # Obtener una pregunta para el tema si no hay una activa
        if 'topic_question_id' not in st.session_state:
            conn = get_db_conn()
            # CORRECCIÓN: Se busca por tag_categoria en la práctica.
            question_row = conn.execute(
                "SELECT id FROM questions WHERE tag_categoria = ? AND status = 'active' ORDER BY RANDOM() LIMIT 1",
                (selected_tag,)
            ).fetchone()
            conn.close()
            st.session_state.topic_question_id = question_row['id'] if question_row else None

        q_id = st.session_state.topic_question_id

        if q_id is None:
            st.warning(f"No quedan más preguntas en la categoría '{selected_tag}'. Por favor, elige otra.")
            return

        # Renderizar la pregunta usando el componente central
        next_requested = render_question_card(q_id)
        
        if next_requested:
            del st.session_state.topic_question_id
            st.rerun()

def show_stats_page():
    """Muestra un dashboard analítico con un sistema de clasificación automática."""
    st.header("📊 Dashboard Analítico de la Comunidad")
    
    conn = get_db_conn()

    # Bloque de extracción de datos para el gráfico de Radar.
    # Se ejecuta una consulta para obtener el rendimiento por tema del usuario.
    sql_radar = """
        SELECT
            q.tag_categoria AS tag,
            COUNT(*) as total_preguntas,
            SUM(CASE WHEN p.interval > 3 THEN 1 ELSE 0 END) as preguntas_dominadas
        FROM questions q
        JOIN progress p ON q.id = p.question_id
        WHERE
            q.status = 'active'
            AND q.tag_categoria IS NOT NULL
            AND q.tag_categoria != ''
            AND p.username = ?
        GROUP BY tag
        ORDER BY total_preguntas DESC
        LIMIT 6
    """
    df_radar = pd.read_sql_query(sql_radar, conn, params=(st.session_state.current_user,))

    if not df_radar.empty:
        df_radar['Puntaje'] = (df_radar['preguntas_dominadas'] / df_radar['total_preguntas']) * 100

    if not df_radar.empty:
        st.subheader("🎯 Tu Radar Clínico")
        # Crear el gráfico
        fig = px.line_polar(
            df_radar,
            r='Puntaje',
            theta='tag',
            line_close=True,
            range_r=[0, 100],  # Escala fija de 0 a 100%
        )
        fig.update_traces(fill='toself') # Relleno de color sólido
        # Mostrar en Streamlit
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Responde preguntas de diferentes temas para activar tu Radar Clínico.")
    
    # 1. Extracción de Datos Granulares
    total_questions_global = conn.execute("SELECT COUNT(*) as count FROM questions WHERE status = 'active'").fetchone()['count']
    
    # Query para obtener todos los datos base de usuarios y su progreso
    query = """
        SELECT 
            u.username,
            u.is_resident,
            u.is_reference_model,
            u.total_active_days,
            u.current_streak,
            COALESCE(SUM(p.aciertos), 0) as total_aciertos,
            COALESCE(SUM(p.fallos), 0) as total_fallos,
            COALESCE(SUM(CASE WHEN p.interval > 7 THEN 1 ELSE 0 END), 0) as mastered_count
        FROM 
            users u
        LEFT JOIN 
            progress p ON u.username = p.username
        WHERE
            u.role != 'admin' AND u.status = 'active'
        GROUP BY
            u.username, u.is_resident, u.is_reference_model
    """
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        st.info("No hay datos de progreso de usuarios para mostrar en el ranking.")
        conn.close()
        return

    # 2. Transformación y Cálculo de Métricas
    df['total_answers'] = df['total_aciertos'] + df['total_fallos']
    df['accuracy'] = (df['total_aciertos'] / df['total_answers'] * 100).fillna(0.0)
    df['mastery'] = (df['mastered_count'] / total_questions_global * 100) if total_questions_global > 0 else 0.0

    # --- CALCULAR PROMEDIO DE RESIDENTES ---
    # Filtrar usuarios que son residentes (is_resident == 1)
    resident_data = df[df['is_resident'] == 1]
    
    # Calcular promedio o usar valor por defecto si no hay residentes
    if not resident_data.empty:
        avg_resident_accuracy = resident_data['accuracy'].mean()
    else:
        avg_resident_accuracy = 85.0  # Valor base por defecto
    
    # Mostrar métrica en consola para depuración
    print(f"📊 Promedio Precisión Residentes: {avg_resident_accuracy:.1f}%")

    # 3. Algoritmo de Etiquetado (Clasificación)
    def get_status_label(row, threshold):
        """Asigna una etiqueta de rango al usuario basada en su rendimiento."""
        # Jerarquía Absoluta: Si es el Fantasma/Modelo, es el Residente Supremo.
        if row.get('is_reference_model') == 1:
            return "🎓 Residente"
        
        if row['is_resident'] == 1:
            return "🎓 Residente"
            
        # --- AQUI SIGUE LA LÓGICA EXISTENTE DE PRECISIÓN ---
        if row['accuracy'] >= (threshold * 0.98) and row['total_answers'] > 50:
            return "⭐ Experto"
        if row['accuracy'] < 60.0 or (row['total_answers'] > 20 and row['mastery'] < 10.0):
            return "🚑 En Riesgo"
        return "🦁 Estudiante"

    # Se pasa 'avg_resident_accuracy' como argumento a la función apply.
    df['Estado'] = df.apply(get_status_label, axis=1, args=(avg_resident_accuracy,))

    # --- INICIO: LÓGICA DE ORDENAMIENTO DEL RANKING ---
    # 1. Ordenar: Constancia (Rey) -> Precisión -> Maestría
    df = df.sort_values(by=['total_active_days', 'accuracy', 'mastery'], ascending=[False, False, False])
    # 2. Resetear índice para que empiece en 0 el orden nuevo
    df = df.reset_index(drop=True)
    # 3. Crear columna de Posición (#) basada en el nuevo índice
    df.insert(0, '#', df.index + 1)
    # --- FIN: LÓGICA DE ORDENAMIENTO ---

    # Lógica de Racha para Display
    df['dias_acumulados_display'] = df.apply(
        lambda row: f"🔥 {row['total_active_days']}" if row['current_streak'] >= 3 else f"{row['total_active_days']}",
        axis=1
    )

    # --- INICIO: GRÁFICO COMPARATIVO DE RENDIMIENTO ---
    st.subheader("📈 Tu Rendimiento vs. La Comunidad")

    # 1. Cálculo de Métricas con Pandas
    # Nota: Se usa 'current_user' que es la variable correcta en st.session_state para esta app.
    user_accuracy_row = df[df['username'] == st.session_state.current_user]
    val_tu = user_accuracy_row['accuracy'].iloc[0] if not user_accuracy_row.empty else 0.0
    
    val_comunidad = df['accuracy'].mean()
    # El df ya está ordenado, por lo que .head(10) obtiene los mejores usuarios.
    val_top10 = df.head(10)['accuracy'].mean()

    # --- INICIO: BLOQUE DE DEBUG AUDITORÍA ---
    print("\n" + "="*40)
    print("🕵️‍♂️ AUDITORÍA DE DATOS DEL GRÁFICO")
    print("="*40)
    # 1. Verificar población total
    print(f"👥 Total de Usuarios en DataFrame: {len(df)}")

    # 2. Verificar datos de tu usuario
    # Corregido a 'current_user' y formato de if/else
    user_row_debug = df[df['username'] == st.session_state.current_user]
    if not user_row_debug.empty:
        tu_data = user_row_debug.iloc[0]
        print(f"👤 TÚ ({tu_data['username']}): Constancia={tu_data['total_active_days']} días | Precisión={tu_data['accuracy']:.2f}%")
    else:
        print("👤 TÚ: No encontrado en el ranking.")

    # 3. Verificar el Top 10 seleccionado
    top_10_debug = df.head(10)
    print("\n🏆 TOP 10 SELECCIONADOS (Orden actual):")
    print(top_10_debug[['username', 'total_active_days', 'accuracy', 'mastery']].to_string(index=False))

    # 4. Verificar los promedios matemáticos
    prom_comunidad = df['accuracy'].mean()
    prom_top10 = top_10_debug['accuracy'].mean()
    print(f"\n🧮 CÁLCULOS INTERNOS:")
    print(f"Promedio Comunidad: {prom_comunidad:.4f}%")
    print(f"Promedio Top 10: {prom_top10:.4f}%")
    print("="*40 + "\n")
    # --- FIN: BLOQUE DE DEBUG AUDITORÍA ---
    
    # 2. Preparación del DataFrame para el gráfico
    data_comp = pd.DataFrame({
        'Comparativa': ['Tú', 'Promedio Comunidad', 'Top 10 Expertos'],
        'Precisión': [val_tu, val_comunidad, val_top10],
        'Color': ['#3b82f6', '#9ca3af', '#eab308']  # Azul Vivo, Gris Neutro, Dorado Brillante
    })

    # 3. Visualización (Altair) - Barras + Texto
    bars = alt.Chart(data_comp).mark_bar().encode(
        x=alt.X('Comparativa:N', sort=None, title=None, axis=alt.Axis(labelAngle=0)),
        y=alt.Y('Precisión:Q', title='Precisión (%)', axis=alt.Axis(grid=False)),
        color=alt.Color('Color:N', scale=None, legend=None),
        tooltip=['Comparativa', alt.Tooltip('Precisión', title='Precisión', format='.1f')]
    )

    text = bars.mark_text(
        align='center',
        baseline='bottom',
        dy=-10  # Mueve el texto 10px por encima de la barra
    ).encode(
        text=alt.Text('Precisión:Q', format='.1f')
    )

    chart = (bars + text).configure_view(strokeWidth=0)

    st.altair_chart(chart, use_container_width=True)
    st.markdown("---") # Separador visual antes de la tabla de ranking
    # --- FIN: GRÁFICO COMPARATIVO DE RENDIMIENTO ---

    # 4. Preparación para Visualización
    df_display = df[['#', 'username', 'Estado', 'dias_acumulados_display', 'accuracy', 'mastery', 'total_answers']].copy()
    df_display.rename(columns={
        'username': 'Usuario',
        'accuracy': 'Precisión',
        'mastery': 'Maestría',
        'total_answers': 'Respuestas',
        'dias_acumulados_display': 'Días Acumulados'
    }, inplace=True)

    st.dataframe(
        df_display,
        column_config={
            "#": st.column_config.NumberColumn("Pos.", width="small", format="%d"),
            "Usuario": "Usuario",
            "Estado": "Estado",
            "Días Acumulados": "Días",
            "Precisión": st.column_config.ProgressColumn(
                "Precisión",
                help="Porcentaje de respuestas correctas (Aciertos / Totales).",
                format="%.1f%%", min_value=0, max_value=100,
            ),
            "Maestría": st.column_config.ProgressColumn(
                "Maestría",
                help="Porcentaje de preguntas del sistema dominadas (intervalo > 7 días).",
                format="%.1f%%", min_value=0, max_value=100,
            ),
        },
        use_container_width=True,
        hide_index=True,
        column_order=("#", "Usuario", "Estado", "Días Acumulados", "Precisión", "Maestría", "Respuestas")
    )

    conn.close()

def show_manage_questions_page():
    """Permite gestionar (Editar y Eliminar) preguntas con confirmación de borrado, agrupadas por categoría."""
    if 'editing_question_id' not in st.session_state:
        st.session_state.editing_question_id = None
    
    if 'confirm_delete_id' not in st.session_state:
        st.session_state.confirm_delete_id = None

    is_admin = (st.session_state.user_role == 'admin')
    
    # --- VISTA DE EDICIÓN (TOMA PRIORIDAD) ---
    if st.session_state.editing_question_id is not None:
        q_id = st.session_state.editing_question_id
        st.subheader(f"✏️ Editando Pregunta ID: {q_id}")
        conn = get_db_conn()
        row = conn.execute("SELECT * FROM questions WHERE id = ?", (q_id,)).fetchone()
        conn.close()
        if not row:
            st.error("La pregunta no se encontró.")
            st.session_state.editing_question_id = None
            st.rerun()

        CATEGORIAS_MEDICAS = ["Medicina Interna", "Cirugía General", "Ortopedia", "Urología", "ORL", "Urgencia", "Psiquiatría", "Neurología", "Neurocirugía", "Epidemiología", "Pediatría", "Ginecología", "Oftalmología", "Otra"]
        try:
            cat_index = CATEGORIAS_MEDICAS.index(row['tag_categoria'])
        except (ValueError, TypeError):
            cat_index = None
        
        with st.form("edit_question_form"):
            new_enunciado = st.text_area("Enunciado", value=row['enunciado'])
            ops = row['opciones'].split('|')
            op_a, op_b, op_c, op_d = ops[0], ops[1], ops[2], ops[3]
            op_a = st.text_input("Opción A", value=op_a)
            op_b = st.text_input("Opción B", value=op_b)
            op_c = st.text_input("Opción C", value=op_c)
            op_d = st.text_input("Opción D", value=op_d)
            new_correcta_idx = st.radio("Respuesta Correcta", (0, 1, 2, 3), format_func=lambda x: f"Opción {chr(65+x)}")
            new_retro = st.text_area("Retroalimentación", value=row['retroalimentacion'])
            new_cat = st.selectbox("Categoría", options=CATEGORIAS_MEDICAS, index=cat_index)
            new_tema = st.text_input("Tema", value=row['tag_tema'] or "")
            
            save_btn, cancel_btn = st.columns(2)
            if save_btn.form_submit_button("💾 Guardar Cambios", type="primary"):
                new_opciones = "|".join([op_a, op_b, op_c, op_d])
                correcta_val = [op_a, op_b, op_c, op_d][new_correcta_idx]
                conn = get_db_conn()
                conn.execute("UPDATE questions SET enunciado=?, opciones=?, correcta=?, retroalimentacion=?, tag_categoria=?, tag_tema=? WHERE id=?", (new_enunciado, new_opciones, correcta_val, new_retro, new_cat, new_tema, q_id))
                conn.commit()
                conn.close()
                st.success("Pregunta actualizada.")
                st.session_state.editing_question_id = None
                st.rerun()
            if cancel_btn.form_submit_button("❌ Cancelar"):
                st.session_state.editing_question_id = None
                st.rerun()
        return

    # --- VISTA PRINCIPAL (LISTADO POR CATEGORÍAS) ---
    st.subheader("🔑 Gestionar Preguntas" if is_admin else "📋 Mis Preguntas")
    conn = get_db_conn()
    
    # Query para Admins (trae todo) o Usuarios (solo las suyas)
    if is_admin:
        query = "SELECT id, enunciado, owner_username, status, tag_categoria FROM questions ORDER BY id DESC"
        params = ()
    else:
        query = "SELECT id, enunciado, owner_username, status, tag_categoria FROM questions WHERE owner_username = ? ORDER BY id DESC"
        params = (st.session_state.current_user,)
    
    preguntas = conn.execute(query, params).fetchall()
    conn.close()

    if not preguntas:
        st.info("No hay preguntas registradas.")
    else:
        # 1. Buscador
        search_q = st.text_input("🔍 Buscar en banco de preguntas:", "").lower().strip()

        # 2. Filtrado y Agrupación
        grouped_questions = {}
        for p in preguntas:
            # Filtro de texto
            if search_q and search_q not in p['enunciado'].lower():
                continue

            # Agrupación
            cat = p['tag_categoria'] if p['tag_categoria'] else "General / Sin Etiqueta"
            if cat not in grouped_questions:
                grouped_questions[cat] = []
            grouped_questions[cat].append(p)
            
        # 3. Renderizado por Categorías
        if not grouped_questions:
            st.warning(f"🚫 No se encontraron preguntas que coincidan con '{search_q}'.")
        else:
            for category in sorted(grouped_questions.keys()):
                count = len(grouped_questions[category])
                with st.expander(f"📂 {category} ({count})", expanded=False):
                    for preg in grouped_questions[category]:
                        # --- INICIO DEL CÓDIGO ORIGINAL DE LA TARJETA ---
                        pregunta_id = preg['id']
                        with st.container(border=True):
                            col_main, col_buttons = st.columns([0.8, 0.2])

                            with col_main:
                                col_main.write(preg['enunciado'])
                                
                                if preg['status'] == 'needs_revision':
                                    col_main.warning("⚠️ En Revisión")
                                
                                if is_admin:
                                    col_main.caption(f"Autor: {preg['owner_username']}")

                            if st.session_state.confirm_delete_id == pregunta_id:
                                with col_main:
                                    st.warning("¿Seguro que deseas eliminar esta pregunta?")
                                
                                with col_buttons:
                                    confirm_col1, confirm_col2 = st.columns(2)
                                    
                                    if confirm_col1.button("Sí, eliminar", key=f"confirm_del_{pregunta_id}", type="primary"):
                                        conn = get_db_conn()

                                        # --- SECURITY CHECK (IDOR) ---
                                        # Verificar en DB quién es el dueño real antes de borrar
                                        check_owner = conn.execute("SELECT owner_username FROM questions WHERE id = ?", (pregunta_id,)).fetchone()
                                        if not check_owner:
                                            st.error("La pregunta ya no existe.")
                                            st.stop()
                                            
                                        real_owner = check_owner[0]
                                        current_user = st.session_state.current_user
                                        user_role = st.session_state.user_role
                                        
                                        # Solo pasa si eres el dueño O eres admin
                                        if real_owner != current_user and user_role != 'admin':
                                            st.error("🚨 ALERTA DE SEGURIDAD: Intento de modificación no autorizado detectado.")
                                            # (Opcional) Podríamos loguear esto, pero por ahora detenemos la ejecución.
                                            st.stop()
                                        # --- FIN SECURITY CHECK ---

                                        conn.execute("DELETE FROM questions WHERE id = ?", (pregunta_id,))
                                        conn.commit()
                                        conn.close()
                                        st.success(f"Pregunta {pregunta_id} eliminada.")
                                        st.session_state.confirm_delete_id = None
                                        st.rerun()
                                    
                                    if confirm_col2.button("Cancelar", key=f"cancel_del_{pregunta_id}"):
                                        st.session_state.confirm_delete_id = None
                                        st.rerun()
                            else:
                                with col_buttons:
                                    if st.button("✏️ Editar", key=f"edit_{pregunta_id}"):
                                        st.session_state.editing_question_id = pregunta_id
                                        st.rerun()
                                    
                                    if st.button("🗑️ Eliminar", key=f"del_{pregunta_id}", type="primary"):
                                        st.session_state.confirm_delete_id = pregunta_id
                                        st.rerun()
                        # --- FIN DEL CÓDIGO ORIGINAL DE LA TARJETA ---

# --- INICIO DE SECCIÓN NUEVA: PÁGINA DE DUELOS ---
def play_duel_interface():
    """
    Maneja la interfaz de un duelo, el historial de respuestas y el resumen final.
    """
    duel_id = st.session_state.current_duel_id
    q_idx = st.session_state.duel_question_index
    questions = st.session_state.duel_questions
    
    # --- 1. LÓGICA DE FIN DE DUELO ---
    if q_idx >= len(questions):
        st.success("¡Has completado el duelo!")
        st.balloons()
        
        # --- 2. INICIO: Resumen Detallado de Desempeño ---
        st.subheader("Resumen Detallado de Desempeño")

        if 'duel_history' in st.session_state and st.session_state.duel_history:
            for i, record in enumerate(st.session_state.duel_history):
                enunciado_corto = (record['enunciado'][:60] + '...') if len(record['enunciado']) > 60 else record['enunciado']
                
                # Definir encabezado del expander según el resultado
                if record['is_timeout']:
                    header = f"Pregunta {i+1}: ⏰ Tiempo Agotado - {enunciado_corto}"
                elif record['correct']:
                    header = f"Pregunta {i+1}: ✅ Correcto - {enunciado_corto}"
                else:
                    header = f"Pregunta {i+1}: ❌ Incorrecto - {enunciado_corto}"

                with st.expander(header):
                    st.markdown(f"**Enunciado:** {record['enunciado']}")
                    
                    if record['is_timeout']:
                        st.warning("No se registró respuesta por tiempo.")
                    else:
                        st.write(f"**Tu respuesta:** {record['opcion_elegida']}")

                    st.write(f"**Respuesta Correcta:** {record['opcion_correcta']}")
                    st.markdown("---")
                    st.info(f"**Retroalimentación:**\n\n{record['retroalimentacion']}")
        else:
            st.info("No hay historial de duelo para mostrar.")
        # --- FIN: Resumen Detallado de Desempeño ---

        conn = get_db_conn()
        cursor = conn.cursor()
        current_user = st.session_state.current_user
        score = st.session_state.duel_user_score
        
        duel = cursor.execute("SELECT * FROM duels WHERE id = ?", (duel_id,)).fetchone()
        
        # Actualizar puntaje del usuario actual
        if duel['challenger_username'] == current_user:
            cursor.execute("UPDATE duels SET challenger_score = ? WHERE id = ?", (score, duel_id))
            opponent_finished = duel['opponent_score'] is not None
            opponent_score = duel['opponent_score']
        else: # es oponente
            cursor.execute("UPDATE duels SET opponent_score = ? WHERE id = ?", (score, duel_id))
            opponent_finished = True
            opponent_score = duel['challenger_score']
        conn.commit()

        # --- 3. Anuncio del Ganador (Debajo del resumen) ---
        if opponent_finished:
            user_score = score if score is not None else 0
            opponent_score_val = opponent_score if opponent_score is not None else 0
            is_tie = (user_score == opponent_score_val)

            if user_score > opponent_score_val:
                winner = current_user
            elif opponent_score_val > user_score:
                winner = duel['challenger_username'] if duel['challenger_username'] != current_user else duel['opponent_username']
            else:  # Empate
                winner = duel['challenger_username']  # Empate gana el retador
            
            cursor.execute("UPDATE duels SET status = 'finished', winner = ? WHERE id = ?", (winner, duel_id))
            conn.commit()
            
            st.markdown("---")
            st.subheader("Resultado Final del Duelo")

            if is_tie:
                st.warning(f"🤝 Hubo un empate ({user_score} a {opponent_score_val}). El retador ('{winner}') gana por regla.")
            elif winner == current_user:
                st.success(f"🏆 ¡Ganaste el duelo! Resultado: {user_score} a {opponent_score_val}.")
            else:
                st.error(f"💔 Perdiste el duelo contra '{winner}'. Resultado: {user_score} a {opponent_score_val}.")

        conn.close()
        
        if st.button("Volver a Duelos"):
            del st.session_state.duel_state
            if 'duel_history' in st.session_state:
                del st.session_state.duel_history # Limpiar historial
            st.rerun()
        return

    # --- 4. LÓGICA DE PREGUNTA EN CURSO ---
    if 'duel_question_start_time' not in st.session_state:
        st.session_state.duel_question_start_time = datetime.datetime.now()

    pregunta = questions[q_idx]

    st.warning("⚠️ Tienes 40 segundos. Si respondes tarde, la pregunta contará como fallida.")
    st.subheader(f"Pregunta {q_idx + 1}/{len(questions)}")
    st.markdown(f"### {pregunta['enunciado']}")

    with st.form(f"duel_q_{pregunta['id']}", clear_on_submit=True):
        opciones = pregunta['opciones'].split('|')
        user_choice = st.radio("Elige una respuesta:", options=opciones, key=f"duel_radio_{pregunta['id']}")
        
        if st.form_submit_button("Responder"):
            tiempo_usado = (datetime.datetime.now() - st.session_state.duel_question_start_time).total_seconds()
            
            is_timeout = tiempo_usado > 40
            is_correct = user_choice == pregunta['correcta'] and not is_timeout

            # --- 5. Captura de Datos para el historial ---
            history_record = {
                'enunciado': pregunta['enunciado'],
                'opcion_elegida': user_choice if not is_timeout else "Ninguna (Tiempo Agotado)",
                'opcion_correcta': pregunta['correcta'],
                'retroalimentacion': pregunta['retroalimentacion'],
                'is_timeout': is_timeout,
                'correct': is_correct
            }
            st.session_state.duel_history.append(history_record)

            if is_timeout:
                st.error("¡Tiempo agotado! Te demoraste más de 40 segundos.")
            else:
                if is_correct:
                    st.session_state.duel_user_score += 1
                    st.toast("¡Correcto! ✅")
                else:
                    st.toast("Incorrecto. ❌")
            
            st.session_state.duel_question_index += 1
            del st.session_state.duel_question_start_time
            st.rerun()

def show_duels_page():
    """Página principal de Duelos (PvP Asincrónico), excluyendo al admin de la lógica de juego."""
    st.header("⚔️ Duelos PvP")

    # 1. Identificar al Admin para excluirlo
    try:
        admin_user = st.secrets["ADMIN_USER"]
    except KeyError:
        admin_user = "admin"

    if 'duel_state' not in st.session_state:
        st.session_state.duel_state = 'overview'

    if st.session_state.duel_state == 'playing':
        play_duel_interface()
        return

    # --- VISTA GENERAL DE DUELOS ---
    conn = get_db_conn()
    cursor = conn.cursor()
    current_user = st.session_state.current_user

    # Sección A: Desafiar
    st.subheader("Desafiar a un Oponente")
    if st.button("🤺 Buscar Oponente Aleatorio", use_container_width=True, type="primary"):
        # 2. Modificar consulta para que no seleccione al admin como oponente
        cursor.execute(
            "SELECT username FROM users WHERE username != ? AND username != ? AND is_approved = 1 ORDER BY RANDOM() LIMIT 1",
            (current_user, admin_user)
        )
        opponent = cursor.fetchone()
        
        if not opponent:
            st.warning("No hay otros usuarios disponibles para desafiar.")
        else:
            opponent_username = opponent['username']
            cursor.execute("SELECT id FROM questions ORDER BY RANDOM() LIMIT 5")
            questions = cursor.fetchall()
            if len(questions) < 5:
                st.error("No hay suficientes preguntas en la base de datos para un duelo (se necesitan 5).")
            else:
                question_ids = ",".join([str(q['id']) for q in questions])
                now = datetime.datetime.now()
                
                cursor.execute(
                    "INSERT INTO duels (challenger_username, opponent_username, question_ids, status, created_at) VALUES (?, ?, ?, 'pending', ?)",
                    (current_user, opponent_username, question_ids, now)
                )
                duel_id = cursor.lastrowid
                conn.commit()
                
                # Inicialización del estado del duelo
                st.session_state.duel_state = 'playing'
                st.session_state.current_duel_id = duel_id
                st.session_state.duel_question_index = 0
                st.session_state.duel_user_score = 0
                st.session_state.duel_history = [] # INICIALIZAR HISTORIAL
                st.session_state.duel_questions = [dict(q) for q in conn.execute(f"SELECT * FROM questions WHERE id IN ({question_ids})").fetchall()]
                st.rerun()

    st.markdown("---")

    # Sección B: Duelos Pendientes
    st.subheader("Duelos Pendientes")
    pending_duels = cursor.execute(
        "SELECT * FROM duels WHERE opponent_username = ? AND status = 'pending' ORDER BY created_at DESC",
        (current_user,)
    ).fetchall()

    if not pending_duels:
        st.info("Nadie te ha desafiado... todavía.")
    else:
        for duel in pending_duels:
            with st.container(border=True):
                st.write(f"Has sido desafiado por **{duel['challenger_username']}**.")
                if st.button("🔥 Aceptar Duelo", key=f"accept_{duel['id']}"):
                    question_ids = duel['question_ids']
                    # Inicialización del estado del duelo
                    st.session_state.duel_state = 'playing'
                    st.session_state.current_duel_id = duel['id']
                    st.session_state.duel_question_index = 0
                    st.session_state.duel_user_score = 0
                    st.session_state.duel_history = [] # INICIALIZAR HISTORIAL
                    st.session_state.duel_questions = [dict(q) for q in conn.execute(f"SELECT * FROM questions WHERE id IN ({question_ids})").fetchall()]
                    st.rerun()
    
    st.markdown("---")

    # Sección de Estadísticas y Ranking
    st.subheader("Estadísticas y Ranking de Duelos")
    
    wins = cursor.execute("SELECT COUNT(*) FROM duels WHERE winner = ?", (current_user,)).fetchone()[0]
    losses = cursor.execute("SELECT COUNT(*) FROM duels WHERE winner != ? AND (challenger_username = ? OR opponent_username = ?)", (current_user, current_user, current_user)).fetchone()[0]
    
    col1, col2 = st.columns(2)
    col1.metric("Duelos Ganados", wins)
    col2.metric("Duelos Perdidos", losses)

    # 3. Modificar consulta del ranking para excluir al admin de los resultados
    st.markdown("##### Top Duelistas")
    ranking_df = pd.read_sql_query(
        "SELECT winner as Usuario, COUNT(id) as Victorias FROM duels WHERE winner IS NOT NULL AND winner != ? GROUP BY winner ORDER BY Victorias DESC",
        conn,
        params=(admin_user,)
    )
    if not ranking_df.empty:
        ranking_df.index += 1
        st.dataframe(ranking_df, use_container_width=True)
    else:
        st.info("Aún no hay resultados de duelos para mostrar un ranking.")

    conn.close()
# --- FIN DE SECCIÓN NUEVA ---

def get_user_analytics(username):
    conn = get_db_conn()
    # Traemos los últimos 500 eventos de respuesta
    query = """
        SELECT timestamp, metadata 
        FROM activity_log 
        WHERE username = ? AND action_type = 'answer_submitted' 
        ORDER BY id ASC
    """
    df = pd.read_sql_query(query, conn, params=(username,))
    
    if df.empty:
        return pd.DataFrame()

    # Procesamiento del JSON en metadatos
    parsed_data = []
    for index, row in df.iterrows():
        try:
            meta = json.loads(row['metadata'])
            parsed_data.append({
                'Fecha': pd.to_datetime(row['timestamp']),
                'Velocidad (s)': float(meta.get('time_seconds', 0)),
                'Resultado': meta.get('result', 'unknown'),
                'Dificultad': meta.get('difficulty_rating', 'N/A'),
                'Tema': meta.get('topic', 'General')
            })
        except:
            continue # Saltar filas corruptas
            
    return pd.DataFrame(parsed_data)

class PredictionEngine:
    def __init__(self, current_user_stats):
        # current_user_stats espera: {'precision': float, 'velocidad': float}
        self.user = current_user_stats
        self.ghost = get_ghost_profile()
        
    def calculate_gap(self):
        # Calcula la distancia contra el Fantasma
        if not self.ghost:
            return None 
            
        # 1. Obtener datos del Fantasma (con fallbacks seguros)
        ghost_acc = float(self.ghost.get('final_accuracy_snapshot', 0) or 80.0)
        ghost_speed = float(self.ghost.get('avg_seconds_per_question', 0) or 30.0)
        
        if ghost_speed == 0: ghost_speed = 30.0 # Evitar división por cero
        
        # 2. Comparar
        # Gap de Precisión: (Tu 70% - Ghost 80% = -10)
        acc_gap = self.user.get('precision', 0) - ghost_acc
        
        # Ratio de Velocidad: (Ghost 20s / Tu 40s = 0.5 -> Vas a la mitad de velocidad)
        user_speed = self.user.get('velocidad', 0)
        speed_ratio = ghost_speed / user_speed if user_speed > 0 else 0
        
        return {
            "accuracy_gap": acc_gap, 
            "speed_ratio": speed_ratio,
            "ghost_specialty": self.ghost.get('admitted_specialty', 'General')
        }

def show_admin_panel():
    """Página de gestión de usuarios, moderación, backups y logs."""
    if st.session_state.user_role != 'admin':
        st.error("Acceso denegado."); return
    
    st.header("🔑 Panel de Admin")

    # Initialize session state for confirmations
    if 'admin_pending_action' not in st.session_state:
        st.session_state.admin_pending_action = None
    if 'execution_pending_user' not in st.session_state:
        st.session_state.execution_pending_user = None

    conn = get_db_conn()

    st.markdown("## 🔭 Observatorio de Rendimiento (Consultoría)")
    
    with st.expander("📊 Abrir Panel de Telemetría", expanded=False):
        # 1. Obtener lista de usuarios para el selector
        users_list_df = pd.read_sql_query("SELECT username FROM users", conn)
        all_users = users_list_df['username'].tolist()
        
        if all_users:
            # Selector de Usuario (Por defecto el 'cun' o el primero)
            tgt_user = st.selectbox("Seleccionar Usuario a Espiar:", all_users, index=0)
            
            # 2. Obtener Datos
            df_analytics = get_user_analytics(tgt_user)
            
            if not df_analytics.empty:
                # 3. KPIs
                kpi1, kpi2, kpi3 = st.columns(3)
                avg_speed = df_analytics['Velocidad (s)'].mean()
                accuracy = (df_analytics['Resultado'] == 'correct').mean() * 100
                total_q = len(df_analytics)
                
                kpi1.metric("Velocidad Promedio", f"{avg_speed:.2f} s")
                kpi2.metric("Precisión Actual", f"{accuracy:.1f} %")
                kpi3.metric("Preguntas Analizadas", f"{total_q}")
                
                st.divider()
                st.markdown("#### 🧠 Análisis vs. Fantasma")
                
                # 1. Instanciar el Motor con los datos actuales
                current_stats = {
                    'precision': float(accuracy),
                    'velocidad': float(avg_speed)
                }
                engine = PredictionEngine(current_stats)
                gaps = engine.calculate_gap()
                
                if gaps:
                    # 2. Mostrar la Comparativa Visual
                    c_ghost1, c_ghost2, c_ghost3 = st.columns(3)
                    
                    # Brecha de Precisión
                    gap_acc = gaps['accuracy_gap']
                    c_ghost1.metric(
                        "Brecha de Precisión", 
                        f"{gap_acc:.1f}%", 
                        delta=f"{gap_acc:.1f}%",
                        delta_color="normal" # Verde si es positivo, rojo si es negativo
                    )
                    
                    # Ratio de Velocidad (Mostramos como porcentaje del ideal)
                    speed_pct = gaps['speed_ratio'] * 100
                    c_ghost2.metric(
                        "Ritmo vs Fantasma", 
                        f"{speed_pct:.0f}%",
                        delta=f"{speed_pct - 100:.0f}% (Más lento)" if speed_pct < 100 else "Más rápido",
                        delta_color="normal"
                    )
                    
                    c_ghost3.info(f"Comparando contra: **{gaps['ghost_specialty']}**")
                else:
                    st.warning("⚠️ No se ha configurado un Usuario Fantasma (Referencia) en la BD.")

                # 4. Gráficas
                st.caption("📈 Evolución de Velocidad (Segundos por Pregunta)")
                st.line_chart(df_analytics.set_index('Fecha')['Velocidad (s)'])
                
                st.caption("🎯 Distribución de Resultados")
                res_counts = df_analytics['Resultado'].value_counts()
                st.bar_chart(res_counts)
                
                with st.expander("Ver Datos Crudos"):
                    st.dataframe(df_analytics)

                st.divider()
                st.markdown("#### 🧬 ADN Temático: Tú vs. El Fantasma")
                
                # 1. Obtener datos del Fantasma
                ghost_profile = get_ghost_profile()
                
                if ghost_profile:
                    # Traemos los logs del fantasma
                    df_ghost = get_user_analytics(ghost_profile['username'])
                    
                    if not df_ghost.empty and not df_analytics.empty:
                        # 2. Agrupar por 'Tema' y calcular % de acierto
                        # Usuario Actual
                        user_topic_acc = df_analytics.groupby('Tema').apply(
                            lambda x: (x['Resultado'] == 'correct').mean() * 100
                        ).rename("Usuario")
                        
                        # Fantasma
                        ghost_topic_acc = df_ghost.groupby('Tema').apply(
                            lambda x: (x['Resultado'] == 'correct').mean() * 100
                        ).rename("Fantasma")
                        
                        # 3. Unir (Merge) para comparar
                        # fillna(0) es importante por si el usuario no ha visto un tema que el fantasma sí
                        comparison_df = pd.concat([user_topic_acc, ghost_topic_acc], axis=1).fillna(0)
                        
                        # 4. Visualización
                        st.bar_chart(comparison_df)
                        
                        # 5. Detector de Brechas Críticas
                        # Buscamos temas donde el Fantasma sabe (>60%) y tú fallas (Brecha > 20%)
                        comparison_df['Brecha'] = comparison_df['Usuario'] - comparison_df['Fantasma']
                        critical = comparison_df[
                            (comparison_df['Fantasma'] > 60) & 
                            (comparison_df['Brecha'] < -20)
                        ]
                        
                        if not critical.empty:
                            st.error("🚨 ALERTA: El Fantasma domina estos temas y tú no:")
                            for topic, row in critical.iterrows():
                                diff = abs(row['Brecha'])
                                st.write(f"- **{topic}**: Estás {diff:.1f}% por debajo del nivel de referencia.")
                    else:
                        st.info("Aún no hay suficientes datos coincidentes entre ambos usuarios para comparar temas.")
                else:
                    st.warning("⚠️ No hay Fantasma configurado.")
            else:
                st.info(f"El usuario {tgt_user} aún no tiene telemetría registrada (Eventos 'answer_submitted').")
        else:
            st.warning("No hay usuarios en la base de datos.")
    
    st.markdown("---") # Separador antes de la lista de gestión de usuarios

    try:
        admin_user = st.secrets["ADMIN_USER"]
    except (KeyError, FileNotFoundError):
        admin_user = "admin" 
    
    # --- 1. SECCIÓN: GESTIONAR USUARIOS ACTIVOS ---
    
    usuarios_activos = conn.execute(
        "SELECT username, role, is_approved, is_intensive, max_inactivity_days, status, is_reference_model, admitted_status, admitted_specialty, final_accuracy_snapshot, avg_daily_questions, avg_seconds_per_question, total_questions_snapshot FROM users WHERE username != ? AND status = 'active'", 
        (admin_user,)
    ).fetchall()

    if not usuarios_activos:
        st.info("No hay usuarios activos para gestionar.")
    else:
        st.markdown("### 👥 Gestión de Accesos")
    
        with st.expander("📂 Ver / Buscar Usuarios Activos", expanded=True):
            # 1. Buscador
            search_query = st.text_input("🔍 Buscar por nombre de usuario:", "").lower().strip()
            
            # 2. Filtrado seguro
            # Si hay texto, filtramos. Si no, mostramos todos.
            if search_query:
                filtered_users = [u for u in usuarios_activos if search_query in u['username'].lower()]
            else:
                filtered_users = usuarios_activos
                
            # 3. Contador de resultados
            if search_query:
                st.caption(f"Encontrados: {len(filtered_users)} de {len(usuarios_activos)}")
            
            # 4. Bucle sobre la lista FILTRADA
            if filtered_users:
                for user_row in filtered_users:
                    username = user_row['username']
                    is_approved = user_row['is_approved']
                    
                    st.markdown("---")
                    col1, col2, col3 = st.columns([2, 1, 1.5])
                    
                    with col1:
                        st.markdown(f"**{username}** ({user_row['role']})")
                        status_text = "🔥 Activo" if user_row['is_intensive'] else "Inactivo"
                        st.caption(f"Modo Intensivo: {status_text}")

                    with col2:
                        st.write("✅ Aprobado" if user_row['is_approved'] else "⏳ Pendiente")

                    with col3:
                        pending_action = st.session_state.admin_pending_action
                        if pending_action and pending_action['username'] == username:
                            action_text_map = {'aprobar': 'aprobar', 'revocar': 'revocar la aprobación', 'eliminar': 'eliminar'}
                            action_text = action_text_map.get(pending_action['action'], 'realizar esta acción')
                            st.warning(f"¿Seguro que deseas {action_text} a **{username}**?")
                            
                            confirm_col, cancel_col = st.columns(2)
                            if confirm_col.button("✅ Sí, confirmar", key=f"confirm_{username}", type="primary"):
                                action = pending_action['action']
                                if action == 'aprobar':
                                    conn.execute("UPDATE users SET is_approved = 1 WHERE username = ?", (username,))
                                    conn.commit()
                                    st.success(f"Usuario {username} aprobado.")
                                elif action == 'revocar':
                                    conn.execute("UPDATE users SET is_approved = 0 WHERE username = ?", (username,))
                                    conn.commit()
                                    st.success(f"Aprobación de {username} revocada.")
                                elif action == 'eliminar':
                                    delete_user_from_db(username)
                                
                                st.session_state.admin_pending_action = None
                                st.rerun()

                            if cancel_col.button("❌ Cancelar", key=f"cancel_{username}"):
                                st.session_state.admin_pending_action = None
                                st.rerun()
                        else:
                            if is_approved == 0:
                                if st.button("Aprobar", key=f"approve_{username}"):
                                    st.session_state.admin_pending_action = {'username': username, 'action': 'aprobar'}
                                    st.rerun()
                            else:
                                if st.button("Revocar", key=f"revoke_{username}", type="secondary"):
                                    st.session_state.admin_pending_action = {'username': username, 'action': 'revocar'}
                                    st.rerun()
                            if st.button("Eliminar ⚠️", key=f"del_{username}"):
                                st.session_state.admin_pending_action = {'username': username, 'action': 'eliminar'}
                                st.rerun()

                    with st.expander('⚙️ Configurar Modo Intensivo'):
                        with st.form(key=f"intensive_form_{username}"):
                            intensive_active = st.checkbox('Activar Modo Intensivo', value=bool(user_row['is_intensive']))
                            inactivity_days = st.number_input('Días Máximos de Inactividad', min_value=1, max_value=30, value=user_row['max_inactivity_days'])
                            
                            if st.form_submit_button('Guardar Configuración'):
                                new_is_intensive = 1 if intensive_active else 0
                                
                                # --- INICIO DE LA CORRECCIÓN LÓGICA ---
                                # Si se está activando el modo intensivo, guardar la fecha de inicio.
                                # Si se desactiva, se limpia la fecha.
                                if new_is_intensive == 1 and not user_row['is_intensive']:
                                    # Se está activando AHORA
                                    start_date = datetime.date.today()
                                    conn.execute("UPDATE users SET is_intensive = ?, max_inactivity_days = ?, intensive_start_date = ? WHERE username = ?", (new_is_intensive, inactivity_days, start_date, username))
                                elif new_is_intensive == 0 and user_row['is_intensive']:
                                    # Se está desactivando AHORA
                                    conn.execute("UPDATE users SET is_intensive = ?, intensive_start_date = NULL WHERE username = ?", (new_is_intensive, username))
                                else:
                                    # Solo se actualizan los días, sin cambiar estado o fecha
                                    conn.execute("UPDATE users SET max_inactivity_days = ? WHERE username = ?", (inactivity_days, username))
                                # --- FIN DE LA CORRECCIÓN LÓGICA ---
                                
                                conn.commit()
                                st.success(f"Configuración de Modo Intensivo guardada para {username}.")
                                st.rerun()

                    with st.expander('👻 Configuración de Modelo / Fantasma'):
                        with st.form(key=f"ghost_form_{username}"):
                            st.markdown("##### 🧬 Perfil del Experto (Reference Model)")
                            # Fila 1: Estatus y Especialidad (Datos cualitativos)
                            c1, c2, c3 = st.columns(3)

                            # Asumimos que user_row tiene las claves correctas tras actualizar el SELECT
                            new_is_ref = c1.checkbox("Es Modelo Referencia", value=bool(user_row['is_reference_model']), key=f"ref_{user_row['username']}")

                            # Lógica para index del selectbox
                            current_status = user_row['admitted_status'] if user_row['admitted_status'] in ["No Admitido", "Admitido", "Pending"] else "Pending"
                            status_opts = ["Pending", "No Admitido", "Admitido"]
                            new_status = c2.selectbox("Estatus", status_opts, index=status_opts.index(current_status), key=f"stat_{user_row['username']}")

                            new_specialty = c3.text_input("Especialidad Objetivo/Lograda", value=user_row['admitted_specialty'] or "", key=f"spec_{user_row['username']}")

                            st.divider()
                            st.caption("📊 Métricas de Hábito (Se llenarán automáticamente tras el estudio o puedes editar):")

                            # Fila 2: Métricas Cuantitativas
                            c4, c5 = st.columns(2)
                            new_acc = c4.number_input("Precisión Global (%)", value=float(user_row['final_accuracy_snapshot'] or 0.0), key=f"acc_{user_row['username']}")
                            new_speed = c5.number_input("Velocidad (Seg/Pregunta)", value=float(user_row['avg_seconds_per_question'] or 0.0), key=f"spd_{user_row['username']}")

                            c6, c7 = st.columns(2)
                            new_daily = c6.number_input("Promedio Diario (Preg/Día)", value=float(user_row['avg_daily_questions'] or 0.0), key=f"dly_{user_row['username']}")
                            new_total = c7.number_input("Total Histórico", value=int(user_row['total_questions_snapshot'] or 0), key=f"tot_{user_row['username']}")

                            if st.form_submit_button('Guardar Rol Fantasma'):
                                conn.execute(
                                    """UPDATE users SET 
                                        is_reference_model=?, admitted_status=?, admitted_specialty=?, 
                                        final_accuracy_snapshot=?, avg_daily_questions=?, avg_seconds_per_question=?, 
                                        total_questions_snapshot=? 
                                       WHERE username=?""",
                                    (1 if new_is_ref else 0, new_status, new_specialty, new_acc, new_daily, new_speed, new_total, username)
                                )
                                conn.commit()
                                st.success(f"Configuración de Modelo de Referencia guardada para {username}.")
                                st.rerun()
            else:
                st.info(f"🚫 No se encontraron usuarios que coincidan con '{search_query}'.")

    # --- 2. SECCIÓN: ZONA DE JUICIO ---
    st.markdown("---")
    pending_deletion_users = conn.execute("SELECT * FROM users WHERE status = 'pending_delete'").fetchall()

    with st.expander("💀 Zona de Juicio (Pendientes de Eliminación)", expanded=False):
        if not pending_deletion_users:
            st.info("No hay usuarios pendientes de eliminación.")
        else:
            search_juicio = st.text_input("🔍 Buscar condenado:", "", key="search_juicio").lower()
            
            # Filtrar lista
            filtered_pending = [u for u in pending_deletion_users if search_juicio in u['username'].lower()]
            
            if filtered_pending:
                for user_row in filtered_pending:
                    username = user_row['username']
                    st.markdown("---")
                    
                    score, _, _ = calculate_user_score(username, user_row['max_inactivity_days'])
                    reason = f"Puntaje de productividad bajo ({score}/30)"
                    
                    container = st.container(border=True)
                    container.error(f"**Usuario:** {username}\n\n**Motivo:** {reason}")
                    
                    if st.session_state.execution_pending_user == username:
                        container.warning(f"¿Seguro que deseas ELIMINAR PERMANENTEMENTE a {username}?")
                        exec_col, cancel_exec_col = container.columns(2)
                        
                        if exec_col.button("✅ Sí, ejecutar", key=f"exec_confirm_{username}", type="primary"):
                            conn.execute("INSERT INTO deleted_users_log (username, deletion_date, reason) VALUES (?, ?, ?)", (username, datetime.datetime.now(), reason))
                            conn.commit()
                            delete_user_from_db(username)
                            st.session_state.execution_pending_user = None
                            st.success(f"El usuario {username} ha sido ejecutado.")
                            st.rerun()

                        if cancel_exec_col.button("❌ No, cancelar ejecución", key=f"exec_cancel_{username}"):
                            st.session_state.execution_pending_user = None
                            st.rerun()
                    else:
                        pardon_col, execute_col = container.columns(2)
                        if pardon_col.button("Indultar (Perdonar)", key=f"pardon_{username}"):
                            conn.execute("UPDATE users SET status = 'active' WHERE username = ?", (username,))
                            conn.execute("INSERT INTO activity_log (username, action_type, timestamp) VALUES (?, 'pardoned', ?)", (username, datetime.datetime.now()))
                            conn.commit()
                            st.success(f"{username} ha sido indultado y su cuenta ha sido reactivada.")
                            st.rerun()

                        if execute_col.button("Ejecutar (Eliminar)", key=f"execute_{username}", type="primary"):
                            st.session_state.execution_pending_user = username
                            st.rerun()
            else:
                st.warning("No se encontraron coincidencias.")

    # --- 3. SECCIÓN: HISTORIAL DE ELIMINADOS ---
    st.markdown("---")
    deleted_log_df = pd.read_sql_query("SELECT username, deletion_date, reason FROM deleted_users_log ORDER BY deletion_date DESC", conn)

    with st.expander("🪵 Historial de Eliminados (Cementerio)", expanded=False):
        # Asumiendo que deleted_log_df ya está creado antes de esto
        if deleted_log_df.empty:
            st.info("El cementerio está vacío.")
        else:
            search_hist = st.text_input("🔍 Buscar en historial:", "", key="search_hist")
            
            if search_hist:
                # Filtro simple: busca el texto en la columna username
                # (Asegúrate de que la columna exista, si no, filtra sobre todo el DF)
                try:
                    filtered_df = deleted_log_df[deleted_log_df['username'].astype(str).str.contains(search_hist, case=False, na=False)]
                    st.dataframe(filtered_df, use_container_width=True)
                except:
                    st.dataframe(deleted_log_df, use_container_width=True) # Fallback si falla el filtro
            else:
                st.dataframe(deleted_log_df, use_container_width=True)

    conn.close()

    # --- INICIO DE LA NUEVA SECCIÓN DE BACKUP ---
    st.markdown("---")
    st.subheader("📦 Copia de Seguridad (Backup)")

    try:
        with open(DB_FILE, "rb") as fp:
            st.download_button(
                label="Descargar Base de Datos (SQLite)",
                data=fp,
                file_name=f"backup_prisma_srs_{datetime.date.today().strftime('%Y-%m-%d')}.db",
                mime="application/x-sqlite3"
            )
        st.info("Este archivo contiene todos los datos de usuarios y preguntas. Guárdalo en un lugar seguro.")
    except FileNotFoundError:
        st.error(f"Error: No se encontró el archivo de la base de datos en la ruta: {DB_FILE}")
    except Exception as e:
        st.error(f"Ocurrió un error inesperado al leer el archivo de la base de datos: {e}")
    # --- FIN DE LA NUEVA SECCIÓN DE BACKUP ---

    # --- INICIO DE EXPORTACIÓN DE DATOS PARA ANÁLISIS ---
    st.markdown("---")
    st.subheader("📊 Exportar Data para Análisis")

    @st.cache_data
    def generate_excel_export():
        """
        Genera un archivo Excel con los datos del sistema y lo devuelve como bytes.
        Usa cache para no regenerar el archivo en cada rerun.
        """
        output = io.BytesIO()
        conn_export = get_db_conn()
        try:
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # --- Hoja 1: Usuarios ---
                df_users = pd.read_sql_query("SELECT * FROM users", conn_export)
                if 'password_hash' in df_users.columns:
                    df_users = df_users.drop(columns=['password_hash'])
                df_users.to_excel(writer, sheet_name='Usuarios', index=False)

                # --- Hoja 2: Telemetría (NUEVA) ---
                df_logs = pd.read_sql_query("SELECT * FROM activity_log", conn_export)

                if not df_logs.empty and 'metadata' in df_logs.columns:
                    def safe_json_load(x):
                        """Intenta cargar un JSON, si falla devuelve un diccionario vacío."""
                        try:
                            # Asegurarse que el dato no es nulo y es un string
                            if x and isinstance(x, str):
                                return json.loads(x)
                        except (json.JSONDecodeError, TypeError):
                            pass # Ignora el error y retorna el dict vacío
                        return {}

                    # Normaliza la columna 'metadata' en un nuevo DataFrame
                    # .apply(safe_json_load) asegura que no falle con JSONs corruptos/vacíos
                    df_meta = pd.json_normalize(df_logs['metadata'].apply(safe_json_load))

                    # Une los datos normalizados de vuelta al DataFrame original
                    df_logs = df_logs.join(df_meta)

                    # Renombrar columnas para mayor claridad en el Excel
                    rename_map = {
                        'time_seconds': 'Velocidad (s)',
                        'topic': 'Tema',
                        'result': 'Resultado',
                        'difficulty_rating': 'Dificultad'
                    }
                    
                    # Renombrar solo las columnas que existan para evitar errores
                    existing_renames = {k: v for k, v in rename_map.items() if k in df_logs.columns}
                    if existing_renames:
                        df_logs.rename(columns=existing_renames, inplace=True)
                    
                    # Eliminar la columna de metadatos original que ya no es necesaria
                    if 'metadata' in df_logs.columns:
                        df_logs.drop(columns=['metadata'], inplace=True)
                
                # Escribir el DataFrame procesado a la hoja de Excel
                df_logs.to_excel(writer, sheet_name='Telemetría', index=False)

        finally:
            conn_export.close()
        
        output.seek(0)
        return output.getvalue()

    try:
        # El decorador @st.cache_data se encargará de la eficiencia
        excel_data = generate_excel_export()
        
        st.download_button(
            label="Descargar Dataset Completo (.xlsx)",
            data=excel_data,
            file_name=f"dataset_k_community_{datetime.date.today().strftime('%Y-%m-%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        st.error(f"Ocurrió un error al generar el dataset para descarga: {e}")
    # --- FIN DE EXPORTACIÓN DE DATOS PARA ANÁLISIS ---

def show_change_password_page():
    """Permite al usuario logueado cambiar su propia contraseña."""
    st.subheader("🔐 Cambiar Mi Contraseña")
    with st.form("change_password_form", clear_on_submit=True):
        password_new = st.text_input("Nueva Contraseña", type="password")
        password_confirm = st.text_input("Confirmar Nueva Contraseña", type="password")
        if st.form_submit_button("Actualizar Contraseña"):
            if password_new and password_new == password_confirm:
                password_new_bytes = password_new.encode('utf-8')[:72]
                new_hash = pwd_context.hash(password_new_bytes)
                conn = get_db_conn()
                conn.execute("UPDATE users SET password_hash = ? WHERE username = ?", (new_hash, st.session_state.current_user))
                conn.commit(); conn.close()
                st.success("¡Contraseña actualizada con éxito!"); st.balloons()
            else:
                st.error("Las contraseñas no coinciden o están vacías.")

# --- CONTROLADOR PRINCIPAL (MAIN) ---

def main():
    """Función principal que actúa como enrutador."""
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
        st.session_state.current_user = None
        st.session_state.user_role = None
        st.session_state.current_page = "login"

    if not st.session_state.logged_in:
        show_login_page()
    else:
        st.sidebar.title(f"Bienvenido, {st.session_state.current_user}")
        st.sidebar.caption(f"Rol: {st.session_state.user_role}")

        # --- INICIO SECCIÓN MODO INTENSIVO: Widget de Productividad ---
        show_productivity_widget()
        # --- FIN SECCIÓN MODO INTENSIVO ---

        st.sidebar.markdown("---")
        
        # Navegación
        if st.sidebar.button("🧠 Iniciar Evaluación", use_container_width=True):
            st.session_state.current_page = "evaluacion"; reset_evaluation_state(); st.rerun()
        if st.sidebar.button("📚 Biblioteca por Temas", use_container_width=True):
            st.session_state.current_page = "topics"; reset_evaluation_state(); st.rerun()
        if st.sidebar.button("⚔️ Duelos", use_container_width=True):
            st.session_state.current_page = "duelos"; st.rerun()
        if st.sidebar.button("🖊️ Crear Preguntas", use_container_width=True):
            st.session_state.current_page = "crear"; st.rerun()
        if st.sidebar.button("📋 Gestionar Mis Preguntas", use_container_width=True):
            st.session_state.current_page = "gestionar"; st.rerun()
        if st.sidebar.button("📊 Estadísticas y Ranking", use_container_width=True):
            st.session_state.current_page = "estadisticas"; st.rerun()
            
        if st.session_state.user_role == 'admin':
            st.sidebar.markdown("---"); st.sidebar.markdown("Panel de Administrador")
            if st.sidebar.button("🔑 Gestionar Usuarios", use_container_width=True):
                st.session_state.current_page = "admin_users"; st.rerun()

        st.sidebar.markdown("---")
        if st.sidebar.button("📜 Reglamento / Ayuda", use_container_width=True):
            st.session_state.current_page = "rules"; st.rerun()
        if st.sidebar.button("🔐 Cambiar Contraseña", use_container_width=True):
            st.session_state.current_page = "change_password"; st.rerun()
        if st.sidebar.button("Cerrar Sesión", use_container_width=True):
            for key in list(st.session_state.keys()): del st.session_state[key]
            st.rerun()

        page_functions = {
            "evaluacion": show_evaluation_page,
            "topics": show_topics_page,
            "crear": show_create_page,
            "gestionar": show_manage_questions_page,
            "estadisticas": show_stats_page,
            "duelos": show_duels_page,
            "admin_users": show_admin_panel,
            "change_password": show_change_password_page,
            "rules": show_rules_page,
        }
        
        page_to_show = page_functions.get(st.session_state.get("current_page", "evaluacion"), show_evaluation_page)
        page_to_show()

# --- EJECUCIÓN ---
if __name__ == "__main__":
    # --- INICIO: Ejecución de Tareas de Arranque ---
    if 'backup_done' not in st.session_state:
        run_auto_backup()
        st.session_state.backup_done = True
    # --- FIN: Tareas de Arranque ---
    setup_database()
    main()
