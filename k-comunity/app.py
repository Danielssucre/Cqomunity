import streamlit as st
import sqlite3
import pandas as pd
import datetime
import os
import time
import json
import io
import altair as alt
from passlib.context import CryptContext  # Para hashing de contraseñas

# --- CONFIGURACIÓN DE PÁGINA Y SEGURIDAD ---
st.set_page_config(
    page_title="Plataforma de Estudio SRS",
    page_icon="🧠",
    layout="wide"
)

# Contexto para hashear contraseñas
pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")
# --- CONFIGURACIÓN DE RUTAS (Local vs Render) ---
# Si existe la carpeta de Render, úsala. Si no, usa la carpeta local.
if os.path.exists("/opt/render/data"):
    DB_FILE = "/opt/render/data/prisma_srs.db"
else:
    DB_FILE = "prisma_srs.db"

# --- FUNCIONES DE BASE DE DATOS (SQLite) ---

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

def setup_database():
    """Crea las tablas, actualiza la estructura y asegura que el admin exista y esté aprobado."""
    conn = get_db_conn()
    cursor = conn.cursor()
    
    # --- Creación de Tablas (Sin cambios) ---
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'user'
    );
    """
    )
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS questions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        owner_username TEXT NOT NULL REFERENCES users(username),
        enunciado TEXT NOT NULL,
        opciones TEXT NOT NULL,
        correcta TEXT NOT NULL,
        retroalimentacion TEXT NOT NULL,
        tag_categoria TEXT,
        tag_tema TEXT
    );
    """
    )
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS progress (
        username TEXT NOT NULL REFERENCES users(username),
        question_id INTEGER NOT NULL REFERENCES questions(id) ON DELETE CASCADE,
        due_date DATE NOT NULL,
        interval INTEGER NOT NULL DEFAULT 1,
        aciertos INTEGER NOT NULL DEFAULT 0,
        fallos INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (username, question_id)
    );
    """
    )
    # --- INICIO DE SECCIÓN NUEVA (Tabla de Duelos) ---
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS duels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        challenger_username TEXT NOT NULL REFERENCES users(username),
        opponent_username TEXT NOT NULL REFERENCES users(username),
        question_ids TEXT NOT NULL,
        challenger_score INTEGER,
        opponent_score INTEGER,
        status TEXT NOT NULL, -- 'pending', 'finished'
        winner TEXT,
        created_at DATETIME NOT NULL
    );
    """)
    # --- FIN DE SECCIÓN NUEVA ---

    # --- INICIO SECCIÓN MODO INTENSIVO ---
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS activity_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        action_type TEXT NOT NULL,
        timestamp DATETIME NOT NULL
    );
    """)
    # --- FIN SECCIÓN MODO INTENSIVO ---

    # --- INICIO GUILLOTINA CONTROLADA ---
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS deleted_users_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        deletion_date DATETIME NOT NULL,
        reason TEXT
    );
    """)
    # --- FIN GUILLOTINA CONTROLADA ---

    # --- INICIO SISTEMA DE VOTOS ---
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS question_votes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_username TEXT NOT NULL REFERENCES users(username),
        question_id INTEGER NOT NULL REFERENCES questions(id),
        vote_type INTEGER NOT NULL,
        timestamp DATETIME NOT NULL
    );
    """)
    cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_user_question_vote
    ON question_votes (user_username, question_id);
    """)
    # --- FIN SISTEMA DE VOTOS ---

    # Verificar y añadir columnas a 'users'
    cursor.execute("PRAGMA table_info(users)")
    existing_user_columns = [col[1] for col in cursor.fetchall()]
    
    if 'is_approved' not in existing_user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN is_approved INTEGER NOT NULL DEFAULT 0")
    if 'is_intensive' not in existing_user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN is_intensive INTEGER NOT NULL DEFAULT 0")
    if 'max_inactivity_days' not in existing_user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN max_inactivity_days INTEGER NOT NULL DEFAULT 3")
    if 'status' not in existing_user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN status TEXT NOT NULL DEFAULT 'active'")
    if 'is_resident' not in existing_user_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN is_resident INTEGER NOT NULL DEFAULT 0")

    # Verificar y añadir columnas a 'questions'
    cursor.execute("PRAGMA table_info(questions)")
    existing_question_columns = [col[1] for col in cursor.fetchall()]
    if 'status' not in existing_question_columns:
        cursor.execute("ALTER TABLE questions ADD COLUMN status TEXT NOT NULL DEFAULT 'active'")

    # --- Admin por Defecto (Actualizado) ---
    try:
        ADMIN_USER_DEFAULT = st.secrets["ADMIN_USER"]
        ADMIN_PASS_DEFAULT = st.secrets["ADMIN_PASS"]
    except KeyError:
        st.error("Error: Faltan ADMIN_USER o ADMIN_PASS en los secretos de Render.")
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
        cursor.execute("UPDATE users SET is_approved = 1 WHERE username = ?", (ADMIN_USER_DEFAULT,))

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

# --- PÁGINAS DE LA APLICACIÓN ---

# --- INICIO SECCIÓN DE FEATURES: Votos y Modo Intensivo ---

def cast_vote(username, question_id, vote_type):
    """Registra o actualiza el voto de un usuario y activa la guillotina si es necesario."""
    conn = get_db_conn()
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

    conn.commit()
    conn.close()

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

def calculate_user_score(username, days_limit):
    """Calcula el puntaje de actividad de un usuario en un período determinado."""
    conn = get_db_conn()
    cursor = conn.cursor()
    
    start_date = datetime.datetime.now() - datetime.timedelta(days=days_limit)
    
    cursor.execute(
        "SELECT action_type, COUNT(*) as count FROM activity_log WHERE username = ? AND timestamp >= ? GROUP BY action_type",
        (username, start_date)
    )
    
    activities = cursor.fetchall()
    conn.close()
    
    num_creadas = 0
    num_respuestas = 0
    
    for activity in activities:
        if activity['action_type'] == 'create':
            num_creadas = activity['count']
        elif activity['action_type'] == 'answer':
            num_respuestas = activity['count']
            
    score = (num_creadas * 2) + (num_respuestas * 1)
    
    return score, num_creadas, num_respuestas

def show_productivity_widget():
    """Muestra un widget de productividad en la barra lateral para usuarios en modo intensivo."""
    conn = get_db_conn()
    user_settings = conn.execute(
        "SELECT is_intensive, max_inactivity_days FROM users WHERE username = ?",
        (st.session_state.current_user,)
    ).fetchone()
    conn.close()

    if user_settings and user_settings['is_intensive']:
        days_limit = user_settings['max_inactivity_days']
        score, num_creadas, num_respuestas = calculate_user_score(st.session_state.current_user, days_limit)
        
        st.sidebar.markdown("---")
        st.sidebar.subheader("🔥 Modo Intensivo Activo")
        
        # El 100% de la barra son 30 puntos
        progress_value = min(score, 30) / 30.0
        st.sidebar.progress(progress_value)
        
        st.sidebar.metric(label=f"Cuota ({days_limit} días)", value=f"{score} / 30 Pts")

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

def show_login_page():
    """Muestra login (con chequeo de aprobación), registro y bloqueo por inactividad."""
    st.subheader("Inicio de Sesión")
    
    with st.form("login_form"):
        username = st.text_input("Nombre de usuario")
        password = st.text_input("Contraseña", type="password")
        login_submitted = st.form_submit_button("Ingresar")

        if login_submitted:
            conn = get_db_conn()
            user = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
            
            if user and verify_password(password, user['password_hash']):
                
                # --- INICIO GUILLOTINA CONTROLADA: Lógica de Bloqueo ---
                
                # 1. Check if already pending deletion
                if user['status'] == 'pending_delete':
                    st.error("Cuenta bloqueada por incumplimiento. Contacta al administrador.")
                    conn.close()
                    return

                # 2. Check if user is intensive and fails the rules
                if user['is_intensive']:
                    score, _, _ = calculate_user_score(username, user['max_inactivity_days'])
                    
                    last_activity_row = conn.execute(
                        "SELECT MAX(timestamp) as last_ts FROM activity_log WHERE username = ?", (username,)
                    ).fetchone()
                    
                    is_inactive = False
                    if last_activity_row and last_activity_row['last_ts']:
                        last_activity_date = datetime.datetime.fromisoformat(last_activity_row['last_ts'])
                        if (datetime.datetime.now() - last_activity_date).days > user['max_inactivity_days']:
                            is_inactive = True
                    else: # No activity ever logged means they are inactive
                        is_inactive = True

                    if score < 30 or is_inactive:
                        conn.execute("UPDATE users SET status = 'pending_delete' WHERE username = ?", (username,))
                        conn.commit()
                        st.error("Cuenta bloqueada por incumplimiento. Contacta al administrador.")
                        conn.close()
                        return
                # --- FIN GUILLOTINA CONTROLADA ---

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
                st.error("Usuario o contraseña incorrectos.")
            
            # Ensure connection is closed if login fails early
            if conn:
                conn.close()

    st.subheader("Registro (Nuevos Usuarios)")
    with st.form("register_form", clear_on_submit=True):
        new_username = st.text_input("Nuevo nombre de usuario")
        new_password = st.text_input("Nueva contraseña", type="password")
        reg_submitted = st.form_submit_button("Registrarse")

        if reg_submitted:
            if not new_username or not new_password:
                st.warning("Usuario y contraseña no pueden estar vacíos.")
            elif new_username == st.secrets["ADMIN_USER"]:
                 st.error("Nombre de usuario no disponible.")
            else:
                try:
                    password_new_bytes = new_password.encode('utf-8')[:72]
                    hashed_pass = pwd_context.hash(password_new_bytes)
                    conn = get_db_conn()
                    cursor = conn.cursor()
                    cursor.execute(
                        "INSERT INTO users (username, password_hash, role) VALUES (?, ?, 'user')",
                        (new_username, hashed_pass)
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
                
                conn.commit()
                conn.close()
                st.success("¡Pregunta guardada con éxito!")

def get_next_question_for_user(username, practice_mode=False):
    """Obtiene la próxima pregunta para el usuario, filtrando por estado 'active'."""
    conn = get_db_conn()
    cursor = conn.cursor()

    if practice_mode:
        # Modificado para filtrar por status
        cursor.execute("SELECT id FROM questions WHERE status = 'active' ORDER BY RANDOM() LIMIT 1")
        practice_question = cursor.fetchone()
        conn.close()
        return practice_question['id'] if practice_question else None

    today = datetime.date.today()
    # Modificado para filtrar por status
    cursor.execute("""
        SELECT q.id 
        FROM questions q 
        JOIN progress p ON q.id = p.question_id 
        WHERE p.username = ? AND p.due_date <= ? AND q.status = 'active'
        ORDER BY RANDOM() LIMIT 1
    """, (username, today))
    due_question = cursor.fetchone()
    
    if due_question:
        conn.close()
        return due_question['id']

    # Modificado para filtrar por status
    cursor.execute("""
        SELECT q.id 
        FROM questions q 
        LEFT JOIN progress p ON q.id = p.question_id AND p.username = ? 
        WHERE p.question_id IS NULL AND q.status = 'active'
        ORDER BY RANDOM() LIMIT 1
    """, (username,))
    new_question = cursor.fetchone()
    
    conn.close()
    return new_question['id'] if new_question else None

def update_srs(username, question_id, difficulty):
    """Actualiza el SRS en la base de datos y registra la actividad."""
    conn = get_db_conn()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM progress WHERE username = ? AND question_id = ?", (username, question_id))
    progress = cursor.fetchone()
    today = datetime.date.today()
    
    if progress:
        interval, aciertos, fallos = progress['interval'], progress['aciertos'], progress['fallos']
    else:
        interval, aciertos, fallos = 1, 0, 0

    if difficulty == "fácil":
        interval = interval * 2 + 7; aciertos += 1
    elif difficulty == "medio":
        interval = interval + 3; aciertos += 1
    elif difficulty == "difícil":
        interval = 1; fallos += 1
    
    new_due_date = today + datetime.timedelta(days=interval)
    
    cursor.execute("""
        INSERT INTO progress (username, question_id, due_date, interval, aciertos, fallos)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(username, question_id) DO UPDATE SET
            due_date = excluded.due_date, interval = excluded.interval,
            aciertos = excluded.aciertos, fallos = excluded.fallos
    """, (username, question_id, new_due_date, interval, aciertos, fallos))
    
    # --- INICIO SECCIÓN MODO INTENSIVO: Registrar actividad ---
    cursor.execute(
        "INSERT INTO activity_log (username, action_type, timestamp) VALUES (?, 'answer', ?)",
        (username, datetime.datetime.now())
    )
    # --- FIN SECCIÓN MODO INTENSIVO ---
    
    conn.commit()
    conn.close()

def reset_evaluation_state():
    """Resetea el estado para mostrar la siguiente pregunta."""
    st.session_state.eval_state = "showing_question"
    st.session_state.current_question_id = None
    st.session_state.user_answer = None
    if 'current_question_data' in st.session_state:
        del st.session_state.current_question_data

def show_evaluation_page():
    """Muestra el motor de evaluación con modo de práctica continua."""
    st.subheader("🧠 Evaluación y Práctica")

    if 'eval_state' not in st.session_state:
        st.session_state.eval_state = "showing_question"
    if 'practice_active' not in st.session_state:
        st.session_state.practice_active = False

    if st.session_state.practice_active:
        st.success("✅ **Modo Práctica Continua Activado**")
        if st.button("⏹️ Detener Práctica"):
            st.session_state.practice_active = False
            reset_evaluation_state(); st.rerun()
        st.markdown("---")

    if st.session_state.eval_state == "showing_question":
        q_id = st.session_state.get('current_question_id')
        if q_id is None:
            q_id = get_next_question_for_user(st.session_state.current_user, practice_mode=st.session_state.practice_active)
            st.session_state.current_question_id = q_id

        if q_id is None:
            st.success("¡Felicidades! Has completado todas tus revisiones por hoy.")
            st.balloons()
            if st.button("🚀 Iniciar Práctica Continua"):
                st.session_state.practice_active = True; st.rerun()
            return

        conn = get_db_conn()
        pregunta_row = conn.execute("SELECT * FROM questions WHERE id = ?", (q_id,)).fetchone()
        conn.close()
        
        if not pregunta_row:
            st.error("Error: No se encontró la pregunta."); reset_evaluation_state(); st.rerun()
            return
            
        pregunta = dict(pregunta_row)
        pregunta['opciones'] = pregunta['opciones'].split('|')
        st.session_state.current_question_data = pregunta

        st.markdown(f"### {pregunta['enunciado']}")
        with st.form("eval_form"):
            opciones_con_indices = list(enumerate(pregunta['opciones']))
            user_choice_idx = st.radio("Selecciona tu respuesta:", opciones_con_indices, format_func=lambda x: x[1])
            if st.form_submit_button("Responder"):
                st.session_state.user_answer = user_choice_idx[1] 
                st.session_state.eval_state = "showing_feedback"; st.rerun()

    elif st.session_state.eval_state == "showing_feedback":
        pregunta = st.session_state.current_question_data
        respuesta_usuario = st.session_state.user_answer
        
        st.markdown(f"### {pregunta['enunciado']}")
        for op in pregunta['opciones']:
            if op == pregunta['correcta']: st.success(f"**{op} (Correcta)**")
            elif op == respuesta_usuario: st.error(f"**{op} (Tu respuesta)**")
            else: st.write(op)
        
        st.info(f"**Retroalimentación:**\n{pregunta['retroalimentacion']}")

        # --- INICIO SISTEMA DE VOTOS ---
        st.markdown("---")
        st.write("**Califica la calidad de esta pregunta:**")
        
        likes, unlikes = get_question_votes(pregunta['id'])
        
        # Check if user has already voted
        user_has_voted = has_user_voted(st.session_state.current_user, pregunta['id'])

        if not user_has_voted:
            vote_col1, vote_col2, vote_col3 = st.columns([1, 1, 3])
            
            if vote_col1.button("👍 Es buena"):
                cast_vote(st.session_state.current_user, pregunta['id'], 1)
                st.toast("¡Voto registrado!")
                st.rerun()

            if vote_col2.button("👎 Tiene errores"):
                cast_vote(st.session_state.current_user, pregunta['id'], -1)
                st.toast("¡Voto registrado, gracias por el feedback!")
                st.rerun()
            
            vote_col3.metric("Votos de Calidad", f"{likes} 👍", f"-{unlikes} 👎")
        else:
            metric_col, msg_col = st.columns([1, 2])
            metric_col.metric("Votos de Calidad", f"{likes} 👍", f"-{unlikes} 👎")
            with msg_col:
                st.write("") # Spacer
                st.caption("✔️ Ya calificaste esta pregunta.")
        # --- FIN SISTEMA DE VOTOS ---
        
        st.markdown("**¿Qué tan difícil fue esta pregunta?**")
        
        col1, col2, col3 = st.columns(3)
        if col1.button("Difícil"): 
            update_srs(st.session_state.current_user, pregunta['id'], "difícil")
            reset_evaluation_state()
            st.rerun()
        if col2.button("Medio"): 
            update_srs(st.session_state.current_user, pregunta['id'], "medio")
            reset_evaluation_state()
            st.rerun()
        if col3.button("Fácil"): 
            update_srs(st.session_state.current_user, pregunta['id'], "fácil")
            reset_evaluation_state()
            st.rerun()

def show_stats_page():
    """Muestra un dashboard analítico con un sistema de clasificación automática."""
    st.header("📊 Dashboard Analítico de la Comunidad")
    
    conn = get_db_conn()
    
    # 1. Extracción de Datos Granulares
    total_questions_global = conn.execute("SELECT COUNT(*) as count FROM questions WHERE status = 'active'").fetchone()['count']
    
    # Query para obtener todos los datos base de usuarios y su progreso
    query = """
        SELECT 
            u.username,
            u.is_resident,
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
            u.username, u.is_resident
    """
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        st.info("No hay datos de progreso de usuarios para mostrar en el ranking.")
        conn.close()
        return

    df['total_answers'] = df['total_aciertos'] + df['total_fallos']
    df['accuracy'] = (df['total_aciertos'] / df['total_answers']).fillna(0.0)
    df['mastery'] = (df['mastered_count'] / total_questions_global) if total_questions_global > 0 else 0.0

    # 2. Cálculo del Benchmark (El Estándar 🎓)
    residents_df = df[df['is_resident'] == 1]
    
    if not residents_df.empty:
        avg_resident_accuracy = residents_df['accuracy'].mean()
        avg_resident_mastery = residents_df['mastery'].mean()
        benchmark_source = "Promedio de Residentes"
    else:
        avg_resident_accuracy = 0.85
        avg_resident_mastery = 0.50
        benchmark_source = "Default del Sistema"

    col1, col2 = st.columns(2)
    col1.metric("🎯 Precisión de Referencia", f"{avg_resident_accuracy:.1%}", help=f"Basado en el {benchmark_source}")
    col2.metric("🎯 Maestría de Referencia", f"{avg_resident_mastery:.1%}", help=f"Basado en el {benchmark_source}")
    st.markdown("---")

    # 3. Algoritmo de Etiquetado (Clasificación)
    def get_status_label(row):
        if row['is_resident'] == 1:
            return "🎓 Residente"
        if row['accuracy'] >= (avg_resident_accuracy * 0.98) and row['total_answers'] > 50:
            return "⭐ Experto"
        if row['accuracy'] < 0.60 or (row['total_answers'] > 20 and row['mastery'] < 0.1):
            return "🚑 En Riesgo"
        return "🦁 Estudiante"

    df['Estado'] = df.apply(get_status_label, axis=1)

    # 4. Visualización del Dashboard
    df_display = df[['username', 'Estado', 'accuracy', 'mastery', 'total_answers']].copy()
    df_display.rename(columns={
        'username': 'Usuario',
        'accuracy': 'Precisión',
        'mastery': 'Maestría',
        'total_answers': 'Respuestas'
    }, inplace=True)

    st.dataframe(
        df_display,
        column_config={
            "Usuario": "Usuario",
            "Estado": "Estado",
            "Precisión": st.column_config.ProgressColumn(
                "Precisión",
                help="Porcentaje de respuestas correctas (Aciertos / Totales).",
                format="%.1f%%",
                min_value=0,
                max_value=1,
            ),
            "Maestría": st.column_config.ProgressColumn(
                "Maestría",
                help="Porcentaje de preguntas del sistema dominadas (intervalo > 7 días).",
                format="%.1f%%",
                min_value=0,
                max_value=1,
            ),
        },
        use_container_width=True,
        hide_index=True,
        column_order=("Usuario", "Estado", "Precisión", "Maestría", "Respuestas")
    )

    conn.close()

def show_manage_questions_page():
    """Permite gestionar (Editar y Eliminar) preguntas con confirmación de borrado."""
    if 'editing_question_id' not in st.session_state:
        st.session_state.editing_question_id = None
    
    # 1. Estado: Inicializa una variable st.session_state.confirm_delete_id = None al principio de la función.
    if 'confirm_delete_id' not in st.session_state:
        st.session_state.confirm_delete_id = None

    is_admin = (st.session_state.user_role == 'admin')
    
    if st.session_state.editing_question_id is not None:
        q_id = st.session_state.editing_question_id
        st.subheader(f"✏️ Editando Pregunta ID: {q_id}")
        conn = get_db_conn()
        row = conn.execute("SELECT * FROM questions WHERE id = ?", (q_id,)).fetchone()
        conn.close()
        if not row:
            st.error("La pregunta no se encontró."); st.session_state.editing_question_id = None; st.rerun()

        CATEGORIAS_MEDICAS = ["Medicina Interna", "Cirugía General", "Ortopedia", "Urología", "ORL", "Urgencia", "Psiquiatría", "Neurología", "Neurocirugía", "Epidemiología", "Pediatría", "Ginecología", "Oftalmología", "Otra"]
        try: cat_index = CATEGORIAS_MEDICAS.index(row['tag_categoria'])
        except (ValueError, TypeError): cat_index = None
        
        with st.form("edit_question_form"):
            new_enunciado = st.text_area("Enunciado", value=row['enunciado'])
            ops = row['opciones'].split('|'); op_a, op_b, op_c, op_d = ops[0], ops[1], ops[2], ops[3]
            op_a = st.text_input("Opción A", value=op_a); op_b = st.text_input("Opción B", value=op_b)
            op_c = st.text_input("Opción C", value=op_c); op_d = st.text_input("Opción D", value=op_d)
            new_correcta_idx = st.radio("Respuesta Correcta", (0, 1, 2, 3), format_func=lambda x: f"Opción {chr(65+x)}")
            new_retro = st.text_area("Retroalimentación", value=row['retroalimentacion'])
            new_cat = st.selectbox("Categoría", options=CATEGORIAS_MEDICAS, index=cat_index)
            new_tema = st.text_input("Tema", value=row['tag_tema'] or "")
            
            save_btn, cancel_btn = st.columns(2)
            if save_btn.form_submit_button("💾 Guardar Cambios", type="primary"):
                new_opciones = "|".join([op_a, op_b, op_c, op_d]); correcta_val = [op_a, op_b, op_c, op_d][new_correcta_idx]
                conn = get_db_conn()
                conn.execute("UPDATE questions SET enunciado=?, opciones=?, correcta=?, retroalimentacion=?, tag_categoria=?, tag_tema=? WHERE id=?", (new_enunciado, new_opciones, correcta_val, new_retro, new_cat, new_tema, q_id))
                conn.commit(); conn.close()
                st.success("Pregunta actualizada."); st.session_state.editing_question_id = None; st.rerun()
            if cancel_btn.form_submit_button("❌ Cancelar"):
                st.session_state.editing_question_id = None; st.rerun()
        return

    st.subheader("🔑 Gestionar Preguntas" if is_admin else "📋 Mis Preguntas")
    conn = get_db_conn()
    
    # Modificar consulta para traer también el estado de la pregunta
    query, params = (
        "SELECT id, enunciado, owner_username, status FROM questions", []
    ) if is_admin else (
        "SELECT id, enunciado, status FROM questions WHERE owner_username = ?", [st.session_state.current_user]
    )
    preguntas = conn.execute(query, params).fetchall()
    conn.close()
    
    for preg in preguntas:
        pregunta_id = preg['id']
        with st.container(border=True):
            col_main, col_buttons = st.columns([0.8, 0.2])

            with col_main:
                col_main.write(preg['enunciado'])
                
                # Mostrar estado si necesita revisión
                if preg['status'] == 'needs_revision':
                    col_main.warning("⚠️ En Revisión")
                
                if is_admin:
                    col_main.caption(f"Autor: {preg['owner_username']}")

            # Caso B (Modo Confirmación): Si el ID de esta pregunta está en el estado de confirmación.
            if st.session_state.confirm_delete_id == pregunta_id:
                with col_main: # Mostrar el warning en la columna principal
                    st.warning("¿Seguro que deseas eliminar esta pregunta?")
                
                with col_buttons: # Mostrar botones de confirmación en la columna derecha
                    confirm_col1, confirm_col2 = st.columns(2)
                    
                    if confirm_col1.button("Sí, eliminar", key=f"confirm_del_{pregunta_id}", type="primary"):
                        conn = get_db_conn()
                        conn.execute("DELETE FROM questions WHERE id = ?", (pregunta_id,))
                        conn.commit()
                        conn.close()
                        st.success(f"Pregunta {pregunta_id} eliminada.")
                        st.session_state.confirm_delete_id = None # Limpiar estado
                        st.rerun()
                    
                    if confirm_col2.button("Cancelar", key=f"cancel_del_{pregunta_id}"):
                        st.session_state.confirm_delete_id = None # Limpiar estado
                        st.rerun()
            
            # Caso A (Modo Normal): Mostrar botones estándar.
            else:
                with col_buttons:
                    if st.button("✏️ Editar", key=f"edit_{pregunta_id}"):
                        st.session_state.editing_question_id = pregunta_id
                        st.rerun()
                    
                    if st.button("🗑️ Eliminar", key=f"del_{pregunta_id}", type="primary"):
                        # No borra, solo activa el modo confirmación y hace rerun.
                        st.session_state.confirm_delete_id = pregunta_id
                        st.rerun()

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
    try:
        admin_user = st.secrets["ADMIN_USER"]
    except (KeyError, FileNotFoundError):
        admin_user = "admin" 
    
    # --- 1. SECCIÓN: GESTIONAR USUARIOS ACTIVOS ---
    st.subheader("👥 Gestionar Usuarios Activos")
    
    usuarios_activos = conn.execute(
        "SELECT username, role, is_approved, is_intensive, max_inactivity_days, status FROM users WHERE username != ? AND status = 'active'", 
        (admin_user,)
    ).fetchall()

    if not usuarios_activos:
        st.info("No hay usuarios activos para gestionar.")
    else:
        for user_row in usuarios_activos:
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
                        conn.execute("UPDATE users SET is_intensive = ?, max_inactivity_days = ? WHERE username = ?", (new_is_intensive, inactivity_days, username))
                        conn.commit()
                        st.success(f"Configuración de Modo Intensivo guardada para {username}.")
                        st.rerun()

    # --- 2. SECCIÓN: ZONA DE JUICIO ---
    st.markdown("---")
    st.subheader("💀 Zona de Juicio")

    pending_deletion_users = conn.execute("SELECT * FROM users WHERE status = 'pending_delete'").fetchall()

    if not pending_deletion_users:
        st.info("No hay usuarios pendientes de eliminación.")
    else:
        for user in pending_deletion_users:
            username = user['username']
            st.markdown("---")
            
            score, _, _ = calculate_user_score(username, user['max_inactivity_days'])
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

    # --- 3. SECCIÓN: HISTORIAL DE ELIMINADOS ---
    st.markdown("---")
    st.subheader("⚰️ Historial de Eliminados")
    deleted_log_df = pd.read_sql_query("SELECT username, deletion_date, reason FROM deleted_users_log ORDER BY deletion_date DESC", conn)
    if deleted_log_df.empty:
        st.info("El cementerio está vacío.")
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
                # Hoja 'Usuarios': Tabla users. IMPORTANTE: Elimina la columna password_hash.
                df_users = pd.read_sql_query("SELECT * FROM users", conn_export)
                if 'password_hash' in df_users.columns:
                    df_users = df_users.drop(columns=['password_hash'])
                df_users.to_excel(writer, sheet_name='Usuarios', index=False)

                # Hoja 'Preguntas': Tabla questions.
                df_questions = pd.read_sql_query("SELECT * FROM questions", conn_export)
                df_questions.to_excel(writer, sheet_name='Preguntas', index=False)

                # Hoja 'Progreso_SRS': Tabla progress.
                df_progress = pd.read_sql_query("SELECT * FROM progress", conn_export)
                df_progress.to_excel(writer, sheet_name='Progreso_SRS', index=False)

                # Hoja 'Duelos': Tabla duels.
                df_duels = pd.read_sql_query("SELECT * FROM duels", conn_export)
                df_duels.to_excel(writer, sheet_name='Duelos', index=False)
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
    setup_database()
    main()
