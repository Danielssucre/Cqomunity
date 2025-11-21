import streamlit as st
import sqlite3
import pandas as pd
import datetime
import os
import time
import json
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

    # Verificar y añadir la columna 'is_approved' a 'users' si no existe
    cursor.execute("PRAGMA table_info(users)")
    existing_columns = [col[1] for col in cursor.fetchall()]
    
    if 'is_approved' not in existing_columns:
        cursor.execute("ALTER TABLE users ADD COLUMN is_approved INTEGER NOT NULL DEFAULT 0")

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
    """Elimina un usuario y todo su progreso de la base de datos."""
    try:
        conn = get_db_conn()
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON") 
        cursor.execute("DELETE FROM users WHERE username = ?", (username,))
        conn.commit()
        conn.close()
        st.success(f"Usuario {username} eliminado permanentemente.")
    except Exception as e:
        st.error(f"Error al eliminar usuario: {e}")

# --- PÁGINAS DE LA APLICACIÓN ---

def show_login_page():
    """Muestra login (con chequeo de aprobación) y registro (con estado pendiente)."""
    st.subheader("Inicio de Sesión")
    
    with st.form("login_form"):
        username = st.text_input("Nombre de usuario")
        password = st.text_input("Contraseña", type="password")
        login_submitted = st.form_submit_button("Ingresar")

        if login_submitted:
            conn = get_db_conn()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
            user = cursor.fetchone()
            conn.close()
            
            if user and verify_password(password, user['password_hash']):
                if user['is_approved'] == 1:
                    st.session_state.logged_in = True
                    st.session_state.current_user = user['username']
                    st.session_state.user_role = user['role']
                    st.session_state.current_page = "evaluacion"
                    st.rerun()
                else:
                    st.error("Tu cuenta está registrada, pero aún no ha sido aprobada por un administrador.")
            else:
                st.error("Usuario o contraseña incorrectos.")

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
                conn.commit()
                conn.close()
                st.success("¡Pregunta guardada con éxito!")

def get_next_question_for_user(username, practice_mode=False):
    """Obtiene la próxima pregunta para el usuario desde SQLite."""
    conn = get_db_conn()
    cursor = conn.cursor()

    if practice_mode:
        cursor.execute("SELECT id FROM questions ORDER BY RANDOM() LIMIT 1")
        practice_question = cursor.fetchone()
        conn.close()
        return practice_question['id'] if practice_question else None

    today = datetime.date.today()
    cursor.execute("SELECT q.id FROM questions q JOIN progress p ON q.id = p.question_id WHERE p.username = ? AND p.due_date <= ? ORDER BY RANDOM() LIMIT 1", (username, today))
    due_question = cursor.fetchone()
    
    if due_question:
        conn.close()
        return due_question['id']

    cursor.execute("SELECT q.id FROM questions q LEFT JOIN progress p ON q.id = p.question_id AND p.username = ? WHERE p.question_id IS NULL ORDER BY RANDOM() LIMIT 1", (username,))
    new_question = cursor.fetchone()
    
    conn.close()
    return new_question['id'] if new_question else None

def update_srs(username, question_id, difficulty):
    """Actualiza el SRS en la base de datos."""
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
        st.markdown("**¿Qué tan difícil fue esta pregunta?**")
        
        col1, col2, col3 = st.columns(3)
        if col1.button("Difícil"): update_srs(st.session_state.current_user, pregunta['id'], "difícil"); reset_evaluation_state(); st.rerun()
        if col2.button("Medio"): update_srs(st.session_state.current_user, pregunta['id'], "medio"); reset_evaluation_state(); st.rerun()
        if col3.button("Fácil"): update_srs(st.session_state.current_user, pregunta['id'], "fácil"); reset_evaluation_state(); st.rerun()

def show_stats_page():
    """Muestra un dashboard de métricas avanzado para el usuario y la comunidad."""
    st.header("📊 Dashboard de Métricas")
    conn = get_db_conn()
    cursor = conn.cursor()
    current_user = st.session_state.current_user

    st.subheader("🌎 Métrica Global de la Comunidad")
    total_preguntas_global = conn.execute("SELECT COUNT(*) as total FROM questions").fetchone()['total']
    st.metric(label="Nº Total de Preguntas en la BD", value=total_preguntas_global)

    if total_preguntas_global == 0:
        st.info("Aún no hay preguntas en el sistema."); conn.close(); return
    st.markdown("---")

    st.subheader(f"📈 Progreso de {current_user}")
    preguntas_aprendidas_user = conn.execute("SELECT COUNT(*) as aprendidas FROM progress WHERE username = ? AND interval > 7", (current_user,)).fetchone()['aprendidas']
    col1, col2 = st.columns(2)
    col1.metric(label="Preguntas Aprendidas", value=preguntas_aprendidas_user)
    col2.metric(label="Preguntas Por Aprender", value=total_preguntas_global - preguntas_aprendidas_user)
    st.markdown("---")

    st.subheader("📚 Desglose de Contenido y Maestría por Tema")
    df_temas_cat = pd.read_sql_query("SELECT tag_categoria, COUNT(DISTINCT tag_tema) as 'Nº de Temas' FROM questions WHERE tag_categoria IS NOT NULL AND tag_tema IS NOT NULL GROUP BY tag_categoria ORDER BY `Nº de Temas` DESC", conn)
    if not df_temas_cat.empty:
        st.dataframe(df_temas_cat, use_container_width=True)

    df_dominio = pd.read_sql_query("SELECT q.tag_tema, COUNT(p.question_id) as 'Preguntas Aprendidas' FROM progress p JOIN questions q ON p.question_id = q.id WHERE p.username = ? AND p.interval > 7 GROUP BY q.tag_tema ORDER BY `Preguntas Aprendidas` DESC", conn, params=(current_user,))
    if not df_dominio.empty:
        st.dataframe(df_dominio, use_container_width=True)
    st.markdown("---")

    st.subheader("🏆 Ranking Global de Aprendizaje")
    ranking_data = pd.read_sql_query("SELECT username, COUNT(CASE WHEN interval > 7 THEN 1 END) as aprendidas FROM progress GROUP BY username", conn)
    if not ranking_data.empty and total_preguntas_global > 0:
        ranking_data["Tasa de Aprendizaje (%)"] = (ranking_data["aprendidas"] / total_preguntas_global) * 100
        st.dataframe(ranking_data.sort_values(by="Tasa de Aprendizaje (%)", ascending=False), use_container_width=True)
    conn.close()

def show_manage_questions_page():
    """Permite gestionar (Editar y Eliminar) preguntas."""
    if 'editing_question_id' not in st.session_state:
        st.session_state.editing_question_id = None
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
    query, params = ("SELECT id, enunciado, owner_username FROM questions", []) if is_admin else ("SELECT id, enunciado FROM questions WHERE owner_username = ?", [st.session_state.current_user])
    preguntas = conn.execute(query, params).fetchall()
    conn.close()
    
    for preg in preguntas:
        with st.container(border=True):
            c1, c2 = st.columns([0.8, 0.2])
            c1.write(preg['enunciado'])
            if is_admin: c1.caption(f"Autor: {preg['owner_username']}")
            if c2.button("✏️ Editar", key=f"edit_{preg['id']}"):
                st.session_state.editing_question_id = preg['id']; st.rerun()
            if c2.button("🗑️ Eliminar", key=f"del_{preg['id']}", type="primary"):
                conn = get_db_conn(); conn.execute("DELETE FROM questions WHERE id = ?", (preg['id'],)); conn.commit(); conn.close()
                st.success("Eliminada."); st.rerun()

# --- INICIO DE SECCIÓN NUEVA: PÁGINA DE DUELOS ---
def play_duel_interface():
    """Maneja la interfaz y lógica de una partida de duelo en curso."""
    duel_id = st.session_state.current_duel_id
    q_idx = st.session_state.duel_question_index
    questions = st.session_state.duel_questions
    
    if q_idx >= len(questions):
        st.success("¡Has completado el duelo!")
        st.balloons()
        
        # Registrar puntaje final
        conn = get_db_conn()
        cursor = conn.cursor()
        current_user = st.session_state.current_user
        score = st.session_state.duel_user_score
        
        duel = cursor.execute("SELECT * FROM duels WHERE id = ?", (duel_id,)).fetchone()
        
        # Determinar si somos el retador o el oponente
        if duel['challenger_username'] == current_user:
            cursor.execute("UPDATE duels SET challenger_score = ? WHERE id = ?", (score, duel_id))
            opponent_finished = duel['opponent_score'] is not None
            opponent_score = duel['opponent_score']
        else: # Somos el oponente
            cursor.execute("UPDATE duels SET opponent_score = ? WHERE id = ?", (score, duel_id))
            opponent_finished = True # El retador siempre termina primero
            opponent_score = duel['challenger_score']

        conn.commit()

        # Si ambos han jugado, determinar ganador
        if opponent_finished:
            if score > opponent_score: winner = current_user
            elif opponent_score > score: winner = duel['challenger_username'] if duel['challenger_username'] != current_user else duel['opponent_username']
            else: winner = duel['challenger_username'] # Empate gana el retador
            
            cursor.execute("UPDATE duels SET status = 'finished', winner = ? WHERE id = ?", (winner, duel_id))
            conn.commit()
            st.info(f"El ganador del duelo es: {winner}")

        conn.close()
        
        if st.button("Volver a Duelos"):
            del st.session_state.duel_state
            st.rerun()
        return

    # Lógica del temporizador y pregunta
    pregunta = questions[q_idx]
    
    if 'duel_question_start_time' not in st.session_state:
        st.session_state.duel_question_start_time = time.time()

    time_limit = 40.0
    elapsed = time.time() - st.session_state.duel_question_start_time
    time_left = time_limit - elapsed
    
    st.progress(max(0, time_left / time_limit), text=f"Tiempo restante: {max(0, int(time_left))}s")

    st.subheader(f"Pregunta {q_idx + 1}/{len(questions)}")
    st.markdown(f"### {pregunta['enunciado']}")

    with st.form(f"duel_q_{pregunta['id']}"):
        opciones = pregunta['opciones'].split('|')
        user_choice = st.radio("Elige una respuesta:", options=opciones, key=f"duel_radio_{pregunta['id']}")
        
        if st.form_submit_button("Responder"):
            if time_left > 0:
                if user_choice == pregunta['correcta']:
                    st.session_state.duel_user_score += 1
                    st.toast("¡Correcto! ✅")
                else:
                    st.toast("Incorrecto. ❌")
            else:
                st.toast("¡Se acabó el tiempo! ⌛️")

            st.session_state.duel_question_index += 1
            del st.session_state.duel_question_start_time
            st.rerun()

def show_duels_page():
    """Página principal de Duelos (PvP Asincrónico)."""
    st.header("⚔️ Duelos PvP")

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
        # Encontrar oponente que no sea el usuario actual
        cursor.execute("SELECT username FROM users WHERE username != ? AND is_approved = 1 ORDER BY RANDOM() LIMIT 1", (current_user,))
        opponent = cursor.fetchone()
        
        if not opponent:
            st.warning("No hay otros usuarios disponibles para desafiar.")
        else:
            opponent_username = opponent['username']
            # Seleccionar 5 preguntas al azar
            cursor.execute("SELECT id FROM questions ORDER BY RANDOM() LIMIT 5")
            questions = cursor.fetchall()
            if len(questions) < 5:
                st.error("No hay suficientes preguntas en la base de datos para un duelo (se necesitan 5).")
            else:
                question_ids = ",".join([str(q['id']) for q in questions])
                now = datetime.datetime.now()
                
                # Crear el duelo
                cursor.execute(
                    "INSERT INTO duels (challenger_username, opponent_username, question_ids, status, created_at) VALUES (?, ?, ?, 'pending', ?)",
                    (current_user, opponent_username, question_ids, now)
                )
                duel_id = cursor.lastrowid
                conn.commit()
                
                # Iniciar el flujo de juego para el retador
                st.session_state.duel_state = 'playing'
                st.session_state.current_duel_id = duel_id
                st.session_state.duel_question_index = 0
                st.session_state.duel_user_score = 0
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
                    st.session_state.duel_state = 'playing'
                    st.session_state.current_duel_id = duel['id']
                    st.session_state.duel_question_index = 0
                    st.session_state.duel_user_score = 0
                    st.session_state.duel_questions = [dict(q) for q in conn.execute(f"SELECT * FROM questions WHERE id IN ({question_ids})").fetchall()]
                    st.rerun()
    
    st.markdown("---")

    # Sección de Estadísticas y Ranking
    st.subheader("Estadísticas y Ranking de Duelos")
    
    # Métricas personales
    wins = cursor.execute("SELECT COUNT(*) FROM duels WHERE winner = ?", (current_user,)).fetchone()[0]
    losses = cursor.execute("SELECT COUNT(*) FROM duels WHERE winner != ? AND (challenger_username = ? OR opponent_username = ?)", (current_user, current_user, current_user)).fetchone()[0]
    
    col1, col2 = st.columns(2)
    col1.metric("Duelos Ganados", wins)
    col2.metric("Duelos Perdidos", losses)

    # Top Duelistas
    st.markdown("##### Top Duelistas")
    ranking_df = pd.read_sql_query("SELECT winner as Usuario, COUNT(id) as Victorias FROM duels WHERE winner IS NOT NULL GROUP BY winner ORDER BY Victorias DESC", conn)
    if not ranking_df.empty:
        ranking_df.index += 1
        st.dataframe(ranking_df, use_container_width=True)
    else:
        st.info("Aún no hay resultados de duelos para mostrar un ranking.")

    conn.close()
# --- FIN DE SECCIÓN NUEVA ---

def show_admin_panel():
    """Página de gestión de usuarios (Aprobar, Revocar, Eliminar)."""
    if st.session_state.user_role != 'admin':
        st.error("Acceso denegado."); return
        
    st.subheader("🔑 Panel de Admin: Gestionar Usuarios")
    conn = get_db_conn()
    usuarios = conn.execute("SELECT username, role, is_approved FROM users WHERE username != ?", (st.secrets["ADMIN_USER"],)).fetchall()

    if not usuarios:
        st.info("No hay otros usuarios registrados."); conn.close(); return

    for user in usuarios:
        st.markdown("---")
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            st.markdown(f"**{user['username']}** ({user['role']})")
        with col2:
            st.write("✅ Aprobado" if user['is_approved'] else "⏳ Pendiente")
        with col3:
            if user['is_approved'] == 0:
                if st.button("Aprobar", key=f"approve_{user['username']}"):
                    conn.execute("UPDATE users SET is_approved = 1 WHERE username = ?", (user['username'],)); conn.commit(); st.rerun()
            else:
                if st.button("Revocar", key=f"revoke_{user['username']}", type="secondary"):
                    conn.execute("UPDATE users SET is_approved = 0 WHERE username = ?", (user['username'],)); conn.commit(); st.rerun()
            if st.button("Eliminar ⚠️", key=f"del_{user['username']}"):
                delete_user_from_db(user['username']); st.rerun()
    conn.close()

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
        }
        
        page_to_show = page_functions.get(st.session_state.get("current_page", "evaluacion"), show_evaluation_page)
        page_to_show()

# --- EJECUCIÓN ---
if __name__ == "__main__":
    setup_database()
    main()
