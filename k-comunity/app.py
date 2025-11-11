import streamlit as st
import sqlite3
import pandas as pd
import datetime
from passlib.context import CryptContext  # Para hashing de contraseñas

# --- CONFIGURACIÓN DE PÁGINA Y SEGURIDAD ---
st.set_page_config(
    page_title="Plataforma de Estudio SRS",
    page_icon="🧠",
    layout="wide"
)

# Contexto para hashear contraseñas
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
DB_FILE = "prisma_srs.db"

# --- FUNCIONES DE BASE DE DATOS (SQLite) ---

def get_db_conn():
    """Establece conexión con la BD SQLite."""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row # Permite acceder a las columnas por nombre
    return conn

def setup_database():
    """Crea las tablas, inserta al admin y actualiza la estructura (migración)."""
    conn = get_db_conn()
    cursor = conn.cursor()
    
    # Tabla de Usuarios
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'user'
    );
    """)
    
    # Tabla de Preguntas
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
    """)
    
    # Tabla de Progreso (SRS)
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
    """)
    
    # --- Migración: Añadir etiquetas si no existen ---
    cursor.execute("PRAGMA table_info(questions)")
    existing_columns = [col['name'] for col in cursor.fetchall()]
    
    if 'tag_categoria' not in existing_columns:
        cursor.execute("ALTER TABLE questions ADD COLUMN tag_categoria TEXT")
    if 'tag_tema' not in existing_columns:
        cursor.execute("ALTER TABLE questions ADD COLUMN tag_tema TEXT")
    
    # --- Fin Migración ---

    # --- INICIO DE SECCIÓN MODIFICADA (Admin por Defecto) ---
    
    # ADVERTENCIA: Contraseña codificada. No subir a GitHub público.
    ADMIN_USER_DEFAULT = "admin"
    ADMIN_PASS_DEFAULT = "admin123"

    # Verificar si el admin existe, si no, crearlo
    cursor.execute("SELECT * FROM users WHERE username = ?", (ADMIN_USER_DEFAULT,))
    admin = cursor.fetchone()
    if not admin:
        admin_pass_hash = pwd_context.hash(ADMIN_PASS_DEFAULT)
        cursor.execute("INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)",
                       (ADMIN_USER_DEFAULT, admin_pass_hash, "admin"))
    # --- FIN DE SECCIÓN MODIFICADA ---

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
    """Elimina un usuario y su progreso (Admin)."""
    conn = get_db_conn()
    cursor = conn.cursor()
    if username == 'admin':
        conn.close()
        return False, "No se puede eliminar al administrador principal."
    
    try:
        # ON DELETE CASCADE en 'progress' debería manejar esto, pero por seguridad:
        cursor.execute("DELETE FROM progress WHERE username = ?", (username,))
        cursor.execute("DELETE FROM users WHERE username = ?", (username,))
        conn.commit()
        return True, "Usuario eliminado."
    except sqlite3.Error as e:
        return False, f"Error de base de datos: {e}"
    finally:
        conn.close()

# --- PÁGINAS DE LA APLICACIÓN ---

def show_login_page():
    """Muestra login o registro."""
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
                st.session_state.logged_in = True
                st.session_state.current_user = user['username']
                st.session_state.user_role = user['role']
                st.session_state.current_page = "evaluacion"
                st.rerun()
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
            elif new_username == "admin":
                 st.error("Nombre de usuario no disponible.")
            else:
                conn = get_db_conn()
                cursor = conn.cursor()
                try:
                    hashed_pass = pwd_context.hash(new_password)
                    cursor.execute("INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)",
                                   (new_username, hashed_pass, 'user'))
                    conn.commit()
                    st.success("¡Usuario registrado! Ahora puedes iniciar sesión.")
                except sqlite3.IntegrityError:
                    st.error("Ese nombre de usuario ya existe.")
                finally:
                    conn.close()

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
        tag_categoria = st.selectbox(
            "Etiqueta 1: Categoría (Parametrizada)",
            options=CATEGORIAS_MEDICAS,
            index=None,
            placeholder="Selecciona la categoría principal..."
        )
        tag_tema = st.text_input(
            "Etiqueta 2: Tema (Texto libre)",
            placeholder="Ej: Fisiopatología de la Diabetes"
        )
        
        submitted = st.form_submit_button("Guardar Pregunta")
        
        if submitted:
            if not all([enunciado, opciones[0], opciones[1], opciones[2], opciones[3], retroalimentacion, tag_categoria, tag_tema]):
                st.warning("Por favor, completa todos los campos (incluyendo etiquetas).")
            else:
                conn = get_db_conn()
                cursor = conn.cursor()
                opciones_str = "|".join(opciones) 
                correcta = opciones[correcta_idx]
                owner = st.session_state.current_user
                
                cursor.execute("""
                    INSERT INTO questions (
                        owner_username, enunciado, opciones, correcta, 
                        retroalimentacion, tag_categoria, tag_tema
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (owner, enunciado, opciones_str, correcta, 
                      retroalimentacion, tag_categoria, tag_tema))
                
                conn.commit()
                conn.close()
                st.success("¡Pregunta guardada con éxito!")

def get_next_question_for_user(username, practice_mode=False):
    """Obtiene la próxima pregunta para el usuario desde SQLite."""
    conn = get_db_conn()
    cursor = conn.cursor()

    # --- INICIO DE SECCIÓN NUEVA ---
    if practice_mode:
        # Modo de práctica: Devuelve cualquier pregunta al azar
        cursor.execute("""
            SELECT id FROM questions
            ORDER BY RANDOM()
            LIMIT 1
        """)
        practice_question = cursor.fetchone()
        conn.close()
        return practice_question['id'] if practice_question else None
    # --- FIN DE SECCIÓN NUEVA ---

    # Lógica SRS Estándar (la que ya tenías)
    today = datetime.date.today()
    
    # 1. Buscar preguntas vencidas (due)
    cursor.execute("""
        SELECT q.id FROM questions q
        JOIN progress p ON q.id = p.question_id
        WHERE p.username = ? AND p.due_date <= ?
        ORDER BY RANDOM() LIMIT 1
    """, (username, today))
    due_question = cursor.fetchone()
    
    if due_question:
        conn.close()
        return due_question['id']

    # 2. Si no hay vencidas, buscar preguntas nuevas
    cursor.execute("""
        SELECT q.id FROM questions q
        LEFT JOIN progress p ON q.id = p.question_id AND p.username = ?
        WHERE p.question_id IS NULL
        ORDER BY RANDOM() LIMIT 1
    """, (username,))
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
        interval = interval * 2 + 7
        aciertos += 1
    elif difficulty == "medio":
        interval = interval + 3
        aciertos += 1
    elif difficulty == "difícil":
        interval = 1 # Reinicia
        fallos += 1
    
    new_due_date = today + datetime.timedelta(days=interval)
    
    cursor.execute("""
        INSERT INTO progress (username, question_id, due_date, interval, aciertos, fallos)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(username, question_id) DO UPDATE SET
            due_date = excluded.due_date,
            interval = excluded.interval,
            aciertos = excluded.aciertos,
            fallos = excluded.fallos
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
    """Muestra el motor de evaluación (preguntas y feedback)."""
    st.subheader("🧠 Iniciar Evaluación")
    
    if 'eval_state' not in st.session_state:
        st.session_state.eval_state = "showing_question"

    if st.session_state.eval_state == "showing_question":
        q_id = st.session_state.get('current_question_id')
        if q_id is None:
            q_id = get_next_question_for_user(st.session_state.current_user)
            st.session_state.current_question_id = q_id

        if q_id is None:
            st.success("¡Felicidades! Has completado todas tus revisiones por hoy.")
            st.balloons()
            
            # --- INICIO DE SECCIÓN NUEVA ---
            st.markdown("---")
            st.markdown("#### ¿Quieres seguir practicando?")
            st.info("El 'Modo Práctica' selecciona preguntas al azar. Tus respuestas seguirán actualizando tu curva del olvido.")
            
            if st.button("Iniciar Práctica Libre (1 Pregunta)", use_container_width=True):
                # Llamamos a la función que modificamos, con practice_mode=True
                practice_q_id = get_next_question_for_user(
                    st.session_state.current_user, 
                    practice_mode=True
                )
                
                if practice_q_id:
                    # Asignamos la nueva pregunta y reiniciamos la página
                    st.session_state.current_question_id = practice_q_id
                    st.session_state.eval_state = "showing_question"
                    st.rerun()
                else:
                    # Esto solo pasaría si la base de datos está totalmente vacía
                    st.error("No hay preguntas en la base de datos para practicar.")
            
            # --- FIN DE SECCIÓN NUEVA ---
            
            return # Detenemos la ejecución aquí para no mostrar el resto de la página

        conn = get_db_conn()
        pregunta_row = conn.execute("SELECT * FROM questions WHERE id = ?", (q_id,)).fetchone()
        conn.close()
        
        if not pregunta_row:
            st.error("Error: No se encontró la pregunta. Puede haber sido eliminada.")
            reset_evaluation_state()
            st.rerun()
            return
            
        pregunta = dict(pregunta_row)
        pregunta['opciones'] = pregunta['opciones'].split('|')
        st.session_state.current_question_data = pregunta

        st.markdown(f"### {pregunta['enunciado']}")
        with st.form("eval_form"):
            respuesta_usuario = st.radio("Selecciona tu respuesta:", pregunta['opciones'], key=f"q_radio_{q_id}")
            submit_respuesta = st.form_submit_button("Responder")

            if submit_respuesta:
                st.session_state.user_answer = respuesta_usuario
                st.session_state.eval_state = "showing_feedback"
                st.rerun()

    elif st.session_state.eval_state == "showing_feedback":
        pregunta = st.session_state.current_question_data
        respuesta_usuario = st.session_state.user_answer
        es_correcta = (respuesta_usuario == pregunta['correcta'])

        st.markdown(f"### {pregunta['enunciado']}")
        for op in pregunta['opciones']:
            if op == pregunta['correcta']: st.success(f"**{op} (Correcta)**")
            elif op == respuesta_usuario: st.error(f"**{op} (Tu respuesta)**")
            else: st.write(op)
        
        st.info(f"**Retroalimentación:**\n{pregunta['retroalimentacion']}")
        st.markdown("**¿Qué tan difícil fue esta pregunta?**")
        
        col1, col2, col3 = st.columns(3)
        if col1.button("Difícil", use_container_width=True):
            update_srs(st.session_state.current_user, pregunta['id'], "difícil")
            reset_evaluation_state(); st.rerun()
        if col2.button("Medio", use_container_width=True):
            update_srs(st.session_state.current_user, pregunta['id'], "medio")
            reset_evaluation_state(); st.rerun()
        if col3.button("Fácil", use_container_width=True):
            update_srs(st.session_state.current_user, pregunta['id'], "fácil")
            reset_evaluation_state(); st.rerun()

def show_stats_page():
    """Muestra estadísticas y el ranking."""
    st.subheader("📊 Estadísticas y Ranking Global")
    
    conn = get_db_conn()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) as total FROM questions")
    total_preguntas_global = cursor.fetchone()['total']
    
    if total_preguntas_global == 0:
        st.info("Aún no hay preguntas en el sistema."); conn.close(); return

    # "Aprendida" = intervalo > 7 días
    cursor.execute("""
        SELECT 
            username, 
            COUNT(CASE WHEN interval > 7 THEN 1 END) as aprendidas,
            COALESCE(SUM(aciertos), 0) as total_aciertos,
            COALESCE(SUM(fallos), 0) as total_fallos
        FROM progress GROUP BY username
    """)
    
    ranking_data, user_stats = [], {}
    for row in cursor.fetchall():
        tasa_aprendizaje = (row['aprendidas'] / total_preguntas_global) * 100
        ranking_data.append({
            "Usuario": row['username'],
            "Tasa de Aprendizaje (%)": tasa_aprendizaje,
            "Preg. Aprendidas": row['aprendidas'],
            "Aciertos": row['total_aciertos'],
            "Fallos": row['total_fallos']
        })
        if row['username'] == st.session_state.current_user: user_stats = row
            
    conn.close()

    st.markdown("#### Ranking de Tasa de Aprendizaje")
    st.info(f"Tasa de Aprendizaje = (Preguntas Aprendidas por Usuario / {total_preguntas_global} Preguntas Totales) * 100")

    if ranking_data:
        df_ranking = pd.DataFrame(ranking_data).sort_values(by="Tasa de Aprendizaje (%)", ascending=False)
        df_ranking = df_ranking.reset_index(drop=True); df_ranking.index += 1
        st.dataframe(df_ranking, use_container_width=True)
    
    st.markdown(f"#### Tus Estadísticas: {st.session_state.current_user}")
    if user_stats and 'username' in user_stats:
        total_aciertos = user_stats['total_aciertos']
        total_fallos = user_stats['total_fallos']
        total = total_aciertos + total_fallos
        tasa_acierto = (total_aciertos / total) * 100 if total > 0 else 0
        st.metric("Tasa de Acierto General", f"{tasa_acierto:.1f}%")
    else:
        st.info("Aún no has respondido ninguna pregunta.")

def show_manage_questions_page():
    """Página para que los usuarios vean/eliminen sus preguntas (y admin vea/elimine todo)."""
    is_admin = (st.session_state.user_role == 'admin')
    st.subheader("🔑 Gestionar Todas las Preguntas" if is_admin else "📋 Mis Preguntas Creadas")
    
    conn = get_db_conn()
    cursor = conn.cursor()
    
    query = "SELECT id, enunciado, owner_username, tag_categoria, tag_tema FROM questions"
    params = []
    if not is_admin:
        query += " WHERE owner_username = ?"
        params.append(st.session_state.current_user)
        
    cursor.execute(query, params)
    preguntas = cursor.fetchall()
    
    if not preguntas: st.info("No hay preguntas para mostrar.")
    
    for preg in preguntas:
        st.markdown("---")
        col1, col2 = st.columns([0.8, 0.2])
        with col1:
            # Accedemos con corchetes []. El 'or' maneja los valores Nulos/None.
            categoria = preg['tag_categoria'] or 'Sin Categoría'
            tema = preg['tag_tema'] or 'Sin Tema'
            st.markdown(f"**ID:** {preg['id']} | **Categoría:** {categoria}")
            st.markdown(f"**Enunciado:** {preg['enunciado']}")
            st.caption(f"Tema: {tema}")
            if is_admin: st.caption(f"Creada por: {preg['owner_username']}")
        with col2:
            if st.button("Eliminar", key=f"del_q_{preg['id']}", use_container_width=True):
                cursor.execute("DELETE FROM questions WHERE id = ?", (preg['id'],))
                conn.commit(); conn.close()
                st.success(f"Pregunta {preg['id']} eliminada.")
                st.rerun()
    conn.close()

def show_admin_panel():
    """Página de gestión de usuarios (Solo Admin)."""
    if st.session_state.user_role != 'admin':
        st.error("Acceso denegado."); return
        
    st.subheader("🔑 Panel de Admin: Gestionar Usuarios")
    conn = get_db_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT username, role FROM users")
    
    for user in cursor.fetchall():
        st.markdown("---")
        col1, col2 = st.columns([0.8, 0.2])
        col1.markdown(f"**Usuario:** {user['username']} (Rol: {user['role']})")
        if user['username'] != 'admin':
            if col2.button("Eliminar", key=f"del_u_{user['username']}", use_container_width=True):
                success, message = delete_user_from_db(user['username'])
                if success: st.success(message); conn.close(); st.rerun()
                else: st.error(message)
    conn.close()

def show_change_password_page():
    """Permite al usuario logueado cambiar su propia contraseña."""
    st.subheader("🔐 Cambiar Mi Contraseña")
    st.write(f"Estás modificando la contraseña para el usuario: **{st.session_state.current_user}**")
    
    with st.form("change_password_form", clear_on_submit=True):
        password_new = st.text_input("Nueva Contraseña", type="password")
        password_confirm = st.text_input("Confirmar Nueva Contraseña", type="password")
        submitted = st.form_submit_button("Actualizar Contraseña")
        
        if submitted:
            if not password_new or not password_confirm:
                st.warning("Por favor, rellena ambos campos.")
            elif password_new != password_confirm:
                st.error("Las contraseñas no coinciden. Inténtalo de nuevo.")
            else:
                # Las contraseñas coinciden, proceder a hashear y actualizar
                try:
                    new_password_hash = pwd_context.hash(password_new)
                    conn = get_db_conn()
                    cursor = conn.cursor()
                    
                    cursor.execute(
                        "UPDATE users SET password_hash = ? WHERE username = ?",
                        (new_password_hash, st.session_state.current_user)
                    )
                    conn.commit()
                    conn.close()
                    
                    st.success("¡Tu contraseña ha sido actualizada con éxito!")
                    st.balloons()
                except Exception as e:
                    st.error(f"Ocurrió un error al actualizar la base de datos: {e}")

# --- CONTROLADOR PRINCIPAL (MAIN) ---

def main():
    """Función principal que actúa como enrutador."""
    
    # Inicializar estado de sesión
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
    if 'current_user' not in st.session_state:
        st.session_state.current_user = None
    if 'user_role' not in st.session_state:
        st.session_state.user_role = None
    if 'current_page' not in st.session_state:
        st.session_state.current_page = "login"

    # --- Interfaz Principal ---
    if not st.session_state.logged_in:
        show_login_page()
    else:
        st.sidebar.title(f"Bienvenido, {st.session_state.current_user}")
        st.sidebar.caption(f"Rol: {st.session_state.user_role}")
        st.sidebar.markdown("---")
        
        # Navegación
        if st.sidebar.button("🧠 Iniciar Evaluación", use_container_width=True):
            st.session_state.current_page = "evaluacion"; reset_evaluation_state(); st.rerun()
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

        # --- INICIO DE SECCIÓN NUEVA ---
        st.sidebar.markdown("---")
        st.sidebar.markdown("Mi Perfil")
        if st.sidebar.button("🔐 Cambiar Contraseña", use_container_width=True):
            st.session_state.current_page = "change_password"
            st.rerun()
        # --- FIN DE SECCIÓN NUEVA ---
            
        st.sidebar.markdown("---")
        if st.sidebar.button("Cerrar Sesión", use_container_width=True):
            for key in list(st.session_state.keys()): del st.session_state[key]
            st.rerun()

        # Enrutador de páginas
        page_functions = {
            "evaluacion": show_evaluation_page,
            "crear": show_create_page,
            "gestionar": show_manage_questions_page,
            "estadisticas": show_stats_page,
            "admin_users": show_admin_panel,
            "change_password": show_change_password_page,
        }
        
        # Ejecutar la función de la página actual
        # Asegurarse de que el admin_users solo sea accesible por admin
        if st.session_state.current_page == "admin_users" and st.session_state.user_role != 'admin':
            st.session_state.current_page = "evaluacion" # Volver a la página por defecto
        
        page_to_show = page_functions.get(st.session_state.current_page, show_evaluation_page)
        page_to_show()

# --- EJECUCIÓN ---
if __name__ == "__main__":
    setup_database() # Asegurar que la BD esté lista al iniciar
    main()

