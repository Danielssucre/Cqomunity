
import sqlite3
import os

# --- CONFIGURACIÓN INTELIGENTE ---
POSIBLES_RUTAS = [
    "k-comunity/prisma_srs.db",
    "prisma_srs.db",
]

def encontrar_db():
    for ruta in POSIBLES_RUTAS:
        if os.path.exists(ruta) and os.path.getsize(ruta) > 0:
            return ruta
    return None

db_path = encontrar_db()
if not db_path:
    print("⚠️ No se encontró la base de datos.")
else:
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        row = cursor.execute("SELECT metadata FROM activity_log WHERE action_type='answer_submitted' ORDER BY id DESC LIMIT 1").fetchone()
        if row and row[0]:
            print(f"📦 JSON ÚLTIMO EVENTO: {row[0]}")
        else:
            print("⚠️ No hay eventos 'answer_submitted' con metadatos en la base de datos.")
        conn.close()
    except Exception as e:
        print(f"❌ Error al consultar la base de datos: {e}")
