import sqlite3
import json
import os

# --- CONFIGURACIÓN INTELIGENTE ---
# Buscamos la base de datos en las ubicaciones probables
POSIBLES_RUTAS = [
    "k-comunity/prisma_srs.db",  # Ruta más probable (subcarpeta)
    "prisma_srs.db",             # Ruta alternativa (raíz)
    "comunity_learning.db",      # Nombre antiguo por si acaso
    "k-comunity/comunity_learning.db"
]

USER_TO_CHECK = "cun"

def encontrar_db():
    for ruta in POSIBLES_RUTAS:
        if os.path.exists(ruta):
            # Verificar que no sea un archivo vacío (0 bytes)
            if os.path.getsize(ruta) > 0:
                return ruta
    return None

def audit_system():
    db_path = encontrar_db()
    
    if not db_path:
        print("❌ ERROR CRÍTICO: No encuentro la base de datos en ninguna carpeta.")
        print(f"   Busqué en: {POSIBLES_RUTAS}")
        print("   Asegúrate de ejecutar este script desde la carpeta raíz 'cqomunity'.")
        return

    print(f"🕵️ AUDITANDO USUARIO: '{USER_TO_CHECK}'")
    print(f"📁 BASE DE DATOS ENCONTRADA: '{db_path}'")
    print("-" * 50)

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # BLOQUE 1: PERFIL
        print("📊 1. PERFIL (Tabla 'users'):")
        cursor.execute("SELECT * FROM users WHERE username = ?", (USER_TO_CHECK,))
        user = cursor.fetchone()

        if not user:
            print(f"   ⚠️ El usuario '{USER_TO_CHECK}' NO existe en este archivo de DB.")
        else:
            cols = [
                "is_reference_model", "admitted_status", "admitted_specialty",
                "final_accuracy_snapshot", "avg_seconds_per_question",
                "avg_daily_questions", "total_questions_snapshot"
            ]
            for col in cols:
                try:
                    val = user[col]
                    icon = "✅" if val not in [None, 0, 0.0, "Pending", ""] else "⚪️"
                    print(f"   {icon} {col}: {val}")
                except IndexError:
                    print(f"   ❌ {col}: FALTA COLUMNA (Requiere Migración)")

        print("-" * 50)

        # BLOQUE 2: LOGS
        print("⏱️ 2. TELEMETRÍA (Tabla 'activity_log'):")
        try:
            cursor.execute("""
                SELECT action_type, metadata, timestamp 
                FROM activity_log 
                WHERE username = ? 
                ORDER BY id DESC LIMIT 3
            """, (USER_TO_CHECK,))
            
            logs = cursor.fetchall()
            
            if not logs:
                print("   ⚠️ No hay logs recientes para este usuario.")
            else:
                for log in logs:
                    print(f"   📢 [{log['timestamp']}] {log['action_type']}")
                    try:
                        if log['metadata']:
                            meta = json.loads(log['metadata'])
                            if 'time_seconds' in meta:
                                print(f"      ⏱️ TIEMPO: {meta['time_seconds']}s")
                            if 'result' in meta:
                                print(f"      🎯 RESULTADO: {meta['result']}")
                    except:
                        pass
                    print("   " + "."*20)
        except sqlite3.OperationalError:
            print("   ❌ ERROR: La tabla 'activity_log' NO EXISTE en esta base de datos.")

        conn.close()

    except Exception as e:
        print(f"❌ Error general: {e}")

if __name__ == "__main__":
    audit_system()
