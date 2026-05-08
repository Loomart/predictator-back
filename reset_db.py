#!/usr/bin/env python3
"""
Script para resetear la base de datos: elimina todos los registros
y opcionalmente inserta datos de prueba iniciales.
"""

import sys
from backend.db import SessionLocal, engine
from backend.models import Market, MarketSnapshot, Signal, JobRun
from backend.seed_test_data import seed


def reset_database(seed_data: bool = False):
    """
    Resetea la base de datos eliminando todos los registros.
    Opcionalmente inserta datos de prueba.

    Args:
        seed_data: Si True, inserta datos de prueba después del reset.
    """
    db = SessionLocal()

    try:
        print("Iniciando reset de la base de datos...")

        # Eliminar en orden inverso a las dependencias para evitar errores de foreign key
        print("Eliminando registros de job_runs...")
        db.query(JobRun).delete()

        print("Eliminando registros de signals...")
        db.query(Signal).delete()

        print("Eliminando registros de market_snapshots...")
        db.query(MarketSnapshot).delete()

        print("Eliminando registros de markets...")
        db.query(Market).delete()

        db.commit()
        print("✅ Todos los registros eliminados exitosamente.")

        if seed_data:
            print("Insertando datos de prueba...")
            seed()
            print("✅ Datos de prueba insertados.")

        print("🎉 Reset completado!")

    except Exception as e:
        db.rollback()
        print(f"❌ Error durante el reset: {e}")
        sys.exit(1)

    finally:
        db.close()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--seed":
        reset_database(seed_data=True)
    else:
        reset_database(seed_data=False)
        print("\n💡 Para insertar datos de prueba, ejecuta: python reset_db.py --seed")