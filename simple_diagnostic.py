#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Diagnóstico simple sin emojis para Windows
"""

import os
import sys

def test_polars():
    """Prueba Polars de manera simple"""
    print("\n[TEST] Polars...")
    try:
        import polars as pl
        
        # Test básico
        df = pl.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
        filtered = df.filter(pl.col('a') > 1)
        
        print(f"[OK] Polars {pl.__version__} - {filtered.height} filas filtradas")
        return True
    except Exception as e:
        print(f"[ERROR] Polars: {e}")
        return False

def test_database():
    """Prueba conexión a base de datos"""
    print("\n[TEST] Base de datos...")
    try:
        from config import DATABASE_CONFIG
        import fdb
        
        con = fdb.connect(
            host=DATABASE_CONFIG["host"],
            database=DATABASE_CONFIG["database_path"],
            user=DATABASE_CONFIG["user"],
            password=DATABASE_CONFIG["password"],
            charset=DATABASE_CONFIG["charset"]
        )
        
        cur = con.cursor()
        cur.execute("SELECT 1 FROM RDB$DATABASE")
        result = cur.fetchone()
        con.close()
        
        print("[OK] Conexión a Firebird exitosa")
        return True
    except Exception as e:
        print(f"[ERROR] Base de datos: {e}")
        return False

def test_csv_files():
    """Verifica archivos CSV"""
    print("\n[TEST] Archivos CSV...")
    
    csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]
    
    if not csv_files:
        print("[INFO] No se encontraron archivos CSV")
        return False
    
    print(f"[OK] {len(csv_files)} archivos CSV encontrados:")
    for csv_file in csv_files[:5]:  # Mostrar máximo 5
        size_mb = os.path.getsize(csv_file) / (1024 * 1024)
        print(f"  - {csv_file} ({size_mb:.1f} MB)")
    
    return True

def test_propuesta_query():
    """Prueba consulta de propuesta específica"""
    print("\n[TEST] Consulta de propuesta...")
    
    try:
        from config import DATABASE_CONFIG
        import fdb
        
        con = fdb.connect(
            host=DATABASE_CONFIG["host"],
            database=DATABASE_CONFIG["database_path"],
            user=DATABASE_CONFIG["user"],
            password=DATABASE_CONFIG["password"],
            charset=DATABASE_CONFIG["charset"]
        )
        
        # Consulta simple
        query = """
        SELECT DISTINCT 
            p.LEGAJO AS COD_PROPUESTA,
            c.NOMBRE as Nombre_Cliente,
            s.NOMBRE AS SUCURSAL
        FROM CONTRATOS_CLIENTES cc
        INNER JOIN clientes c ON cc.COD_CLIENTE=c.COD_CLIENTE
        INNER JOIN contratos ct ON cc.COD_CONTRATO=ct.COD_CONTRATO
        INNER JOIN propuesta p ON ct.COD_PROPUESTA=p.COD_PROPUESTA
        INNER JOIN creditos cr ON cr.COD_PROPUESTA = p.COD_PROPUESTA
        INNER JOIN sucursales s ON cr.COD_SUCURSAL=s.COD_SUCURSAL
        WHERE p.LEGAJO = ?
        """
        
        cur = con.cursor()
        
        # Probar con LEGAJO del ejemplo
        test_legajo = "642799"
        cur.execute(query, (test_legajo,))
        result = cur.fetchone()
        
        if result:
            print(f"[OK] LEGAJO {test_legajo} encontrado: {result[1]}")
            con.close()
            return True
        else:
            print(f"[INFO] LEGAJO {test_legajo} no encontrado, buscando otros...")
            
            # Buscar LEGAJOs existentes
            cur.execute("SELECT FIRST 5 LEGAJO FROM propuesta ORDER BY LEGAJO")
            sample_legajos = cur.fetchall()
            
            if sample_legajos:
                print("[INFO] LEGAJOs disponibles:")
                for legajo_row in sample_legajos:
                    print(f"  - {legajo_row[0]}")
                
                # Probar con el primer LEGAJO encontrado
                first_legajo = str(sample_legajos[0][0])
                cur.execute(query, (first_legajo,))
                test_result = cur.fetchone()
                
                if test_result:
                    print(f"[OK] Consulta funciona con LEGAJO {first_legajo}: {test_result[1]}")
                    con.close()
                    return True
            
            con.close()
            return False
            
    except Exception as e:
        print(f"[ERROR] Consulta: {e}")
        return False

def process_sample_csv():
    """Procesa un CSV de ejemplo"""
    print("\n[TEST] Procesamiento CSV...")
    
    try:
        import polars as pl
        
        # Crear CSV de prueba
        test_data = """HEADER,PROPUESTA JKM,OTROS
HEADER2,HEADER2,HEADER2
,,,
,,,
,,,
,,,
,,,
,,,
,,,
PROPUESTA JKM,NUMERO,OTROS
OV001,642799,Datos
OV002,10153,Datos
OV003,116090,Datos"""
        
        with open("test_sample.csv", "w", encoding="utf-8") as f:
            f.write(test_data)
        
        # Leer con Polars
        df = pl.read_csv("test_sample.csv", has_header=False, infer_schema_length=0)
        
        print(f"[OK] CSV leído: {df.height} filas, {df.width} columnas")
        
        # Extraer propuestas desde fila 11 (índice 10)
        if df.height > 10 and df.width > 1:
            propuestas = []
            col_b = df.get_column("column_1")
            
            for i in range(10, min(df.height, 15)):  # Revisar filas 11-15
                if i < len(col_b):
                    val = col_b[i]
                    if val and str(val).strip():
                        propuestas.append(str(val).strip())
            
            print(f"[OK] Propuestas extraídas: {propuestas}")
        
        # Limpiar archivo de prueba
        os.remove("test_sample.csv")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Procesamiento CSV: {e}")
        return False

def main():
    print("DIAGNOSTICO SIMPLE PARA WINDOWS")
    print("=" * 50)
    
    tests = [
        ("Polars", test_polars),
        ("Base de datos", test_database),
        ("Archivos CSV", test_csv_files),
        ("Consulta propuesta", test_propuesta_query),
        ("Procesamiento CSV", process_sample_csv),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n--- {test_name} ---")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"[ERROR] {test_name}: {e}")
            results.append((test_name, False))
    
    # Resumen
    print("\n" + "=" * 50)
    print("RESUMEN:")
    
    passed = 0
    for test_name, success in results:
        status = "[OK]" if success else "[ERROR]"
        print(f"  {status} {test_name}")
        if success:
            passed += 1
    
    print(f"\nRESULTADO: {passed}/{len(results)} pruebas exitosas")
    
    if passed >= 3:  # Al menos 3 de 5
        print("\n[EXITO] Sistema funcional!")
        print("Puedes ejecutar:")
        print("  python main_polars.py")
        print("  start_server.bat")
    else:
        print("\n[AVISO] Hay problemas que necesitan atencion")
        print("Revisa los errores arriba")

if __name__ == "__main__":
    main()