#!/usr/bin/env python3
"""
Script para verificar nombres reales de columnas y corregir el acceso
"""

import polars as pl
import os

def check_column_names():
    """Verifica los nombres reales de las columnas en Polars"""
    print("[CHECK] Verificando nombres de columnas...")
    
    csv_file = "ORDEN DE VENTA CUA.csv"
    
    try:
        df = pl.read_csv(
            csv_file,
            separator=',',
            has_header=False,
            truncate_ragged_lines=True,
            ignore_errors=True,
            infer_schema_length=0,
            encoding='utf-8'
        )
        
        print(f"[OK] CSV leído: {df.height} filas x {df.width} columnas")
        print(f"[INFO] Nombres de columnas: {df.columns}")
        
        # Verificar columna B específicamente
        if df.width >= 2:
            column_b_name = df.columns[1]  # Segunda columna (B)
            print(f"[INFO] Nombre de columna B: '{column_b_name}'")
            
            # Mostrar algunos valores de columna B
            col_b_data = df.get_column(column_b_name)
            print(f"[INFO] Primeros valores de columna B:")
            
            for i in range(10, min(20, len(col_b_data))):  # Desde fila 11
                val = col_b_data[i]
                print(f"  Fila {i+1}: '{val}'")
        
        return df
        
    except Exception as e:
        print(f"[ERROR] {e}")
        return None

def extract_legajos_correct(df):
    """Extrae LEGAJOs usando los nombres correctos de columnas"""
    print("\n[EXTRACCION] Usando nombres correctos de columnas...")
    
    if df.width < 2:
        print("[ERROR] CSV no tiene suficientes columnas")
        return []
    
    # Usar el nombre real de la columna B
    column_b_name = df.columns[1]  # Segunda columna
    col_b_data = df.get_column(column_b_name)
    
    print(f"[INFO] Extrayendo de columna '{column_b_name}'")
    
    legajos_found = []
    
    # Buscar desde fila 11 (índice 10)
    for i in range(10, min(df.height, len(col_b_data))):
        val = col_b_data[i]
        if val is not None:
            val_str = str(val).strip()
            
            # Buscar números de 4+ dígitos
            if val_str and val_str.isdigit() and len(val_str) >= 4:
                legajos_found.append(val_str)
                print(f"  Fila {i+1}: LEGAJO '{val_str}'")
    
    print(f"[RESULTADO] {len(legajos_found)} LEGAJOs encontrados")
    return legajos_found

def test_database_with_legajos(legajos):
    """Prueba algunos LEGAJOs en la base de datos"""
    print(f"\n[TEST] Probando {len(legajos)} LEGAJOs en base de datos...")
    
    if not legajos:
        return {}
    
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
        
        cursor = con.cursor()
        matches = {}
        
        for legajo in legajos[:10]:  # Probar primeros 10
            try:
                cursor.execute(query, (legajo,))
                result = cursor.fetchone()
                
                if result:
                    matches[legajo] = result[1]  # Nombre del cliente
                    print(f"  [MATCH] {legajo} -> {result[1]}")
                else:
                    print(f"  [NO MATCH] {legajo}")
                    
            except Exception as e:
                print(f"  [ERROR] {legajo}: {e}")
        
        con.close()
        
        print(f"[RESULTADO] {len(matches)} matches encontrados")
        return matches
        
    except Exception as e:
        print(f"[ERROR] Base de datos: {e}")
        return {}

def main():
    print("VERIFICACION Y CORRECCION DE NOMBRES DE COLUMNAS")
    print("=" * 60)
    
    # 1. Verificar nombres de columnas
    df = check_column_names()
    if df is None:
        return
    
    # 2. Extraer LEGAJOs con nombres correctos
    legajos = extract_legajos_correct(df)
    
    # 3. Probar en base de datos
    matches = test_database_with_legajos(legajos)
    
    print(f"\n{'=' * 60}")
    print("[RESUMEN]")
    print(f"LEGAJOs encontrados: {len(legajos)}")
    print(f"Matches en BD: {len(matches)}")
    
    if matches:
        print("\n[EXITO] ¡El proceso funciona!")
        print("Ejemplos de matches:")
        for legajo, cliente in list(matches.items())[:3]:
            print(f"  {legajo} -> {cliente}")
        
        print(f"\n[SIGUIENTE] Ahora puedes ejecutar el script principal corregido")
    else:
        print("\n[INFO] No se encontraron matches")
        print("Verifica:")
        print("  1. Que los LEGAJOs existan en la BD")
        print("  2. Que la consulta SQL sea correcta")

if __name__ == "__main__":
    main()