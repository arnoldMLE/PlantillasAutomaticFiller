#!/usr/bin/env python3
"""
Procesador final de CSV con estadísticas completas y opción de sobrescribir
"""

import polars as pl
import fdb
import os
from config import DATABASE_CONFIG

def process_csv_final(overwrite_existing=False):
    """Procesa el CSV con opción de sobrescribir datos existentes"""
    print("PROCESADOR FINAL CSV-FIREBIRD")
    print("=" * 60)
    
    csv_file = "ORDEN DE VENTA CUA.csv"
    
    # 1. Leer CSV
    print("[1/5] Leyendo CSV...")
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
        print(f"[OK] {df.height} filas x {df.width} columnas")
    except Exception as e:
        print(f"[ERROR] {e}")
        return
    
    # 2. Extraer LEGAJOs
    print("\n[2/5] Extrayendo LEGAJOs...")
    column_b_name = df.columns[1]
    col_b = df.get_column(column_b_name)
    
    legajos = []
    for i in range(10, df.height):  # Desde fila 11
        val = col_b[i]
        if val is not None:
            val_str = str(val).strip()
            if val_str and val_str.isdigit() and len(val_str) >= 4:
                legajos.append(val_str)
    
    print(f"[OK] {len(legajos)} LEGAJOs encontrados")
    
    # 3. Consultar base de datos
    print("\n[3/5] Consultando base de datos...")
    try:
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
            c.NOMBRE as Nombre_Cliente
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
        
        print("[INFO] Procesando en lotes de 100...")
        
        for i in range(0, len(legajos), 100):
            batch = legajos[i:i+100]
            print(f"  Lote {i//100 + 1}: {len(batch)} LEGAJOs")
            
            for legajo in batch:
                try:
                    cursor.execute(query, (legajo,))
                    result = cursor.fetchone()
                    if result:
                        matches[legajo] = result[1]
                except:
                    pass
        
        con.close()
        print(f"[OK] {len(matches)} matches encontrados en BD")
        
    except Exception as e:
        print(f"[ERROR] Base de datos: {e}")
        return
    
    # 4. Actualizar CSV
    print("\n[4/5] Actualizando CSV...")
    
    cliente_col_name = df.columns[5]  # Columna F (CLIENTE)
    current_values = df.get_column(cliente_col_name).to_list()
    
    stats = {
        'total_legajos': len(legajos),
        'found_in_db': len(matches),
        'already_filled': 0,
        'newly_filled': 0,
        'overwritten': 0
    }
    
    for i in range(10, df.height):  # Desde fila 11
        if i < len(col_b):
            legajo = str(col_b[i]).strip()
            
            if legajo in matches:
                if i < len(current_values):
                    current_val = str(current_values[i]).strip()
                    
                    if not current_val or current_val in ['', 'CLIENTE', 'nan', 'None', 'null']:
                        # Campo vacío - llenar
                        current_values[i] = matches[legajo]
                        stats['newly_filled'] += 1
                    elif current_val != matches[legajo]:
                        # Campo diferente
                        if overwrite_existing:
                            current_values[i] = matches[legajo]
                            stats['overwritten'] += 1
                        else:
                            stats['already_filled'] += 1
                    else:
                        # Campo ya correcto
                        stats['already_filled'] += 1
    
    # Actualizar DataFrame
    while len(current_values) < df.height:
        current_values.append("")
    
    df = df.with_columns([
        pl.Series(name=cliente_col_name, values=current_values[:df.height])
    ])
    
    # 5. Guardar archivo
    print("\n[5/5] Guardando archivo...")
    
    try:
        output_file = "ORDEN DE VENTA CUA_PROCESADO.csv"
        df.write_csv(output_file, separator=',')
        print(f"[OK] Archivo guardado: {output_file}")
        
    except Exception as e:
        print(f"[ERROR] Guardando: {e}")
        return
    
    # 6. Mostrar estadísticas finales
    print(f"\n{'=' * 60}")
    print("ESTADISTICAS FINALES")
    print(f"{'=' * 60}")
    print(f"Total LEGAJOs en CSV:      {stats['total_legajos']:,}")
    print(f"Encontrados en BD:         {stats['found_in_db']:,}")
    print(f"Ya tenían cliente:         {stats['already_filled']:,}")
    print(f"Recién llenados:           {stats['newly_filled']:,}")
    
    if overwrite_existing:
        print(f"Sobrescritos:              {stats['overwritten']:,}")
    
    efficiency = (stats['found_in_db'] / stats['total_legajos']) * 100 if stats['total_legajos'] > 0 else 0
    print(f"Eficiencia BD:             {efficiency:.1f}%")
    
    fill_rate = stats['newly_filled'] + stats['already_filled'] + stats['overwritten']
    fill_percentage = (fill_rate / stats['total_legajos']) * 100 if stats['total_legajos'] > 0 else 0
    print(f"Campos completados:        {fill_percentage:.1f}%")
    
    if stats['newly_filled'] > 0 or stats['overwritten'] > 0:
        print(f"\n[EXITO] ¡{stats['newly_filled'] + stats['overwritten']} registros actualizados!")
    else:
        print(f"\n[INFO] Todos los campos ya estaban completos")
    
    print(f"\nArchivo final: {output_file}")

def main():
    print("¿Quieres sobrescribir datos existentes? (s/n): ", end="")
    response = input().strip().lower()
    overwrite = response.startswith('s')
    
    if overwrite:
        print("[INFO] Modo: Sobrescribir datos existentes")
    else:
        print("[INFO] Modo: Solo llenar campos vacíos")
    
    process_csv_final(overwrite_existing=overwrite)

if __name__ == "__main__":
    main()