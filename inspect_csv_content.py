#!/usr/bin/env python3
"""
Script para inspeccionar el contenido real del CSV y encontrar dónde están las propuestas
"""

import polars as pl
import os

def read_csv():
    """Lee el CSV"""
    print("[LECTURA] Leyendo CSV...")
    
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
        return df
        
    except Exception as e:
        print(f"[ERROR] {e}")
        return None

def inspect_first_rows(df, num_rows=15):
    """Inspecciona las primeras filas del CSV"""
    print(f"\n[INSPECCION] Primeras {num_rows} filas del CSV:")
    print("=" * 100)
    
    for i in range(min(num_rows, df.height)):
        row_data = df.row(i)
        print(f"Fila {i+1:2d}: ", end="")
        
        # Mostrar primeras 8 columnas (A-H)
        for j in range(min(8, len(row_data))):
            val = str(row_data[j]) if row_data[j] is not None else ""
            val = val[:20] + ("..." if len(val) > 20 else "")
            col_letter = chr(65 + j)  # A, B, C, D, etc.
            print(f"{col_letter}:'{val}' ", end="")
        
        print()  # Nueva línea

def inspect_column_b_detailed(df):
    """Inspecciona específicamente la columna B en detalle"""
    print(f"\n[INSPECCION] Columna B en detalle:")
    print("=" * 60)
    
    if df.width < 2:
        print("[ERROR] No hay columna B")
        return
    
    col_b = df.get_column("column_1")  # Columna B (índice 1)
    
    print(f"Columna B tiene {len(col_b)} valores")
    
    # Revisar las primeras 25 filas
    print("\nPrimeras 25 filas de columna B:")
    for i in range(min(25, len(col_b))):
        val = col_b[i]
        val_str = str(val) if val is not None else "None"
        is_digit = val_str.isdigit() if val_str != "None" else False
        length = len(val_str) if val_str != "None" else 0
        
        print(f"  Fila {i+1:2d}: '{val_str}' (dígito: {is_digit}, len: {length})")

def search_for_numbers_anywhere(df):
    """Busca números que parezcan LEGAJOs en cualquier parte del CSV"""
    print(f"\n[BUSQUEDA] Buscando números de 4+ dígitos en todo el CSV:")
    print("=" * 70)
    
    numbers_found = []
    
    # Buscar en las primeras 10 columnas y primeras 30 filas
    for col_idx in range(min(10, df.width)):
        col_data = df.get_column(f"column_{col_idx}")
        col_letter = chr(65 + col_idx)
        
        for row_idx in range(min(30, len(col_data))):
            val = col_data[row_idx]
            if val is not None:
                val_str = str(val).strip()
                
                # Buscar números de 4+ dígitos
                if val_str.isdigit() and len(val_str) >= 4:
                    numbers_found.append({
                        'value': val_str,
                        'column': col_letter,
                        'column_idx': col_idx,
                        'row': row_idx + 1
                    })
    
    if numbers_found:
        print(f"Encontrados {len(numbers_found)} números de 4+ dígitos:")
        
        # Agrupar por columna
        by_column = {}
        for item in numbers_found:
            col = item['column']
            if col not in by_column:
                by_column[col] = []
            by_column[col].append(item)
        
        for col, items in by_column.items():
            print(f"\n  Columna {col}:")
            for item in items[:10]:  # Mostrar máximo 10 por columna
                print(f"    Fila {item['row']:2d}: {item['value']}")
            if len(items) > 10:
                print(f"    ... y {len(items) - 10} más")
    else:
        print("No se encontraron números de 4+ dígitos en las primeras filas")

def search_propuesta_text(df):
    """Busca el texto 'PROPUESTA' en el CSV"""
    print(f"\n[BUSQUEDA] Buscando texto 'PROPUESTA':")
    print("=" * 50)
    
    propuesta_found = []
    
    # Buscar en todas las columnas, primeras 20 filas
    for col_idx in range(min(df.width, 20)):
        col_data = df.get_column(f"column_{col_idx}")
        col_letter = chr(65 + col_idx)
        
        for row_idx in range(min(20, len(col_data))):
            val = col_data[row_idx]
            if val is not None:
                val_str = str(val).strip().upper()
                
                if "PROPUESTA" in val_str:
                    propuesta_found.append({
                        'value': str(col_data[row_idx]),
                        'column': col_letter,
                        'column_idx': col_idx,
                        'row': row_idx + 1
                    })
    
    if propuesta_found:
        print("Texto 'PROPUESTA' encontrado en:")
        for item in propuesta_found:
            print(f"  Columna {item['column']}, Fila {item['row']}: '{item['value']}'")
    else:
        print("No se encontró texto 'PROPUESTA' en las primeras filas")

def suggest_correct_column(df):
    """Sugiere cuál podría ser la columna correcta"""
    print(f"\n[SUGERENCIA] Analizando cuál podría ser la columna correcta:")
    print("=" * 70)
    
    # Buscar columnas con más números desde la fila 10 en adelante
    column_scores = {}
    
    for col_idx in range(min(10, df.width)):
        col_data = df.get_column(f"column_{col_idx}")
        col_letter = chr(65 + col_idx)
        
        numbers_count = 0
        valid_numbers = []
        
        # Contar desde fila 10 (índice 9) en adelante
        for row_idx in range(9, min(30, len(col_data))):
            val = col_data[row_idx]
            if val is not None:
                val_str = str(val).strip()
                if val_str.isdigit() and len(val_str) >= 4:
                    numbers_count += 1
                    valid_numbers.append(val_str)
        
        if numbers_count > 0:
            column_scores[col_letter] = {
                'index': col_idx,
                'count': numbers_count,
                'samples': valid_numbers[:5]
            }
    
    if column_scores:
        print("Columnas con números de 4+ dígitos (posibles LEGAJOs):")
        
        # Ordenar por cantidad de números
        sorted_columns = sorted(column_scores.items(), key=lambda x: x[1]['count'], reverse=True)
        
        for col_letter, data in sorted_columns:
            print(f"\n  Columna {col_letter} (índice {data['index']}):")
            print(f"    {data['count']} números encontrados")
            print(f"    Ejemplos: {', '.join(data['samples'])}")
    else:
        print("No se encontraron columnas con números válidos")
        
    return column_scores

def main():
    print("INSPECCION DETALLADA DEL CONTENIDO CSV")
    print("=" * 60)
    
    # 1. Leer CSV
    df = read_csv()
    if df is None:
        return
    
    # 2. Inspeccionar primeras filas
    inspect_first_rows(df, 15)
    
    # 3. Inspeccionar columna B específicamente
    inspect_column_b_detailed(df)
    
    # 4. Buscar números en cualquier parte
    search_for_numbers_anywhere(df)
    
    # 5. Buscar texto PROPUESTA
    search_propuesta_text(df)
    
    # 6. Sugerir columna correcta
    column_scores = suggest_correct_column(df)
    
    print(f"\n{'=' * 60}")
    print("[RESUMEN]")
    print(f"CSV: {df.height} filas x {df.width} columnas")
    
    if column_scores:
        best_column = max(column_scores.items(), key=lambda x: x[1]['count'])
        print(f"RECOMENDACION: Usar columna {best_column[0]} como PROPUESTA")
        print(f"  Tiene {best_column[1]['count']} números válidos")
        print(f"  Ejemplos: {', '.join(best_column[1]['samples'][:3])}")
    else:
        print("No se pudo determinar la columna correcta")
        print("Verifica manualmente el archivo CSV")

if __name__ == "__main__":
    main()