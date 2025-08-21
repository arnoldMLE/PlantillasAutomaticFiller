#!/usr/bin/env python3
"""
Procesador CSV final con SQL optimizada para devolver solo 1 cliente por LEGAJO
"""

import polars as pl
import fdb
import os
import time
from config import DATABASE_CONFIG

def get_single_client_per_legajo_query(placeholders):
    """
    Devuelve consulta SQL que prioriza TITULARES y garantiza solo 1 cliente por LEGAJO
    """
    return f"""
    SELECT 
        main_data.LEGAJO,
        main_data.Nombre_Cliente,
        main_data.Caracter_Titular,
        main_data.SUCURSAL,
        main_data.MONTO,
        main_data.FECHA_CONTRATO
    FROM (
        SELECT DISTINCT 
            p.LEGAJO,
            c.NOMBRE as Nombre_Cliente,
            cart.NOMBRE as Caracter_Titular,
            s.NOMBRE AS SUCURSAL,
            CR.MONTO,
            ct.FECHA as FECHA_CONTRATO,
            ROW_NUMBER() OVER (
                PARTITION BY p.LEGAJO 
                ORDER BY 
                    CASE 
                        WHEN UPPER(cart.NOMBRE) = 'TITULAR' THEN 1
                        WHEN UPPER(cart.NOMBRE) LIKE 'PROPIETARIO' THEN 2
                        ELSE 3
                    END ASC,
                    c.NOMBRE ASC
            ) as rn
        FROM CONTRATOS_CLIENTES cc
        INNER JOIN clientes c ON cc.COD_CLIENTE=c.COD_CLIENTE
        INNER JOIN CARACTER_TITULARES cart ON cart.COD_CARACTER_TITULAR = cc.COD_CARACTER_TITULAR
        INNER JOIN contratos ct ON cc.COD_CONTRATO=ct.COD_CONTRATO
        INNER JOIN propuesta p ON ct.COD_PROPUESTA=p.COD_PROPUESTA
        INNER JOIN creditos cr ON cr.COD_PROPUESTA = p.COD_PROPUESTA
        INNER JOIN sucursales s ON cr.COD_SUCURSAL=s.COD_SUCURSAL
        INNER JOIN ESTADOS_DEUDAS ed ON cr.COD_ESTADO_DEUDA=ed.COD_ESTADO_DEUDA
        INNER JOIN ESTADOS_CONTRATOS ec ON ct.COD_ESTADO_CONTRATO = ec.COD_ESTADO_CONTRATO
        INNER JOIN TIPOS_PROPUESTAS TP ON P.COD_TIPO_PROPUESTA=TP.COD_TIPO_PROPUESTA
        LEFT JOIN CONTRATOS_PARCELAS cp ON ct.COD_CONTRATO=cp.COD_CONTRATO_PARCELA
        LEFT JOIN parcela pa ON cp.COD_PARCELA=pa.COD_PARCELA
        LEFT JOIN ESTADOS_PARCELA EP ON EP.COD_ESTADO_PARCELA = PA.COD_ESTADO
        LEFT JOIN MANZANA ma ON pa.COD_MANZANA=ma.COD_MANZANA
        LEFT JOIN ZONAS_PARQUES zp ON ma.COD_ZONA_PARQUE=zp.COD_ZONA_PARQUE
        INNER JOIN vendedor vg ON ct.COD_VENDEDOR=vg.COD_VENDEDOR
        INNER JOIN EQUIPO_VENDEDORES eq ON vg.COD_EQUIPO=eq.COD_EQUIPO
        INNER JOIN TIPOS_CONTRATOS TC ON CT.COD_TIPO_CONTRATO=TC.COD_TIPO_CONTRATO
        LEFT JOIN PLANES_VENTAS_MODELOS pvm ON ct.COD_PLAN_VENTA_MODELO=pvm.COD_PLAN_VENTA_MODELO
        LEFT JOIN MEDIOS_COBROS mco ON mco.COD_CLIENTE = c.COD_CLIENTE
        LEFT JOIN ZONA_COBRANZA zc ON zc.COD_ZONA_COBRANZA = mco.COD_ZONA_COBRANZA
        LEFT JOIN MEDIOS_COBROS_DOMICILIOS mcd ON mcd.COD_MEDIO_COBRO = mco.COD_MEDIO_COBRO
        LEFT JOIN DOMICILIOS DOM ON dom.COD_DOMICILIO = mcd.COD_DOMICILIO
        LEFT JOIN BARRIOS bar ON bar.COD_BARRIO = dom.COD_BARRIO
        LEFT JOIN LOCALIDADES loc ON loc.COD_LOCALIDAD = dom.COD_LOCALIDAD
        LEFT JOIN PROVINCIAS prv ON prv.COD_PROVINCIA = loc.COD_PROVINCIA
        LEFT OUTER JOIN TARJETA_OXXO tx ON tx.COD_PROPUESTA = p.COD_PROPUESTA
        LEFT OUTER JOIN CLABE_BANCARIA cb ON cb.COD_PROPUESTA = p.COD_PROPUESTA
        LEFT OUTER JOIN MEDIOS_COBROS_BANCOS mcb ON mcb.COD_MEDIO_COBRO = mco.COD_MEDIO_COBRO
        WHERE p.LEGAJO IN ({placeholders})
    ) main_data
    WHERE main_data.rn = 1
    ORDER BY main_data.LEGAJO
    """

def get_simple_query_fallback(placeholders):
    """
    Consulta más simple que prioriza titulares como fallback
    """
    return f"""
    SELECT 
        p.LEGAJO,
        c.NOMBRE,
        cart.NOMBRE as Caracter_Titular
    FROM CONTRATOS_CLIENTES cc
    INNER JOIN clientes c ON cc.COD_CLIENTE=c.COD_CLIENTE
    INNER JOIN CARACTER_TITULARES cart ON cart.COD_CARACTER_TITULAR = cc.COD_CARACTER_TITULAR
    INNER JOIN contratos ct ON cc.COD_CONTRATO=ct.COD_CONTRATO
    INNER JOIN propuesta p ON ct.COD_PROPUESTA=p.COD_PROPUESTA
    INNER JOIN creditos cr ON cr.COD_PROPUESTA = p.COD_PROPUESTA
    INNER JOIN sucursales s ON cr.COD_SUCURSAL=s.COD_SUCURSAL
    WHERE p.LEGAJO IN ({placeholders})
    ORDER BY p.LEGAJO, 
             CASE 
                 WHEN UPPER(cart.NOMBRE) = 'TITULAR' THEN 1
                 WHEN UPPER(cart.NOMBRE) = 'PROPIETARIO' THEN 2
                 ELSE 3
             END ASC,
             c.NOMBRE ASC
    """

def process_csv_final():
    """Procesador final con SQL que prioriza TITULARES y garantiza 1 resultado por LEGAJO"""
    print("PROCESADOR CSV FINAL - PRIORIZANDO TITULARES")
    print("=" * 60)
    print("[INFO] Este procesador prioriza TITULARES sobre otros caracteres")
    print("[INFO] Orden de prioridad: 1.TITULAR → 2.PROPIETARIO → 3.OTROS")
    print("=" * 60)
    
    csv_file = "ORDEN DE VENTA CUA.csv"
    
    # 1. Leer CSV
    print("[1/4] Leyendo CSV...")
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
    print("[2/4] Extrayendo LEGAJOs...")
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
    
    # 3. Consultar base de datos con SQL optimizada
    print("[3/4] Consultando BD (1 cliente por LEGAJO)...")
    
    try:
        con = fdb.connect(
            host=DATABASE_CONFIG["host"],
            database=DATABASE_CONFIG["database_path"],
            user=DATABASE_CONFIG["user"],
            password=DATABASE_CONFIG["password"],
            charset=DATABASE_CONFIG["charset"]
        )
        
        cursor = con.cursor()
        matches = {}
        
        batch_size = 50  # Lotes más pequeños para consultas complejas
        total_batches = (len(legajos) + batch_size - 1) // batch_size
        
        print(f"[INFO] Procesando {total_batches} lotes de {batch_size} LEGAJOs...")
        
        for i in range(0, len(legajos), batch_size):
            batch = legajos[i:i+batch_size]
            batch_num = i//batch_size + 1
            
            print(f"  Lote {batch_num}/{total_batches}: {len(batch)} LEGAJOs...", end=" ", flush=True)
            
            start_time = time.time()
            placeholders = ','.join(['?' for _ in batch])
            
            try:
                # Intentar primero con ROW_NUMBER()
                query = get_single_client_per_legajo_query(placeholders)
                cursor.execute(query, batch)
                results = cursor.fetchall()
                
            except Exception as e1:
                print(f"ROW_NUMBER falló: {e1}")
                print(f"    Usando consulta simple...")
                
                try:
                    # Fallback a consulta más simple
                    simple_query = get_simple_query_fallback(placeholders)
                    cursor.execute(simple_query, batch)
                    all_results = cursor.fetchall()
                    
                    # Filtrar para tomar solo el primer resultado por LEGAJO
                    results = []
                    seen_legajos = set()
                    
                    for result in all_results:
                        legajo = result[0]
                        if legajo not in seen_legajos:
                            results.append(result)
                            seen_legajos.add(legajo)
                    
                except Exception as e2:
                    print(f"Consulta simple falló: {e2}")
                    print(f"    Usando consultas individuales...")
                    
                    # Último fallback: consultas individuales con prioridad a titulares
                    results = []
                    for legajo in batch:
                        try:
                            individual_query = """
                            SELECT FIRST 1 
                                p.LEGAJO, 
                                c.NOMBRE,
                                cart.NOMBRE as Caracter_Titular
                            FROM CONTRATOS_CLIENTES cc
                            INNER JOIN clientes c ON cc.COD_CLIENTE=c.COD_CLIENTE
                            INNER JOIN CARACTER_TITULARES cart ON cart.COD_CARACTER_TITULAR = cc.COD_CARACTER_TITULAR
                            INNER JOIN contratos ct ON cc.COD_CONTRATO=ct.COD_CONTRATO
                            INNER JOIN propuesta p ON ct.COD_PROPUESTA=p.COD_PROPUESTA
                            INNER JOIN creditos cr ON cr.COD_PROPUESTA = p.COD_PROPUESTA
                            INNER JOIN sucursales s ON cr.COD_SUCURSAL=s.COD_SUCURSAL
                            WHERE p.LEGAJO = ?
                            ORDER BY 
                                CASE 
                                    WHEN UPPER(cart.NOMBRE) = 'TITULAR' THEN 1
                                    WHEN UPPER(cart.NOMBRE) = 'PROPIETARIO' THEN 2
                                    ELSE 3
                                END ASC,
                                c.NOMBRE ASC
                            """
                            cursor.execute(individual_query, (legajo,))
                            result = cursor.fetchone()
                            if result:
                                results.append(result)
                        except:
                            pass
            
            # Procesar resultados (ya deberían ser únicos por LEGAJO)
            batch_matches = 0
            for result in results:
                legajo = str(result[0])
                cliente = result[1]
                caracter = result[2] if len(result) > 2 else "No especificado"
                monto = result[4]
                fecha_contrato = result[5]
                
                matches[legajo] = {
                    'cliente': cliente,
                    'caracter': caracter,
                    'monto':monto,
                    'fecha_contrato':fecha_contrato
                }
                batch_matches += 1
            
            elapsed = time.time() - start_time
            print(f"{batch_matches} únicos en {elapsed:.1f}s")
        
        con.close()
        print(f"\n[OK] {len(matches)} LEGAJOs únicos procesados")
        
    except Exception as e:
        print(f"[ERROR] Base de datos: {e}")
        return
    
    # 4. Actualizar CSV y guardar
    print("[4/4] Actualizando CSV...")
    
    cliente_col_name = df.columns[5]  # Columna F (CLIENTE)

    monto_col_name = df.columns[21]  #Columna V (IMPORTE ORIGINAL)

    fecha_col_name = df.columns[7]   #Columna h (FECHA_CONTRATO)


    current_values = df.get_column(cliente_col_name).to_list()


    monto_current_values = df.get_column(monto_col_name).to_list()


    fecha_contrato_current_values = df.get_column(fecha_col_name).to_list()
    
    updates_count = 0
    titulares_count = 0
    monto_count = 0    
    for i in range(10, df.height):  # Desde fila 11
        if i < len(col_b):
            legajo = str(col_b[i]).strip()
            
            if legajo in matches:
                if i < len(current_values):
                    current_val = str(current_values[i]).strip()
                    if not current_val or current_val in ['', 'CLIENTE', 'nan', 'None', 'null']:
                        current_values[i] = matches[legajo]['cliente']
                        monto_current_values[i] = matches[legajo]['monto']
                        fecha_contrato_current_values[i] = matches[legajo]['fecha_contrato']
                        updates_count += 1
                        
                        # Contar si es titular
                        if 'TITULAR' in matches[legajo]['caracter'].upper():
                            titulares_count += 1
    
    # Actualizar DataFrame
    while len(current_values) < df.height:
        current_values.append("")
        monto_current_values.append("")
        fecha_contrato_current_values.append("")
    
    df = df.with_columns([
        pl.Series(name=cliente_col_name, values=current_values[:df.height]),
        pl.Series(name=monto_col_name, values=monto_current_values[:df.height]),
        pl.Series(name=fecha_col_name, values=fecha_contrato_current_values[:df.height])
    ])
    
    # Guardar archivo
    try:
        output_file = "ORDEN DE VENTA CUA_PROCESADO.csv"
        df.write_csv(output_file, separator=',')
        print(f"[OK] Archivo guardado: {output_file}")
        
    except Exception as e:
        print(f"[ERROR] Guardando: {e}")
        return
    
    # Estadísticas finales
    print(f"\n{'=' * 60}")
    print("RESULTADO FINAL")
    print(f"{'=' * 60}")
    print(f"LEGAJOs en CSV:            {len(legajos):,}")
    print(f"LEGAJOs únicos en BD:      {len(matches):,}")
    print(f"Campos actualizados:       {updates_count:,}")
    print(f"De los cuales TITULARES:   {titulares_count:,}")
    
    efficiency = (len(matches) / len(legajos)) * 100 if len(legajos) > 0 else 0
    titular_rate = (titulares_count / updates_count) * 100 if updates_count > 0 else 0
    
    print(f"Eficiencia de match:       {efficiency:.1f}%")
    print(f"% de titulares:            {titular_rate:.1f}%")
    
    if updates_count > 0:
        print(f"\n[EXITO] ¡{updates_count} registros actualizados!")
        print(f"[INFO] {titulares_count} son TITULARES principales")
        print(f"[INFO] {updates_count - titulares_count} son otros caracteres")
    else:
        print(f"\n[INFO] Todos los campos ya estaban completos")
    
    print(f"\nArchivo final: {output_file}")

def quick_test_unique():
    """Prueba rápida para verificar priorización de TITULARES"""
    print("PRUEBA DE PRIORIZACION DE TITULARES")
    print("=" * 40)
    print("[INFO] Probando LEGAJOs que sabemos tienen múltiples clientes")
    print("[INFO] Debe priorizar: 1.TITULAR → 2.PROPIETARIO → 3.OTROS")
    print("-" * 40)
    
    # LEGAJOs que sabemos tienen múltiples clientes
    test_legajos = ['10153', '116090']
    
    try:
        con = fdb.connect(
            host=DATABASE_CONFIG["host"],
            database=DATABASE_CONFIG["database_path"],
            user=DATABASE_CONFIG["user"],
            password=DATABASE_CONFIG["password"],
            charset=DATABASE_CONFIG["charset"]
        )
        
        cursor = con.cursor()
        
        print("[TEST] Probando consulta con ROW_NUMBER()...")
        
        placeholders = ','.join(['?' for _ in test_legajos])
        
        try:
            query = get_single_client_per_legajo_query(placeholders)
            cursor.execute(query, test_legajos)
            results = cursor.fetchall()
            
            print(f"[OK] {len(results)} resultados únicos con prioridad a TITULARES:")
            for result in results:
                titular_info = f" ({result[2]})" if len(result) > 2 else ""
                print(f"  {result[0]} -> {result[1]}{titular_info}")
                
        except Exception as e:
            print(f"[ERROR] ROW_NUMBER() no soportado: {e}")
            print("[TEST] Probando consulta simple...")
            
            query = get_simple_query_fallback(placeholders)
            cursor.execute(query, test_legajos)
            all_results = cursor.fetchall()
            
            # Filtrar para tomar solo el primer resultado por LEGAJO
            results = []
            seen_legajos = set()
            
            for result in all_results:
                legajo = result[0]
                if legajo not in seen_legajos:
                    results.append(result)
                    seen_legajos.add(legajo)
            
            print(f"[OK] {len(results)} resultados con consulta simple (priorizando TITULARES):")
            for result in results:
                titular_info = f" ({result[2]})" if len(result) > 2 else ""
                print(f"  {result[0]} -> {result[1]}{titular_info}")
        
        con.close()
        return True
        
    except Exception as e:
        print(f"[ERROR] {e}")
        return False

def main():
    print("SELECCIONA OPCION:")
    print("1. Prueba de priorización de TITULARES")
    print("2. Procesamiento completo (priorizando TITULARES)")
    print("Opción (1/2): ", end="")
    
    choice = input().strip()
    
    if choice == "1":
        quick_test_unique()
    elif choice == "2":
        print("\n[INFO] Procesamiento con prioridad a TITULARES...")
        print("[INFO] Esto puede tomar 3-8 minutos...")
        process_csv_final()
    else:
        print("Opción inválida")

if __name__ == "__main__":
    main()