#!/usr/bin/env python3
"""
Procesador CSV ultra-optimizado usando consultas batch
Reduce de 2511 consultas a ~25 consultas batch
"""

import polars as pl
import fdb
import os
import time
from config import DATABASE_CONFIG

def process_csv_optimized():
    """Procesa el CSV con consultas batch optimizadas"""
    print("PROCESADOR CSV ULTRA-OPTIMIZADO")
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
    
    # 3. Consultar base de datos con BATCH OPTIMIZADO
    print("[3/4] Consultando base de datos (BATCH OPTIMIZADO)...")
    
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
        
        # OPTIMIZACIÓN: Consultas batch de 100 LEGAJOs por vez
        batch_size = 100
        total_batches = (len(legajos) + batch_size - 1) // batch_size
        
        print(f"[INFO] Procesando {total_batches} lotes de {batch_size} LEGAJOs...")
        print("[INFO] Esto será ~25x más rápido que consultas individuales")
        
        for i in range(0, len(legajos), batch_size):
            batch = legajos[i:i+batch_size]
            batch_num = i//batch_size + 1
            
            print(f"  Lote {batch_num}/{total_batches}: {len(batch)} LEGAJOs...", end=" ", flush=True)
            
            start_time = time.time()
            
            # Crear consulta IN() para el lote completo usando tu SQL completa
            placeholders = ','.join(['?' for _ in batch])
            
            batch_query = f"""
            SELECT DISTINCT 
                p.LEGAJO AS COD_PROPUESTA,
                c.NOMBRE as Nombre_Cliente,
                cc.COD_CLIENTE, 
                s.NOMBRE AS SUCURSAL, 
                CR.MONTO,
                ct.FECHA as FECHA_CONTRATO, 
                pvm.NOMBRE as nombre_anterior,
                cc.COD_CONTRATO,
                ec.ESTADO as ESTADO,
                TC.NOMBRE AS TIPO_CONTRATO,
                ed.ESTADO as ESTADO_CREDITO,
                eq.DESCRIPCION as equipo,
                vg.NOMBRE as asesor,
                cart.NOMBRE as Caracter_Titular,
                c.TELEFONO,
                c.TELEFONO_MOVIL,
                c.TELEFONO_LABORAL,
                pa.LEGAJO AS UBICACION,
                DOM.COD_DOMICILIO as DIR_CLIENTE,
                mco.DESCRIPCION as medio_cobro,
                mco.COD_COBRADOR_ASIGNADO,
                zc.COD_MODALIDAD_COBRANZA,
                zc.DESCRIPCION as zona_cobranza,
                mcd.COD_DOMICILIO as codigo_cobro_dom,
                DOM.DIRECCION,
                bar.NOMBRE as barrio,
                loc.NOMBRE as localidad,
                prv.NOMBRE as provinca,
                dom.MANZANA,
                tx.COD_USUARIO_ALTA,
                tx.CODIGO_BARRA as cod_barra_oxxo,
                cb.CLABE as clabe_banco,
                C.TIPO_DOCUMENTO,
                C.NRO_DOCUMENTO,
                C.CUIT AS RFC1,
                mcb.NRO_CUENTA as medio_cobro_cuenta,
                mcb.TITULAR as titular_cobro,
                CASE
                    WHEN cb.CLABE IS NULL THEN '0'
                    ELSE 'C.' || CAST(cb.CLABE AS VARCHAR(50))
                END AS clabe_banco_formatted
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
            ORDER BY p.LEGAJO, c.NOMBRE
            """
            
            try:
                cursor.execute(batch_query, batch)
                results = cursor.fetchall()
                
                # Procesar resultados del lote
                # IMPORTANTE: Tomar solo el PRIMER resultado por LEGAJO
                batch_matches = 0
                legajos_processed = set()
                
                for result in results:
                    legajo = str(result[0])
                    cliente = result[1]
                    
                    # Solo tomar el primer cliente por LEGAJO
                    if legajo not in legajos_processed:
                        matches[legajo] = cliente
                        legajos_processed.add(legajo)
                        batch_matches += 1
                
                elapsed = time.time() - start_time
                print(f"{batch_matches} matches en {elapsed:.1f}s")
                
            except Exception as e:
                print(f"ERROR: {e}")
                
                # Fallback: procesar individualmente si el batch falla
                print(f"    Fallback individual para lote {batch_num}...")
                for legajo in batch:
                    if legajo not in matches:  # Solo si no lo tenemos ya
                        try:
                            individual_query = """
                            SELECT DISTINCT p.LEGAJO, c.NOMBRE 
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
                            WHERE p.LEGAJO = ?
                            ORDER BY c.NOMBRE
                            """
                            cursor.execute(individual_query, (legajo,))
                            result = cursor.fetchone()  # Solo el primero
                            if result:
                                matches[legajo] = result[1]
                        except:
                            pass
        
        con.close()
        print(f"\n[OK] {len(matches)} matches encontrados total")
        
    except Exception as e:
        print(f"[ERROR] Base de datos: {e}")
        return
    
    # 4. Actualizar CSV
    print("[4/4] Actualizando CSV y guardando...")
    
    cliente_col_name = df.columns[5]  # Columna F (CLIENTE)
    current_values = df.get_column(cliente_col_name).to_list()
    
    updates_count = 0
    
    for i in range(10, df.height):  # Desde fila 11
        if i < len(col_b):
            legajo = str(col_b[i]).strip()
            
            if legajo in matches:
                if i < len(current_values):
                    current_val = str(current_values[i]).strip()
                    if not current_val or current_val in ['', 'CLIENTE', 'nan', 'None', 'null']:
                        current_values[i] = matches[legajo]
                        updates_count += 1
    
    # Actualizar DataFrame
    while len(current_values) < df.height:
        current_values.append("")
    
    df = df.with_columns([
        pl.Series(name=cliente_col_name, values=current_values[:df.height])
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
    print(f"LEGAJOs procesados:        {len(legajos):,}")
    print(f"Encontrados en BD:         {len(matches):,}")
    print(f"Registros actualizados:    {updates_count:,}")
    
    efficiency = (len(matches) / len(legajos)) * 100 if len(legajos) > 0 else 0
    print(f"Eficiencia BD:             {efficiency:.1f}%")
    
    if updates_count > 0:
        print(f"\n[EXITO] ¡{updates_count} campos actualizados!")
    else:
        print(f"\n[INFO] Todos los campos ya estaban completos")
    
    print(f"\nArchivo final: {output_file}")

def quick_test():
    """Prueba rápida con solo 10 LEGAJOs para verificar velocidad"""
    print("PRUEBA RAPIDA - Solo 10 LEGAJOs")
    print("=" * 40)
    
    # LEGAJOs de prueba que sabemos que existen
    test_legajos = ['10153', '116090', '116939', '119056', '123518', 
                   '123523', '124469', '124903', '125055', '125725']
    
    try:
        con = fdb.connect(
            host=DATABASE_CONFIG["host"],
            database=DATABASE_CONFIG["database_path"],
            user=DATABASE_CONFIG["user"],
            password=DATABASE_CONFIG["password"],
            charset=DATABASE_CONFIG["charset"]
        )
        
        cursor = con.cursor()
        
        print("[TEST] Consulta batch de 10 LEGAJOs...")
        start_time = time.time()
        
        placeholders = ','.join(['?' for _ in test_legajos])
        query = f"""
        SELECT DISTINCT p.LEGAJO, c.NOMBRE 
        FROM CONTRATOS_CLIENTES cc
        INNER JOIN clientes c ON cc.COD_CLIENTE=c.COD_CLIENTE
        INNER JOIN contratos ct ON cc.COD_CONTRATO=ct.COD_CONTRATO
        INNER JOIN propuesta p ON ct.COD_PROPUESTA=p.COD_PROPUESTA
        INNER JOIN creditos cr ON cr.COD_PROPUESTA = p.COD_PROPUESTA
        INNER JOIN sucursales s ON cr.COD_SUCURSAL=s.COD_SUCURSAL
        WHERE p.LEGAJO IN ({placeholders})
        """
        
        cursor.execute(query, test_legajos)
        results = cursor.fetchall()
        
        elapsed = time.time() - start_time
        con.close()
        
        print(f"[OK] {len(results)} resultados en {elapsed:.2f} segundos")
        print("Ejemplos encontrados:")
        for result in results[:5]:
            print(f"  {result[0]} -> {result[1]}")
        
        estimated_total = (elapsed * 25)  # 25 lotes aproximadamente
        print(f"\n[ESTIMACION] Tiempo total: ~{estimated_total:.1f} segundos")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] {e}")
        return False

def main():
    print("SELECCIONA OPCION:")
    print("1. Prueba rápida (10 LEGAJOs)")
    print("2. Procesamiento completo (2511 LEGAJOs)")
    print("Opción (1/2): ", end="")
    
    choice = input().strip()
    
    if choice == "1":
        quick_test()
    elif choice == "2":
        print("\n[INFO] Iniciando procesamiento completo...")
        print("[INFO] Esto puede tomar 2-5 minutos...")
        process_csv_optimized()
    else:
        print("Opción inválida")

if __name__ == "__main__":
    main()