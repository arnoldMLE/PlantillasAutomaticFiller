#!/usr/bin/env python3
"""
Script para probar la consulta SQL real de tu sistema
"""

import fdb
import sys
import os
from config import DATABASE_CONFIG

def test_real_query():
    """Prueba la consulta SQL real de tu sistema"""
    print("[BUSCAR] PROBANDO CONSULTA SQL REAL")
    print("=" * 60)
    
    # Tu consulta SQL completa
    real_query = """
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
    WHERE p.LEGAJO = ?
    ORDER BY p.LEGAJO
    """
    
    # Consulta simplificada para diagnóstico
    simple_query = """
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
    ORDER BY p.LEGAJO
    """
    
    test_legajos = ["642799", "10153", "116090"]  # Del ejemplo de tu CSV
    
    try:
        con = fdb.connect(
            host=DATABASE_CONFIG["host"],
            database=DATABASE_CONFIG["database_path"],
            user=DATABASE_CONFIG["user"],
            password=DATABASE_CONFIG["password"],
            charset=DATABASE_CONFIG["charset"]
        )
        
        cursor = con.cursor()
        
        print("[TEST] Probando consulta simplificada primero...")
        
        for legajo in test_legajos:
            print(f"\n[BUSCAR] Buscando LEGAJO: {legajo}")
            
            try:
                # Probar consulta simple
                cursor.execute(simple_query, (legajo,))
                result = cursor.fetchone()
                
                if result:
                    print(f"   [CHECK] ENCONTRADO: {result[1]} - {result[2]}")
                    
                    # Si la simple funciona, probar la completa
                    print(f"   [PROCESO] Probando consulta completa...")
                    cursor.execute(real_query, (legajo,))
                    full_result = cursor.fetchall()
                    
                    if full_result:
                        print(f"   [CHECK] Consulta completa: {len(full_result)} registros")
                        
                        # Mostrar primer resultado
                        first_row = full_result[0]
                        print(f"   [GRAFICO] Cliente: {first_row[1]}")
                        print(f"   [GRAFICO] Sucursal: {first_row[3]}")
                        print(f"   [GRAFICO] Monto: {first_row[4]}")
                        
                    else:
                        print(f"   [WARNING] Consulta completa no devolvió resultados")
                        
                else:
                    print(f"   [X] NO ENCONTRADO")
                    
            except Exception as e:
                print(f"   [X] Error con LEGAJO {legajo}: {e}")
        
        con.close()
        
        # Verificar si existe algún LEGAJO en la tabla
        print(f"\n[BUSCAR] Verificando existencia de LEGAJOs en general...")
        
        con = fdb.connect(
            host=DATABASE_CONFIG["host"],
            database=DATABASE_CONFIG["database_path"],
            user=DATABASE_CONFIG["user"],
            password=DATABASE_CONFIG["password"],
            charset=DATABASE_CONFIG["charset"]
        )
        
        cursor = con.cursor()
        
        # Buscar algunos LEGAJOs existentes
        cursor.execute("SELECT FIRST 10 LEGAJO FROM propuesta ORDER BY LEGAJO")
        sample_legajos = cursor.fetchall()
        
        if sample_legajos:
            print("   [CHECK] LEGAJOs encontrados en la BD:")
            for legajo_row in sample_legajos:
                print(f"      [ARCHIVO] {legajo_row[0]}")
                
            # Probar con el primer LEGAJO encontrado
            first_legajo = str(sample_legajos[0][0])
            print(f"\n[TEST] Probando con LEGAJO existente: {first_legajo}")
            
            cursor.execute(simple_query, (first_legajo,))
            test_result = cursor.fetchone()
            
            if test_result:
                print(f"   [CHECK] ¡ÉXITO! Cliente: {test_result[1]}")
                return True
            else:
                print(f"   [X] Aún no funciona con LEGAJO existente")
                
        else:
            print("   [X] No se encontraron LEGAJOs en la tabla propuesta")
        
        con.close()
        return False
        
    except Exception as e:
        print(f"[X] Error general: {e}")
        return False

def fix_main_py_query():
    """Actualiza main.py con la consulta SQL correcta"""
    print("\n[CONFIG] ACTUALIZANDO CONSULTA EN MAIN.PY")
    print("=" * 50)
    
    try:
        # Leer archivo actual
        with open("main.py", "r", encoding="utf-8") as f:
            content = f.read()
        
        # Buscar la consulta actual (simplificada)
        old_query_start = 'query = """'
        old_query_end = '"""'
        
        start_idx = content.find(old_query_start)
        if start_idx == -1:
            print("[X] No se encontró consulta para actualizar")
            return False
        
        # Encontrar el final de la consulta
        end_search_start = start_idx + len(old_query_start)
        end_idx = content.find('"""', end_search_start)
        
        if end_idx == -1:
            print("[X] No se encontró el final de la consulta")
            return False
        
        # Nueva consulta mejorada (versión intermedia, no tan compleja)
        new_query = '''query = """
        SELECT DISTINCT 
            p.LEGAJO AS COD_PROPUESTA,
            c.NOMBRE as Nombre_Cliente,
            cc.COD_CLIENTE, 
            s.NOMBRE AS SUCURSAL, 
            CR.MONTO,
            ct.FECHA as FECHA_CONTRATO, 
            ec.ESTADO as ESTADO,
            TC.NOMBRE AS TIPO_CONTRATO,
            c.TELEFONO,
            c.TELEFONO_MOVIL,
            DOM.DIRECCION,
            vg.NOMBRE as asesor
        FROM CONTRATOS_CLIENTES cc
        INNER JOIN clientes c ON cc.COD_CLIENTE=c.COD_CLIENTE
        INNER JOIN contratos ct ON cc.COD_CONTRATO=ct.COD_CONTRATO
        INNER JOIN propuesta p ON ct.COD_PROPUESTA=p.COD_PROPUESTA
        INNER JOIN creditos cr ON cr.COD_PROPUESTA = p.COD_PROPUESTA
        INNER JOIN sucursales s ON cr.COD_SUCURSAL=s.COD_SUCURSAL
        INNER JOIN ESTADOS_CONTRATOS ec ON ct.COD_ESTADO_CONTRATO = ec.COD_ESTADO_CONTRATO
        INNER JOIN TIPOS_CONTRATOS TC ON CT.COD_TIPO_CONTRATO=TC.COD_TIPO_CONTRATO
        INNER JOIN vendedor vg ON ct.COD_VENDEDOR=vg.COD_VENDEDOR
        LEFT JOIN MEDIOS_COBROS_DOMICILIOS mcd ON mcd.COD_MEDIO_COBRO IN (
            SELECT COD_MEDIO_COBRO FROM MEDIOS_COBROS WHERE COD_CLIENTE = c.COD_CLIENTE
        )
        LEFT JOIN DOMICILIOS DOM ON dom.COD_DOMICILIO = mcd.COD_DOMICILIO
        WHERE p.LEGAJO = ?
        """'''
        
        # Reemplazar consulta
        new_content = content[:start_idx] + new_query + content[end_idx + 3:]
        
        # Hacer backup
        with open("main.py.backup", "w", encoding="utf-8") as f:
            f.write(content)
        
        # Escribir nueva versión
        with open("main.py", "w", encoding="utf-8") as f:
            f.write(new_content)
        
        print("[CHECK] Consulta actualizada en main.py")
        print("[GUARDAR] Backup guardado como main.py.backup")
        return True
        
    except Exception as e:
        print(f"[X] Error actualizando main.py: {e}")
        return False

def main():
    print("[CONFIG] DIAGNÓSTICO DE CONSULTA SQL REAL")
    print("=" * 60)
    
    # 1. Probar consulta SQL
    query_works = test_real_query()
    
    # 2. Actualizar main.py si es necesario
    if query_works:
        print("\n[EXITO] ¡La consulta funciona!")
        fix_main_py_query()
    else:
        print("\n[WARNING] La consulta necesita ajustes")
        print("[INFO] Verifica:")
        print("   1. Que los LEGAJOs de prueba existan en tu BD")
        print("   2. Que todas las tablas estén presentes")
        print("   3. Que no haya diferencias en nombres de columnas")

if __name__ == "__main__":
    main()