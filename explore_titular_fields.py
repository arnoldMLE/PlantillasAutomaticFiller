#!/usr/bin/env python3
"""
Script para explorar los valores reales de los campos relacionados con titulares
"""

import fdb
from config import DATABASE_CONFIG

def explore_titular_fields():
    """Explora los valores reales en CARACTER_TITULARES y titular_cobro"""
    print("EXPLORACION DE CAMPOS DE TITULARES")
    print("=" * 60)
    print("Vamos a ver qué valores reales aparecen en:")
    print("1. CARACTER_TITULARES.NOMBRE (cart.NOMBRE)")
    print("2. mcb.TITULAR as titular_cobro")
    print("=" * 60)
    
    # LEGAJOs que sabemos tienen múltiples clientes
    test_legajos = ['10153', '116090', '116939']
    
    try:
        con = fdb.connect(
            host=DATABASE_CONFIG["host"],
            database=DATABASE_CONFIG["database_path"],
            user=DATABASE_CONFIG["user"],
            password=DATABASE_CONFIG["password"],
            charset=DATABASE_CONFIG["charset"]
        )
        
        cursor = con.cursor()
        
        # Consulta que muestra TODOS los resultados para estos LEGAJOs
        # con ambos campos de titulares
        query = """
        SELECT DISTINCT 
            p.LEGAJO,
            c.NOMBRE as Cliente,
            cart.NOMBRE as Caracter_Titular,
            mcb.TITULAR as Titular_Cobro,
            s.NOMBRE AS SUCURSAL,
            CR.MONTO
        FROM CONTRATOS_CLIENTES cc
        INNER JOIN clientes c ON cc.COD_CLIENTE=c.COD_CLIENTE
        INNER JOIN CARACTER_TITULARES cart ON cart.COD_CARACTER_TITULAR = cc.COD_CARACTER_TITULAR
        INNER JOIN contratos ct ON cc.COD_CONTRATO=ct.COD_CONTRATO
        INNER JOIN propuesta p ON ct.COD_PROPUESTA=p.COD_PROPUESTA
        INNER JOIN creditos cr ON cr.COD_PROPUESTA = p.COD_PROPUESTA
        INNER JOIN sucursales s ON cr.COD_SUCURSAL=s.COD_SUCURSAL
        LEFT JOIN MEDIOS_COBROS mco ON mco.COD_CLIENTE = c.COD_CLIENTE
        LEFT OUTER JOIN MEDIOS_COBROS_BANCOS mcb ON mcb.COD_MEDIO_COBRO = mco.COD_MEDIO_COBRO
        WHERE p.LEGAJO IN (?, ?, ?)
        ORDER BY p.LEGAJO, c.NOMBRE
        """
        
        print("[CONSULTA] Obteniendo todos los registros para análisis...")
        cursor.execute(query, test_legajos)
        results = cursor.fetchall()
        
        if not results:
            print("[ERROR] No se encontraron resultados")
            con.close()
            return
        
        print(f"\n[RESULTADOS] {len(results)} registros encontrados:")
        print("-" * 120)
        print(f"{'LEGAJO':<8} {'CLIENTE':<30} {'CARACTER_TITULAR':<20} {'TITULAR_COBRO':<15} {'SUCURSAL':<15} {'MONTO':<10}")
        print("-" * 120)
        
        # Agrupar por LEGAJO para mejor visualización
        current_legajo = None
        legajo_count = 0
        
        for result in results:
            legajo = result[0]
            cliente = result[1][:28] if result[1] else "N/A"
            caracter_titular = result[2] if result[2] else "NULL"
            titular_cobro = result[3] if result[3] else "NULL"
            sucursal = result[4][:13] if result[4] else "N/A"
            monto = f"{result[5]:,.0f}" if result[5] else "0"
            
            # Separador visual entre LEGAJOs
            if current_legajo != legajo:
                if current_legajo is not None:
                    print("-" * 120)
                current_legajo = legajo
                legajo_count = 0
            
            legajo_count += 1
            legajo_display = legajo if legajo_count == 1 else ""
            
            print(f"{legajo_display:<8} {cliente:<30} {caracter_titular:<20} {titular_cobro:<15} {sucursal:<15} {monto:<10}")
        
        con.close()
        
        # Analizar patrones
        print("\n" + "=" * 60)
        print("ANALISIS DE PATRONES")
        print("=" * 60)
        
        # Reconectar para análisis de patrones
        con = fdb.connect(
            host=DATABASE_CONFIG["host"],
            database=DATABASE_CONFIG["database_path"],
            user=DATABASE_CONFIG["user"],
            password=DATABASE_CONFIG["password"],
            charset=DATABASE_CONFIG["charset"]
        )
        cursor = con.cursor()
        
        # Obtener todos los valores únicos de CARACTER_TITULARES
        print("\n[ANALISIS] Valores únicos en CARACTER_TITULARES.NOMBRE:")
        cursor.execute("SELECT DISTINCT NOMBRE FROM CARACTER_TITULARES ORDER BY NOMBRE")
        caracter_values = cursor.fetchall()
        
        for i, (value,) in enumerate(caracter_values, 1):
            print(f"  {i:2d}. '{value}'")
        
        # Obtener muestra de valores de titular_cobro
        print(f"\n[ANALISIS] Muestra de valores en mcb.TITULAR (titular_cobro):")
        cursor.execute("""
            SELECT DISTINCT mcb.TITULAR 
            FROM MEDIOS_COBROS_BANCOS mcb 
            WHERE mcb.TITULAR IS NOT NULL 
            ORDER BY mcb.TITULAR 
            ROWS 20
        """)
        titular_cobro_values = cursor.fetchall()
        
        if titular_cobro_values:
            for i, (value,) in enumerate(titular_cobro_values, 1):
                print(f"  {i:2d}. '{value}'")
        else:
            print("  (No se encontraron valores o todos son NULL)")
        
        con.close()
        
    except Exception as e:
        print(f"[ERROR] {e}")
        return

def analyze_priority_logic():
    """Sugiere lógica de priorización basada en los valores encontrados"""
    print(f"\n{'='*60}")
    print("SUGERENCIAS PARA LOGICA DE PRIORIZACION")
    print(f"{'='*60}")
    print("Basándome en los valores mostrados arriba, podemos usar:")
    print()
    print("OPCION A - Por CARACTER_TITULARES.NOMBRE:")
    print("  1. Buscar valores que contengan palabras clave")
    print("  2. Ejemplo: 'TITULAR', 'PROPIETARIO', 'PRINCIPAL', etc.")
    print()
    print("OPCION B - Por titular_cobro:")
    print("  1. Si tiene valor (no NULL) = prioridad alta")
    print("  2. Si es NULL = prioridad baja")
    print()
    print("OPCION C - Combinada:")
    print("  1. Primero por CARACTER_TITULARES (palabras clave)")
    print("  2. Luego por titular_cobro (no NULL)")
    print("  3. Finalmente por orden alfabético")
    print()
    print("¿Cuál prefieres usar como criterio de priorización?")

def main():
    explore_titular_fields()
    analyze_priority_logic()

if __name__ == "__main__":
    main()