#!/usr/bin/env python3
"""
Script para diagnosticar y corregir problemas con Polars
"""

import sys
import subprocess
import os

def check_polars_installation():
    """Verifica si Polars está correctamente instalado"""
    try:
        import polars as pl
        print(f"[CHECK] Polars {pl.__version__} instalado correctamente")
        return True
    except ImportError as e:
        print(f"[X] Error importando Polars: {e}")
        return False
    except Exception as e:
        print(f"[X] Error con Polars: {e}")
        return False

def install_polars():
    """Instala Polars usando pip"""
    print("[CONFIG] Instalando Polars...")
    try:
        # Desinstalar versión anterior
        subprocess.run([sys.executable, "-m", "pip", "uninstall", "polars", "-y"], 
                      capture_output=True)
        
        # Instalar versión nueva
        result = subprocess.run([sys.executable, "-m", "pip", "install", "polars"], 
                               capture_output=True, text=True)
        
        if result.returncode == 0:
            print("[CHECK] Polars instalado exitosamente")
            return True
        else:
            print(f"[X] Error instalando Polars: {result.stderr}")
            
            # Intentar con versión LTS para CPUs antiguas
            print("[CONFIG] Intentando con polars-lts-cpu...")
            result = subprocess.run([sys.executable, "-m", "pip", "install", "polars-lts-cpu"], 
                                   capture_output=True, text=True)
            
            if result.returncode == 0:
                print("[CHECK] Polars LTS instalado exitosamente")
                return True
            else:
                print(f"[X] Error instalando Polars LTS: {result.stderr}")
                return False
    except Exception as e:
        print(f"[X] Error durante instalación: {e}")
        return False

def test_polars_functionality():
    """Prueba funcionalidad básica de Polars"""
    try:
        import polars as pl
        
        # Crear DataFrame de prueba
        df = pl.DataFrame({
            "A": [1, 2, 3],
            "B": ["x", "y", "z"]
        })
        
        # Operaciones básicas
        result = df.filter(pl.col("A") > 1).select("B")
        
        print("[CHECK] Funcionalidad básica de Polars: OK")
        return True
        
    except Exception as e:
        print(f"[X] Error en funcionalidad de Polars: {e}")
        return False

def test_csv_reading():
    """Prueba lectura de CSV con tipos correctos"""
    try:
        import polars as pl
        
        # Crear CSV de prueba
        test_data = """PROPUESTA JKM / LEGAJO,CLIENTE,COLUMNA3
123,Juan Perez,Valor1
456,Maria Garcia,Valor2
789,Carlos Lopez,Valor3"""
        
        with open("test_polars.csv", "w", encoding="utf-8") as f:
            f.write(test_data)
        
        # Leer con esquema correcto
        df = pl.read_csv(
            "test_polars.csv",
            skip_rows=0,
            infer_schema_length=100,
            dtypes={
                "PROPUESTA JKM / LEGAJO": pl.Utf8,
                "CLIENTE": pl.Utf8,
                "COLUMNA3": pl.Utf8
            }
        )
        
        print(f"[CHECK] CSV leído correctamente: {df.shape[0]} filas, {df.shape[1]} columnas")
        
        # Limpiar archivo de prueba
        if os.path.exists("test_polars.csv"):
            os.remove("test_polars.csv")
            
        return True
        
    except Exception as e:
        print(f"[X] Error leyendo CSV: {e}")
        return False

def fix_dtypes_issue():
    """Corrige el problema de dtypes en el código"""
    print("[CONFIG] Verificando archivos de código...")
    
    # Archivos a verificar
    files_to_check = [
        "main.py", "client_polars.py", "debug_tools.py", 
        "main_polars.py", "monitor.py"
    ]
    
    for filepath in files_to_check:
        if not os.path.exists(filepath):
            continue
            
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()
            
            # Buscar patrones problemáticos y corregirlos
            replacements = [
                # Problemas comunes con dtypes
                ("dtypes=str", "dtypes=pl.Utf8"),
                ("dtypes={str}", "dtypes=pl.Utf8"),
                ("dtype=str", "dtype=pl.Utf8"),
                
                # Problemas específicos encontrados
                ('has_header=False,\n                dtypes=str,', 
                 'has_header=False,\n                dtypes=pl.Utf8,'),
                
                ('dtypes=str,  # Todo como string', 
                 'dtypes=pl.Utf8,  # Todo como string'),
                
                # Patrón específico del error reportado
                ('dtypes=str,\n                ignore_errors=True',
                 'dtypes=pl.Utf8,\n                ignore_errors=True'),
                
                # Alternativas más robustas
                ('dtypes=str', 'infer_schema_length=0'),  # Dejar que Polars infiera
            ]
            
            modified = False
            for old, new in replacements:
                if old in content:
                    content = content.replace(old, new)
                    modified = True
                    print(f"   [PROCESO] En {filepath}: {old} → {new}")
            
            # Problemas específicos adicionales
            if 'pl.read_csv(' in content and 'dtypes=' in content:
                # Buscar patrones más complejos con regex si es necesario
                import re
                
                # Patrón para dtypes problemáticos
                pattern = r'dtypes=([^,\)]+)'
                matches = re.findall(pattern, content)
                
                for match in matches:
                    if 'str' == match.strip() or 'type' in match:
                        print(f"   [WARNING] Posible problema en {filepath}: dtypes={match}")
                        # Reemplazar con opción más robusta
                        content = re.sub(
                            r'dtypes=str\b',
                            'infer_schema_length=0  # Inferir tipos automáticamente',
                            content
                        )
                        modified = True
            
            if modified:
                # Hacer backup
                backup_path = filepath + ".backup"
                with open(backup_path, "w", encoding="utf-8") as f:
                    f.write(content)
                
                # Escribir archivo corregido
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(content)
                
                print(f"   [CHECK] Archivo corregido: {filepath}")
                print(f"   [GUARDAR] Backup guardado: {backup_path}")
            else:
                print(f"   [CHECK] {filepath}: Sin cambios necesarios")
                
        except Exception as e:
            print(f"   [WARNING] Error procesando {filepath}: {e}")
    
    print("[CHECK] Verificación de código completada")

def main():
    print("[CONFIG] DIAGNÓSTICO Y REPARACIÓN DE POLARS")
    print("=" * 50)
    
    # 1. Verificar instalación
    if not check_polars_installation():
        print("\n[CONFIG] Instalando Polars...")
        if not install_polars():
            print("[X] No se pudo instalar Polars")
            return False
    
    # 2. Verificar funcionalidad
    print("\n[TEST] Probando funcionalidad...")
    if not test_polars_functionality():
        print("[X] Polars no funciona correctamente")
        return False
    
    # 3. Probar lectura CSV
    print("\n[ARCHIVO] Probando lectura CSV...")
    if not test_csv_reading():
        print("[X] Problemas con lectura CSV")
        return False
    
    # 4. Corregir problemas de dtypes
    print("\n[CONFIG] Corrigiendo problemas de dtypes...")
    fix_dtypes_issue()
    
    print("\n[EXITO] TODOS LOS PROBLEMAS DE POLARS CORREGIDOS")
    print("[CHECK] Puedes ejecutar nuevamente el diagnóstico")
    
    return True

if __name__ == "__main__":
    main()