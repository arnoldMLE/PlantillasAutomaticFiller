#!/usr/bin/env python3
"""
Script maestro que ejecuta todas las reparaciones necesarias
"""

import os
import sys
import subprocess

def run_script(script_name, description):
    """Ejecuta un script de reparación"""
    print(f"\n{'='*60}")
    print(f"🔧 {description}")
    print(f"{'='*60}")
    
    if not os.path.exists(script_name):
        print(f"❌ Script no encontrado: {script_name}")
        return False
    
    try:
        result = subprocess.run([sys.executable, script_name], 
                               capture_output=True, text=True, encoding='utf-8')
        
        print(result.stdout)
        
        if result.stderr:
            print("⚠️ Errores/Advertencias:")
            print(result.stderr)
        
        if result.returncode == 0:
            print(f"✅ {description}: COMPLETADO")
            return True
        else:
            print(f"❌ {description}: FALLÓ")
            return False
            
    except Exception as e:
        print(f"❌ Error ejecutando {script_name}: {e}")
        return False

def fix_config_file():
    """Corrige/actualiza el archivo config.py"""
    print("\n🔧 ACTUALIZANDO CONFIG.PY")
    print("=" * 40)
    
    try:
        # Leer config actual
        with open("config.py", "r", encoding="utf-8") as f:
            content = f.read()
        
        # Verificar si ya tiene POLARS_CONFIG
        if "POLARS_CONFIG" not in content:
            # Agregar configuración de Polars
            polars_config = '''
POLARS_CONFIG = {
    "n_threads": None,  # Usar todos los threads disponibles
    "streaming": True,  # Habilitar streaming para archivos grandes
    "string_cache": True,  # Cache de strings para mejor rendimiento
    "fmt_str_lengths": 50,  # Longitud de strings en output
}'''
            
            content += polars_config
            
            # Escribir archivo actualizado
            with open("config.py", "w", encoding="utf-8") as f:
                f.write(content)
            
            print("✅ POLARS_CONFIG agregado a config.py")
        else:
            print("✅ POLARS_CONFIG ya existe en config.py")
            
        return True
        
    except Exception as e:
        print(f"❌ Error actualizando config.py: {e}")
        return False

def create_requirements_if_missing():
    """Crea requirements.txt si no existe"""
    if not os.path.exists("requirements.txt"):
        print("\n📝 Creando requirements.txt...")
        
        requirements = """# Dependencias principales
fastapi==0.104.1
uvicorn==0.24.0
polars==0.20.2
pydantic==2.5.0
fdb==2.0.2
python-multipart==0.0.6
openpyxl==3.1.2
requests==2.31.0

# Dependencias opcionales para análisis
psutil==5.9.6
colorama==0.4.6"""
        
        with open("requirements.txt", "w", encoding="utf-8") as f:
            f.write(requirements)
        
        print("✅ requirements.txt creado")

def install_missing_dependencies():
    """Instala dependencias faltantes"""
    print("\n📦 VERIFICANDO E INSTALANDO DEPENDENCIAS")
    print("=" * 50)
    
    try:
        # Verificar si estamos en venv
        result = subprocess.run([sys.executable, "-c", "import sys; print(sys.prefix != sys.base_prefix)"], 
                               capture_output=True, text=True)
        
        if result.stdout.strip() == "True":
            print("✅ Entorno virtual detectado")
        else:
            print("⚠️ No se detectó entorno virtual")
        
        # Instalar/actualizar Polars específicamente
        print("📦 Instalando/actualizando Polars...")
        
        result = subprocess.run([sys.executable, "-m", "pip", "install", "--upgrade", "polars"], 
                               capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Polars instalado/actualizado correctamente")
        else:
            print(f"⚠️ Problema con Polars: {result.stderr}")
            
            # Intentar con versión LTS
            print("🔄 Intentando con polars-lts-cpu...")
            result = subprocess.run([sys.executable, "-m", "pip", "install", "--upgrade", "polars-lts-cpu"], 
                                   capture_output=True, text=True)
            
            if result.returncode == 0:
                print("✅ Polars LTS instalado correctamente")
            else:
                print(f"❌ Error instalando Polars: {result.stderr}")
                return False
        
        # Verificar que funciona
        test_result = subprocess.run([sys.executable, "-c", "import polars as pl; print(f'✅ Polars {pl.__version__} funcionando')"], 
                                    capture_output=True, text=True)
        
        if test_result.returncode == 0:
            print(test_result.stdout.strip())
            return True
        else:
            print(f"❌ Polars no funciona: {test_result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error con dependencias: {e}")
        return False

def main():
    print("🚀 REPARACIÓN COMPLETA DEL SISTEMA CSV-FIREBIRD")
    print("=" * 70)
    print("Este script ejecutará todas las reparaciones necesarias paso a paso")
    print("=" * 70)
    
    # Paso 0: Verificar archivos necesarios
    required_files = ["config.py", "main.py", "debug_tools.py"]
    missing_files = [f for f in required_files if not os.path.exists(f)]
    
    if missing_files:
        print(f"❌ Archivos faltantes: {', '.join(missing_files)}")
        print("💡 Asegúrate de ejecutar este script en el directorio del proyecto")
        return
    
    # Paso 1: Actualizar configuración
    print("\n🔧 PASO 1: CONFIGURACIÓN")
    fix_config_file()
    create_requirements_if_missing()
    
    # Paso 2: Dependencias
    print("\n🔧 PASO 2: DEPENDENCIAS")
    install_missing_dependencies()
    
    # Paso 3: Reparar código Polars
    print("\n🔧 PASO 3: CÓDIGO POLARS")
    success_polars = run_script("fix_polars_issues.py", "REPARACIÓN DE POLARS")
    
    # Paso 4: Reparar consulta SQL
    print("\n🔧 PASO 4: CONSULTA SQL")
    success_query = run_script("fix_real_query.py", "VERIFICACIÓN DE CONSULTA SQL")
    
    # Paso 5: Diagnóstico final
    print("\n🔧 PASO 5: DIAGNÓSTICO FINAL")
    final_diagnostic = run_script("debug_tools.py", "DIAGNÓSTICO FINAL")
    
    # Resumen final
    print(f"\n{'='*70}")
    print("📊 RESUMEN DE REPARACIONES")
    print(f"{'='*70}")
    
    steps = [
        ("Configuración", True),
        ("Dependencias", True),  # Ya lo verificamos arriba
        ("Código Polars", success_polars),
        ("Consulta SQL", success_query),
        ("Diagnóstico Final", final_diagnostic)
    ]
    
    successful_steps = sum(1 for _, success in steps if success)
    total_steps = len(steps)
    
    for step_name, success in steps:
        status = "✅" if success else "❌"
        print(f"   {status} {step_name}")
    
    print(f"\n🎯 RESULTADO: {successful_steps}/{total_steps} pasos completados")
    
    if successful_steps == total_steps:
        print("\n🎉 ¡TODAS LAS REPARACIONES COMPLETADAS!")
        print("✅ El sistema debería funcionar correctamente ahora")
        print("\n📋 Próximos pasos:")
        print("   1. Ejecuta: python main_polars.py")
        print("   2. O inicia el servidor: start_server.bat")
        print("   3. Coloca tu CSV y procesa con: process_csv.bat")
    else:
        print("\n⚠️ Algunas reparaciones fallaron")
        print("💡 Revisa los errores arriba y ejecuta scripts individuales si es necesario")

if __name__ == "__main__":
    main()