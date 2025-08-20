#!/usr/bin/env python3
"""
Script para corregir problemas de codificación Unicode en Windows
Remueve emojis y los reemplaza con texto simple
"""

import os
import re
import sys

def fix_encoding_in_file(filepath):
    """Corrige problemas de codificación en un archivo específico"""
    if not os.path.exists(filepath):
        return False
    
    print(f"Procesando: {filepath}")
    
    try:
        # Leer con diferentes codificaciones
        content = None
        for encoding in ['utf-8', 'utf-8-sig', 'latin1', 'cp1252']:
            try:
                with open(filepath, 'r', encoding=encoding) as f:
                    content = f.read()
                break
            except UnicodeDecodeError:
                continue
        
        if content is None:
            print(f"  ERROR: No se pudo leer {filepath}")
            return False
        
        # Reemplazos de emojis por texto
        emoji_replacements = {
            '🚀': '[INICIO]',
            '✅': '[OK]',
            '❌': '[ERROR]',
            '⚠️': '[AVISO]',
            '🔧': '[CONFIG]',
            '📦': '[INSTALL]',
            '🔍': '[BUSCAR]',
            '📊': '[DATOS]',
            '🧪': '[TEST]',
            '💡': '[INFO]',
            '📋': '[LISTA]',
            '📄': '[ARCHIVO]',
            '📁': '[CARPETA]',
            '🎯': '[TARGET]',
            '🎉': '[EXITO]',
            '🔄': '[PROCESO]',
            '💾': '[GUARDAR]',
            '🔌': '[CONEXION]',
            '⏱️': '[TIEMPO]',
            '📝': '[NOTA]',
            '🛠️': '[HERRAMIENTAS]',
            '🧩': '[CHUNKS]',
            '📈': '[CHART]',
            '🔗': '[LINK]',
            '⌨️': '[TECLADO]',
            '🛑': '[STOP]',
            '👋': '[SALUDO]',
            '🥇': '[PRIMERO]',
            '💻': '[PC]',
            '📺': '[MONITOR]',
            '🔋': '[BATERIA]',
            '⬆️': '[UP]',
            '⬇️': '[DOWN]',
            '➡️': '[DERECHA]',
            '⬅️': '[IZQUIERDA]',
            '🌟': '[ESTRELLA]',
            '💪': '[FUERZA]',
            '🎪': '[CIRCO]',
            '🔥': '[FUEGO]',
            '⚡': '[RAYO]',
            '💯': '[100]',
            '🎮': '[JUEGO]',
            '🎨': '[ARTE]',
            '🍕': '[PIZZA]',
            '☕': '[CAFE]',
            '🍺': '[CERVEZA]',
            '🎵': '[MUSICA]',
            '📱': '[MOVIL]',
            '💰': '[DINERO]',
            '🏆': '[TROFEO]',
            '🎪': '[EVENTO]',
            '🚗': '[AUTO]',
            '✈️': '[AVION]',
            '🏠': '[CASA]',
            '🌍': '[MUNDO]',
            '🔮': '[CRISTAL]',
            '⭐': '[STAR]',
            '🔴': '[ROJO]',
            '🟢': '[VERDE]',
            '🟡': '[AMARILLO]',
            '🔵': '[AZUL]',
            '⚫': '[NEGRO]',
            '⚪': '[BLANCO]',
            # Emojis específicos del código
            '\U0001f6e0\ufe0f': '[HERRAMIENTAS]',
            '\U0001f4ca': '[GRAFICO]',
            '\U0001f680': '[COHETE]',
            '\u2705': '[CHECK]',
            '\u274c': '[X]',
            '\u26a0\ufe0f': '[WARNING]',
        }
        
        # Aplicar reemplazos
        original_content = content
        for emoji, replacement in emoji_replacements.items():
            content = content.replace(emoji, replacement)
        
        # Remover cualquier otro emoji usando regex
        # Patrón para detectar emojis Unicode
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"  # emoticons
            "\U0001F300-\U0001F5FF"  # símbolos & pictogramas
            "\U0001F680-\U0001F6FF"  # transporte & símbolos de mapa
            "\U0001F1E0-\U0001F1FF"  # banderas (iOS)
            "\U00002500-\U00002BEF"  # chinese char
            "\U00002702-\U000027B0"
            "\U00002702-\U000027B0"
            "\U000024C2-\U0001F251"
            "\U0001f926-\U0001f937"
            "\U00010000-\U0010ffff"
            "\u2640-\u2642"
            "\u2600-\u2B55"
            "\u200d"
            "\u23cf"
            "\u23e9"
            "\u231a"
            "\ufe0f"  # dingbats
            "\u3030"
            "]+", flags=re.UNICODE)
        
        content = emoji_pattern.sub('[EMOJI]', content)
        
        # Escribir archivo corregido
        if content != original_content:
            # Hacer backup
            backup_path = filepath + '.emoji_backup'
            with open(backup_path, 'w', encoding='utf-8') as f:
                f.write(original_content)
            
            # Escribir versión corregida
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print(f"  [OK] Corregido: {filepath}")
            print(f"  [BACKUP] Guardado: {backup_path}")
            return True
        else:
            print(f"  [OK] Sin cambios necesarios: {filepath}")
            return True
            
    except Exception as e:
        print(f"  [ERROR] {filepath}: {e}")
        return False

def test_polars_simple():
    """Prueba simple de Polars sin emojis"""
    print("\n[TEST] Probando Polars...")
    
    try:
        import polars as pl
        
        # Test básico
        df = pl.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })
        
        result = df.filter(pl.col('col1') > 1)
        
        print(f"[OK] Polars {pl.__version__} funciona correctamente")
        print(f"[INFO] Test: {result.height} filas filtradas")
        return True
        
    except ImportError:
        print("[ERROR] Polars no está instalado")
        return False
    except Exception as e:
        print(f"[ERROR] Polars: {e}")
        return False

def install_polars_clean():
    """Instalación limpia de Polars"""
    print("\n[INSTALL] Instalando Polars...")
    
    import subprocess
    
    try:
        # Desinstalar cualquier versión existente
        subprocess.run([sys.executable, "-m", "pip", "uninstall", "polars", "-y"], 
                      capture_output=True)
        subprocess.run([sys.executable, "-m", "pip", "uninstall", "polars-lts-cpu", "-y"], 
                      capture_output=True)
        
        # Instalar versión estable
        result = subprocess.run([sys.executable, "-m", "pip", "install", "polars==0.20.2"], 
                               capture_output=True, text=True)
        
        if result.returncode == 0:
            print("[OK] Polars 0.20.2 instalado")
            return test_polars_simple()
        else:
            # Probar con LTS
            result = subprocess.run([sys.executable, "-m", "pip", "install", "polars-lts-cpu"], 
                                   capture_output=True, text=True)
            
            if result.returncode == 0:
                print("[OK] Polars LTS instalado")
                return test_polars_simple()
            else:
                print(f"[ERROR] No se pudo instalar Polars: {result.stderr}")
                return False
                
    except Exception as e:
        print(f"[ERROR] Instalación: {e}")
        return False

def main():
    print("REPARACION DE CODIFICACION UNICODE PARA WINDOWS")
    print("=" * 60)
    
    # Archivos a corregir
    files_to_fix = [
        "debug_tools.py",
        "main_polars.py", 
        "client_polars.py",
        "monitor.py",
        "fix_polars_issues.py",
        "fix_csv_issues.py",
        "fix_database_issues.py",
        "fix_real_query.py"
    ]
    
    print("\n[CONFIG] Corrigiendo archivos...")
    
    fixed_count = 0
    for filepath in files_to_fix:
        if fix_encoding_in_file(filepath):
            fixed_count += 1
    
    print(f"\n[RESULTADO] {fixed_count}/{len(files_to_fix)} archivos procesados")
    
    # Instalar Polars limpio
    print("\n[INSTALL] Verificando Polars...")
    if test_polars_simple():
        print("[OK] Polars ya funciona")
    else:
        install_polars_clean()
    
    # Test final
    print("\n[TEST] Ejecutando diagnóstico simple...")
    
    try:
        # Ejecutar debug_tools con comando específico
        import subprocess
        result = subprocess.run([sys.executable, "debug_tools.py", "diagnostic"], 
                               capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("[OK] Diagnóstico ejecutado correctamente")
            print("SALIDA:")
            print(result.stdout[-500:])  # Últimas 500 chars
        else:
            print("[ERROR] Diagnóstico falló")
            print("ERROR:")
            print(result.stderr[-300:])  # Últimos 300 chars de error
            
    except Exception as e:
        print(f"[ERROR] No se pudo ejecutar diagnóstico: {e}")
    
    print("\n" + "=" * 60)
    print("[COMPLETADO] Reparación de codificación finalizada")
    print("[INFO] Ahora puedes ejecutar: python main_polars.py")
    print("=" * 60)

if __name__ == "__main__":
    main()