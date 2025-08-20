@echo off
chcp 65001 >nul
title Configuración Completa Guiada

echo.
echo ████████████████████████████████████████████████████████
echo █  🎯 CONFIGURACIÓN COMPLETA GUIADA                    █
echo █  Setup paso a paso para usar el sistema              █
echo ████████████████████████████████████████████████████████
echo.

echo 🚀 BIENVENIDO AL SISTEMA CSV-FIREBIRD AUTOMATION
echo.
echo Este asistente te guiará para configurar completamente el sistema
echo con máximo rendimiento usando Polars.
echo.

pause

REM Paso 1: Verificar Python
echo.
echo 1️⃣ VERIFICANDO PYTHON...
echo ═══════════════════════════
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python no encontrado
    echo.
    echo 📥 ACCIÓN REQUERIDA:
    echo    1. Descargar Python 3.9+ desde: https://www.python.org/downloads/
    echo    2. Durante instalación, marcar "Add Python to PATH"
    echo    3. Reiniciar esta ventana después de instalar
    echo.
    pause
    exit /b 1
)

for /f "tokens=2" %%i in ('python --version') do set PYTHON_VERSION=%%i
echo ✅ Python %PYTHON_VERSION% encontrado
echo.

REM Paso 2: Instalar sistema
echo 2️⃣ INSTALANDO SISTEMA...
echo ═══════════════════════════
if not exist "venv" (
    echo 📦 Instalando entorno virtual y dependencias...
    call install.bat
) else (
    echo ✅ Sistema ya instalado
)
echo.

REM Paso 3: Configurar base de datos
echo 3️⃣ CONFIGURANDO BASE DE DATOS...
echo ═══════════════════════════════════
if exist "config.py" (
    echo ✅ Archivo config.py existe
    echo.
    echo 📋 ¿Quieres revisar/editar la configuración? (s/n): 
    set /p EDIT_CONFIG=
    
    if /i "%EDIT_CONFIG%"=="s" (
        echo 📝 Abriendo editor...
        notepad config.py
    )
) else (
    echo 📝 Creando configuración inicial...
    (
    echo # config.py - Configuración del sistema
    echo DATABASE_CONFIG = {
    echo     "host": "localhost",
    echo     "database_path": r"C:\ruta\completa\a\tu\base.fdb",
    echo     "user": "SYSDBA",
    echo     "password": "masterkey",
    echo     "charset": "UTF8"
    echo }
    echo.
    echo CSV_CONFIG = {
    echo     "target_column": "CLIENTE",
    echo     "data_start_row": 11,
    echo     "propuesta_column": "B",
    echo     "chunk_size": 10000,
    echo     "use_streaming": True
    echo }
    ) > config.py
    
    echo ✅ Configuración creada
    echo ⚠️  IMPORTANTE: Edita config.py con tus datos reales
    echo.
    notepad config.py
)
echo.

REM Paso 4: Probar conexión
echo 4️⃣ PROBANDO CONEXIÓN...
echo ═════════════════════════
call venv\Scripts\activate.bat

echo 🧪 Ejecutando diagnóstico...
python debug_tools.py diagnostic

echo.
echo 📋 ¿El diagnóstico fue exitoso? (s/n): 
set /p DIAG_OK=

if not /i "%DIAG_OK%"=="s" (
    echo ⚠️ Hay problemas en la configuración
    echo 💡 Revisa los errores del diagnóstico y ejecuta este script nuevamente
    pause
    exit /b 1
)

echo.

REM Paso 5: Preparar archivos CSV
echo 5️⃣ PREPARANDO ARCHIVOS CSV...
echo ════════════════════════════════
echo 📁 Directorio actual: %CD%
echo.

REM Buscar archivos CSV
set CSV_COUNT=0
for %%f in (*.csv) do (
    set /a CSV_COUNT+=1
)

if %CSV_COUNT%==0 (
    echo ❌ No se encontraron archivos CSV
    echo.
    echo 📋 ACCIÓN REQUERIDA:
    echo    1. Copia tu archivo CSV a este directorio: %CD%
    echo    2. Asegúrate de que tenga el formato correcto:
    echo       - Datos empiezan en fila 11
    echo       - Columna B contiene PROPUESTA JKM / LEGAJO
    echo    3. Ejecuta este script nuevamente
    echo.
    
    echo 📝 ¿Quieres crear un archivo CSV de ejemplo? (s/n): 
    set /p CREATE_SAMPLE=
    
    if /i "%CREATE_SAMPLE%"=="s" (
        python -c "
from debug_tools import create_sample_csv
create_sample_csv()
print('✅ Archivo ejemplo_propuestas.csv creado')
print('💡 Úsalo como referencia para el formato')
"
    )
    
    pause
    exit /b 1
) else (
    echo ✅ Encontrados %CSV_COUNT% archivos CSV
    echo.
    echo 📁 Archivos encontrados:
    for %%f in (*.csv) do (
        echo    📄 %%f
    )
)

echo.

REM Paso 6: Finalizar configuración
echo 6️⃣ FINALIZANDO CONFIGURACIÓN...
echo ══════════════════════════════════
echo ✅ Sistema completamente configurado!
echo.
echo 🎯 PRÓXIMOS PASOS:
echo    1. Para procesar UN archivo:    process_csv.bat
echo    2. Para procesar MÚLTIPLES:     batch_process.bat
echo    3. Para monitorear sistema:     monitor.bat
echo    4. Para diagnóstico:            debug.bat
echo    5. Para mantenimiento:          maintenance.bat
echo.
echo 📖 DOCUMENTACIÓN:
echo    • Inicia el servidor: start_server.bat
echo    • Ve a: http://localhost:8000/docs
echo.

echo 🚀 ¿Quieres procesar un archivo CSV ahora? (s/n): 
set /p PROCESS_NOW=

if /i "%PROCESS_NOW%"=="s" (
    echo.
    echo 🔄 Iniciando procesamiento...
    call process_csv.bat
)

echo.
echo 🎉 CONFIGURACIÓN COMPLETADA EXITOSAMENTE!
echo 💡 Guarda este directorio como tu entorno de trabajo
echo.
pause