@echo off
chcp 65001 >nul
title CSV-Firebird Server

echo.
echo ████████████████████████████████████████████
echo █  🚀 Iniciando servidor FastAPI + Polars  █
echo ████████████████████████████████████████████
echo.

REM Verificar que estamos en el directorio correcto
if not exist "venv" (
    echo ❌ Entorno virtual no encontrado
    echo 💡 Ejecuta install.bat primero
    pause
    exit /b 1
)

REM Activar entorno virtual
echo 🔋 Activando entorno virtual...
call venv\Scripts\activate.bat

REM Verificar configuración
if not exist "config.py" (
    echo ⚠️  config.py no encontrado, creando plantilla...
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
    echo ✅ Plantilla config.py creada
    echo 💡 Edítala con tus datos de conexión
)

REM Verificar archivo principal
if not exist "main.py" (
    echo ❌ main.py no encontrado
    echo 💡 Copia el código principal del sistema
    pause
    exit /b 1
)

echo.
echo 🔍 Verificando dependencias...
python -c "import fastapi, polars, fdb; print('✅ Dependencias OK')" 2>nul
if errorlevel 1 (
    echo ❌ Faltan dependencias
    echo 💡 Ejecuta install.bat nuevamente
    pause
    exit /b 1
)

echo.
echo 🚀 Iniciando servidor...
echo.
echo 🔗 URLs disponibles:
echo    • API: http://localhost:8000
echo    • Docs: http://localhost:8000/docs  
echo    • Health: http://localhost:8000/health
echo.
echo ⌨️ Presiona Ctrl+C para detener
echo ═══════════════════════════════════════════
echo.

python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload

echo.
echo 🛑 Servidor detenido
pause