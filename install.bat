@echo off
chcp 65001 >nul
title CSV-Firebird Automation - Instalación

echo.
echo ████████████████████████████████████████████████████████
echo █  🚀 CSV-Firebird Automation con Polars - Windows     █
echo █  Instalación automática para máximo rendimiento      █
echo ████████████████████████████████████████████████████████
echo.

REM Verificar Python
echo 🔍 Verificando Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python no encontrado
    echo.
    echo 📥 Descarga Python 3.9+ desde: https://www.python.org/downloads/
    echo ⚠️  IMPORTANTE: Marca "Add Python to PATH" durante la instalación
    echo.
    pause
    exit /b 1
)

for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
echo ✅ Python %PYTHON_VERSION% encontrado

REM Verificar pip
echo 🔍 Verificando pip...
python -m pip --version >nul 2>&1
if errorlevel 1 (
    echo ❌ pip no encontrado
    echo 💡 Reinstala Python con pip incluido
    pause
    exit /b 1
)
echo ✅ pip disponible

REM Crear directorio del proyecto
echo.
echo 📁 Configurando directorio del proyecto...
if not exist "csv-firebird-automation" (
    mkdir csv-firebird-automation
    echo ✅ Directorio creado: csv-firebird-automation
) else (
    echo ✅ Directorio ya existe: csv-firebird-automation
)

cd csv-firebird-automation

REM Crear entorno virtual
echo.
echo 🔨 Creando entorno virtual Python...
if exist "venv" (
    echo ⚠️  Entorno virtual ya existe, eliminando...
    rmdir /s /q venv
)

python -m venv venv
if errorlevel 1 (
    echo ❌ Error creando entorno virtual
    pause
    exit /b 1
)
echo ✅ Entorno virtual creado

REM Activar entorno virtual
echo 🔋 Activando entorno virtual...
call venv\Scripts\activate.bat

REM Verificar activación
for /f "tokens=*" %%i in ('where python 2^>nul') do set VENV_PYTHON=%%i
echo ✅ Usando Python: %VENV_PYTHON%

REM Actualizar pip
echo.
echo ⬆️ Actualizando pip a la última versión...
python -m pip install --upgrade pip
echo ✅ pip actualizado

REM Crear requirements.txt
echo.
echo 📝 Creando archivo de dependencias...
(
echo # Dependencias principales
echo fastapi==0.104.1
echo uvicorn==0.24.0
echo polars==0.20.2
echo pydantic==2.5.0
echo fdb==2.0.2
echo python-multipart==0.0.6
echo openpyxl==3.1.2
echo requests==2.31.0
echo.
echo # Dependencias opcionales para análisis
echo psutil==5.9.6
echo colorama==0.4.6
) > requirements.txt

echo ✅ requirements.txt creado

REM Instalar dependencias
echo.
echo 📦 Instalando dependencias (esto puede tomar varios minutos)...
echo    • FastAPI (framework web)
echo    • Polars (procesamiento CSV optimizado)
echo    • Firebird connector
echo    • Otras dependencias...
echo.

python -m pip install -r requirements.txt

if errorlevel 1 (
    echo ❌ Error instalando dependencias
    echo 💡 Verifica tu conexión a internet
    pause
    exit /b 1
)

echo.
echo ✅ Todas las dependencias instaladas correctamente!

REM Verificar instalación de Polars
echo.
echo 🧪 Verificando instalación de Polars...
python -c "import polars as pl; print(f'✅ Polars {pl.__version__} funcionando')"
if errorlevel 1 (
    echo ❌ Polars no se instaló correctamente
    pause
    exit /b 1
)

REM Verificar instalación de fdb
echo 🧪 Verificando cliente Firebird...
python -c "import fdb; print('✅ Cliente Firebird (fdb) funcionando')"
if errorlevel 1 (
    echo ❌ Cliente Firebird no disponible
    echo 💡 Puedes continuar, se configurará después
)

echo.
echo 🎉 ¡INSTALACIÓN COMPLETADA EXITOSAMENTE!
echo.
echo 📋 Próximos pasos:
echo    1. Configura tu base de datos en config.py
echo    2. Ejecuta: start_server.bat
echo    3. Coloca tu CSV en este directorio
echo    4. Ejecuta: process_csv.bat
echo.
echo 📁 Directorio del proyecto: %CD%
echo 🔗 Documentación: Ejecuta el servidor y ve a http://localhost:8000/docs
echo.

pause