"""
Sistema FastAPI para automatizar el llenado de CSV desde base de datos Firebird
Maneja el match entre PROPUESTA JKM y LEGAJO para llenar campos automáticamente
Optimizado con Polars para máximo rendimiento
"""

from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import fdb
import polars as pl
import os
import logging
from datetime import datetime
import asyncio
from contextlib import asynccontextmanager
import tempfile
import uuid

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Modelos Pydantic
class DatabaseConfig(BaseModel):
    host: str = Field(..., description="Host de la base de datos Firebird")
    database_path: str = Field(..., description="Ruta completa al archivo .fdb")
    user: str = Field(default="SYSDBA", description="Usuario de la base de datos")
    password: str = Field(..., description="Contraseña de la base de datos")
    charset: str = Field(default="UTF8", description="Charset de la base de datos")

class PropuestaData(BaseModel):
    cod_propuesta: str = Field(..., description="Código de propuesta (LEGAJO)")
    nombre_cliente: str = Field(..., description="Nombre del cliente")
    cod_cliente: Optional[str] = Field(None, description="Código del cliente")
    sucursal: Optional[str] = Field(None, description="Sucursal")
    monto: Optional[float] = Field(None, description="Monto del crédito")
    fecha_contrato: Optional[str] = Field(None, description="Fecha del contrato")
    estado: Optional[str] = Field(None, description="Estado del contrato")
    tipo_contrato: Optional[str] = Field(None, description="Tipo de contrato")
    telefono: Optional[str] = Field(None, description="Teléfono")
    telefono_movil: Optional[str] = Field(None, description="Teléfono móvil")
    direccion: Optional[str] = Field(None, description="Dirección")
    asesor: Optional[str] = Field(None, description="Asesor de ventas")

class ProcessResult(BaseModel):
    success: bool
    processed_count: int
    matched_count: int
    errors: List[str] = []
    file_path: Optional[str] = None
    execution_time: float

class CSVProcessRequest(BaseModel):
    target_column: str = Field(default="CLIENTE", description="Columna objetivo a llenar")
    data_start_row: int = Field(default=11, description="Fila donde empiezan los datos")
    propuesta_column: str = Field(default="B", description="Columna de PROPUESTA JKM")

# Clase para manejo de la base de datos
class FirebirdManager:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection_string = f"{config.host}:{config.database_path}"
        
    def get_connection(self):
        """Obtiene una conexión a la base de datos Firebird"""
        try:
            con = fdb.connect(
                host=self.config.host,
                database=self.config.database_path,
                user=self.config.user,
                password=self.config.password,
                charset=self.config.charset
            )
            return con
        except Exception as e:
            logger.error(f"Error conectando a Firebird: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error de conexión a BD: {str(e)}")

    def get_propuesta_data(self, legajo: str) -> Optional[PropuestaData]:
        """Obtiene los datos de una propuesta específica por LEGAJO"""
        query = """
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
        """
        
        try:
            with self.get_connection() as con:
                cur = con.cursor()
                cur.execute(query, (legajo,))
                row = cur.fetchone()
                
                if row:
                    return PropuestaData(
                        cod_propuesta=str(row[0]) if row[0] else "",
                        nombre_cliente=str(row[1]) if row[1] else "",
                        cod_cliente=str(row[2]) if row[2] else "",
                        sucursal=str(row[3]) if row[3] else "",
                        monto=float(row[4]) if row[4] else 0.0,
                        fecha_contrato=str(row[5]) if row[5] else "",
                        estado=str(row[6]) if row[6] else "",
                        tipo_contrato=str(row[7]) if row[7] else "",
                        telefono=str(row[8]) if row[8] else "",
                        telefono_movil=str(row[9]) if row[9] else "",
                        direccion=str(row[10]) if row[10] else "",
                        asesor=str(row[11]) if row[11] else ""
                    )
                return None
                
        except Exception as e:
            logger.error(f"Error ejecutando consulta para LEGAJO {legajo}: {str(e)}")
            return None

    def get_multiple_propuestas(self, legajos: List[str]) -> Dict[str, PropuestaData]:
        """Obtiene datos de múltiples propuestas de manera eficiente"""
        if not legajos:
            return {}
            
        # Crear placeholders para la consulta IN
        placeholders = ','.join(['?' for _ in legajos])
        
        query = f"""
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
        WHERE p.LEGAJO IN ({placeholders})
        ORDER BY p.LEGAJO
        """
        
        result = {}
        try:
            with self.get_connection() as con:
                cur = con.cursor()
                cur.execute(query, legajos)
                rows = cur.fetchall()
                
                for row in rows:
                    legajo = str(row[0])
                    result[legajo] = PropuestaData(
                        cod_propuesta=legajo,
                        nombre_cliente=str(row[1]) if row[1] else "",
                        cod_cliente=str(row[2]) if row[2] else "",
                        sucursal=str(row[3]) if row[3] else "",
                        monto=float(row[4]) if row[4] else 0.0,
                        fecha_contrato=str(row[5]) if row[5] else "",
                        estado=str(row[6]) if row[6] else "",
                        tipo_contrato=str(row[7]) if row[7] else "",
                        telefono=str(row[8]) if row[8] else "",
                        telefono_movil=str(row[9]) if row[9] else "",
                        direccion=str(row[10]) if row[10] else "",
                        asesor=str(row[11]) if row[11] else ""
                    )
                    
        except Exception as e:
            logger.error(f"Error ejecutando consulta múltiple: {str(e)}")
            
        return result

# Clase para procesamiento de CSV con Polars optimizado
class CSVProcessor:
    def __init__(self, db_manager: FirebirdManager):
        self.db_manager = db_manager
        
    def process_csv_file(self, file_path: str, request: CSVProcessRequest) -> ProcessResult:
        """Procesa el archivo CSV y llena los campos faltantes usando Polars optimizado"""
        start_time = datetime.now()
        errors = []
        processed_count = 0
        matched_count = 0
        
        try:
            # Verificar tamaño del archivo para decidir estrategia
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            use_streaming = file_size_mb > 100  # Usar streaming para archivos > 100MB
            
            logger.info(f"Procesando CSV ({file_size_mb:.1f} MB) con Polars, streaming: {use_streaming}")
            
            if use_streaming:
                return self._process_large_csv_streaming(file_path, request)
            else:
                return self._process_standard_csv(file_path, request)
                
        except Exception as e:
            logger.error(f"Error procesando CSV: {str(e)}")
            execution_time = (datetime.now() - start_time).total_seconds()
            return ProcessResult(
                success=False,
                processed_count=processed_count,
                matched_count=matched_count,
                errors=[str(e)],
                execution_time=execution_time
            )
    
    def _process_standard_csv(self, file_path: str, request: CSVProcessRequest) -> ProcessResult:
        """Procesa archivos CSV estándar (< 100MB) con Polars"""
        start_time = datetime.now()
        errors = []
        processed_count = 0
        matched_count = 0
        
        try:
            # Leer el CSV con Polars (más rápido que pandas)
            df = pl.read_csv(
                file_path,
                has_header=False,
                dtypes=str,  # Todo como string para evitar problemas de tipo
                ignore_errors=True,
                truncate_ragged_lines=True  # Manejar líneas inconsistentes
            )
            logger.info(f"CSV cargado con {df.height} filas y {df.width} columnas usando Polars")
            
            # Extraer propuestas de manera eficiente
            propuestas, propuesta_indices = self._extract_propuestas_optimized(df, request)
            processed_count = len(propuestas)
            logger.info(f"Encontradas {processed_count} propuestas para procesar")
            
            if not propuestas:
                return ProcessResult(
                    success=True,
                    processed_count=0,
                    matched_count=0,
                    errors=["No se encontraron propuestas válidas"],
                    execution_time=(datetime.now() - start_time).total_seconds()
                )
            
            # Obtener datos de la BD de manera eficiente (en lotes)
            propuestas_data = self._get_data_in_batches(propuestas)
            
            # Aplicar actualizaciones de manera optimizada
            matched_count = self._apply_updates_optimized(df, propuestas, propuesta_indices, 
                                                        propuestas_data, request, errors)
            
            # Guardar el archivo procesado
            output_path = file_path.replace('.csv', '_processed.csv')
            df.write_csv(output_path, has_header=False)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            return ProcessResult(
                success=True,
                processed_count=processed_count,
                matched_count=matched_count,
                errors=errors,
                file_path=output_path,
                execution_time=execution_time
            )
            
        except Exception as e:
            logger.error(f"Error en _process_standard_csv: {str(e)}")
            execution_time = (datetime.now() - start_time).total_seconds()
            return ProcessResult(
                success=False,
                processed_count=processed_count,
                matched_count=matched_count,
                errors=[str(e)],
                execution_time=execution_time
            )
    
    def _extract_propuestas_optimized(self, df: pl.DataFrame, request: CSVProcessRequest):
        """Extrae propuestas del DataFrame de manera optimizada con Polars"""
        propuestas = []
        propuesta_indices = []
        
        if df.width > 1:  # Verificar que existe columna B
            # Obtener columna B (índice 1) usando Polars
            propuesta_col = df.get_column(f"column_1")
            
            # Filtrar desde la fila de datos especificada
            start_idx = request.data_start_row - 1
            
            for idx in range(start_idx, min(len(propuesta_col), df.height)):
                propuesta = propuesta_col[idx]
                if propuesta is not None:
                    propuesta_clean = str(propuesta).strip()
                    if propuesta_clean and propuesta_clean != '':
                        propuestas.append(propuesta_clean)
                        propuesta_indices.append(idx)
        
        return propuestas, propuesta_indices
    
    def _get_data_in_batches(self, propuestas: List[str], batch_size: int = 1000):
        """Obtiene datos de BD en lotes para optimizar rendimiento"""
        all_data = {}
        
        # Procesar en lotes para evitar consultas SQL muy grandes
        for i in range(0, len(propuestas), batch_size):
            batch = propuestas[i:i + batch_size]
            logger.info(f"Procesando lote {i//batch_size + 1}: {len(batch)} propuestas")
            
            batch_data = self.db_manager.get_multiple_propuestas(batch)
            all_data.update(batch_data)
        
        return all_data
    
    def _apply_updates_optimized(self, df: pl.DataFrame, propuestas: List[str], 
                                propuesta_indices: List[int], propuestas_data: Dict,
                                request: CSVProcessRequest, errors: List[str]) -> int:
        """Aplica actualizaciones al DataFrame de manera optimizada"""
        matched_count = 0
        
        # Encontrar o crear columna objetivo
        target_col_index = self._find_or_create_column_index(df, request.target_column)
        target_col_name = f"column_{target_col_index}"
        
        # Preparar lista de valores para la columna objetivo
        if target_col_name in df.columns:
            current_target_values = df.get_column(target_col_name).to_list()
        else:
            current_target_values = [""] * df.height
            # Expandir si es necesario
            while len(current_target_values) < df.height:
                current_target_values.append("")
        
        # Aplicar actualizaciones
        for i, propuesta in enumerate(propuestas):
            row_idx = propuesta_indices[i]
            
            if propuesta in propuestas_data:
                if row_idx < len(current_target_values):
                    current_target_values[row_idx] = propuestas_data[propuesta].nombre_cliente
                    matched_count += 1
            else:
                errors.append(f"No se encontró datos para propuesta: {propuesta}")
        
        # Actualizar DataFrame con nueva columna optimizada
        # Si la columna no existía, agregarla
        if target_col_name not in df.columns:
            df = df.with_columns([
                pl.Series(name=target_col_name, values=current_target_values)
            ])
        else:
            # Reemplazar columna existente
            df = df.with_columns([
                pl.Series(name=target_col_name, values=current_target_values)
            ])
        
        return matched_count
    
    def _find_or_create_column_index(self, df: pl.DataFrame, column_name: str) -> int:
        """Encuentra o crea la columna objetivo y devuelve el índice"""
        # Buscar si ya existe una columna con el nombre (en las primeras filas)
        for col_idx in range(df.width):
            col_name = f"column_{col_idx}"
            if col_name in df.columns:
                col_data = df.get_column(col_name)
                for row_idx in range(min(10, df.height)):  # Buscar en las primeras 10 filas
                    if row_idx < len(col_data) and col_data[row_idx] == column_name:
                        return col_idx
        
        # Si no existe, retornar el índice para nueva columna
        return df.width

# Inicialización de la aplicación
app = FastAPI(
    title="CSV Firebird Automation",
    description="Automatización de llenado de CSV desde base de datos Firebird",
    version="1.0.0"
)

# Variable global para el manager de BD
db_manager: Optional[FirebirdManager] = None

@app.post("/configure-database/")
async def configure_database(config: DatabaseConfig):
    """Configura la conexión a la base de datos Firebird"""
    global db_manager
    try:
        db_manager = FirebirdManager(config)
        # Probar la conexión
        with db_manager.get_connection() as con:
            cur = con.cursor()
            cur.execute("SELECT 1 FROM RDB$DATABASE")
            cur.fetchone()
        
        return {"message": "Base de datos configurada correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error configurando BD: {str(e)}")

@app.post("/process-csv/", response_model=ProcessResult)
async def process_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    target_column: str = "CLIENTE",
    data_start_row: int = 11,
    propuesta_column: str = "B"
):
    """Procesa el archivo CSV y llena los campos faltantes"""
    if db_manager is None:
        raise HTTPException(status_code=400, detail="Base de datos no configurada")
    
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Solo se aceptan archivos CSV")
    
    # Guardar archivo temporal
    temp_dir = tempfile.gettempdir()
    temp_file = os.path.join(temp_dir, f"{uuid.uuid4()}_{file.filename}")
    
    try:
        with open(temp_file, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # Procesar el archivo
        processor = CSVProcessor(db_manager)
        request = CSVProcessRequest(
            target_column=target_column,
            data_start_row=data_start_row,
            propuesta_column=propuesta_column
        )
        
        result = processor.process_csv_file(temp_file, request)
        
        # Limpiar archivo temporal en background
        background_tasks.add_task(os.remove, temp_file)
        
        return result
        
    except Exception as e:
        # Limpiar archivo temporal en caso de error
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/propuesta/{legajo}", response_model=PropuestaData)
async def get_propuesta(legajo: str):
    """Obtiene los datos de una propuesta específica"""
    if db_manager is None:
        raise HTTPException(status_code=400, detail="Base de datos no configurada")
    
    data = db_manager.get_propuesta_data(legajo)
    if data is None:
        raise HTTPException(status_code=404, detail=f"Propuesta {legajo} no encontrada")
    
    return data

@app.get("/health")
async def health_check():
    """Endpoint de salud"""
    status = {
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "database_configured": db_manager is not None
    }
    
    if db_manager:
        try:
            with db_manager.get_connection() as con:
                cur = con.cursor()
                cur.execute("SELECT 1 FROM RDB$DATABASE")
                cur.fetchone()
            status["database_connection"] = "ok"
        except:
            status["database_connection"] = "error"
    
    return status

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)