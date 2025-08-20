import psutil
import time
import os
import polars as pl
from datetime import datetime
from typing import Dict, List
import threading
import json

class SystemMonitor:
    """Monitor del sistema en tiempo real"""
    
    def __init__(self):
        try:
            self.process = psutil.Process()
        except:
            self.process = None
        self.start_time = time.time()
        self.stats = {
            'files_processed': 0,
            'total_records': 0,
            'successful_matches': 0,
            'errors': 0,
            'avg_processing_time': 0,
            'peak_memory_mb': 0
        }
        self.is_monitoring = False
        
    def start_monitoring(self, interval: float = 1.0):
        """Inicia el monitoreo en background"""
        self.is_monitoring = True
        
        def monitor_loop():
            while self.is_monitoring:
                self._update_stats()
                self._display_stats()
                time.sleep(interval)
        
        monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        monitor_thread.start()
        print("[BUSCAR] Monitor iniciado - presiona Ctrl+C para detener")
    
    def stop_monitoring(self):
        """Detiene el monitoreo"""
        self.is_monitoring = False
        print("\n[STOP] Monitor detenido")
    
    def _update_stats(self):
        """Actualiza estadísticas del sistema"""
        current_stats = {'memory_mb': 0, 'cpu_percent': 0, 'uptime_minutes': 0}
        
        # Memoria actual
        if self.process:
            try:
                memory_mb = self.process.memory_info().rss / 1024 / 1024
                if memory_mb > self.stats['peak_memory_mb']:
                    self.stats['peak_memory_mb'] = memory_mb
                current_stats['memory_mb'] = memory_mb
                
                # CPU actual
                current_stats['cpu_percent'] = self.process.cpu_percent()
            except:
                pass
        
        # Tiempo de actividad
        current_stats['uptime_minutes'] = (time.time() - self.start_time) / 60
        
        # Archivos CSV en directorio
        csv_files = [f for f in os.listdir('.') if f.endswith('_processed.csv')]
        self.stats['files_processed'] = len(csv_files)
        
        return current_stats
    
    def _display_stats(self):
        """Muestra estadísticas en consola"""
        current = self._update_stats()
        
        # Limpiar pantalla (Windows)
        os.system('cls' if os.name == 'nt' else 'clear')
        
        print("=" * 60)
        print("[BUSCAR] MONITOR CSV-FIREBIRD AUTOMATION")
        print("=" * 60)
        print(f"⏰ Tiempo activo: {current['uptime_minutes']:.1f} minutos")
        print(f"[EMOJI] Memoria: {current['memory_mb']:.1f} MB (pico: {self.stats['peak_memory_mb']:.1f} MB)")
        print(f"[RAYO] CPU: {current['cpu_percent']:.1f}%")
        print()
        print("[GRAFICO] ESTADÍSTICAS DE PROCESAMIENTO:")
        print(f"   [CARPETA] Archivos procesados: {self.stats['files_processed']}")
        print(f"   [NOTA] Registros totales: {self.stats['total_records']:,}")
        print(f"   [CHECK] Matches exitosos: {self.stats['successful_matches']:,}")
        print(f"   [X] Errores: {self.stats['errors']}")
        if self.stats['files_processed'] > 0:
            print(f"   [TIEMPO] Tiempo promedio: {self.stats['avg_processing_time']:.1f}s")
        print()
        print("[PROCESO] Actualizando cada segundo... (Ctrl+C para salir)")

    def log_processing_result(self, result: Dict):
        """Registra resultado de procesamiento"""
        self.stats['total_records'] += result.get('processed_count', 0)
        self.stats['successful_matches'] += result.get('matched_count', 0)
        self.stats['errors'] += len(result.get('errors', []))
        
        # Calcular tiempo promedio
        if result.get('execution_time'):
            current_avg = self.stats['avg_processing_time']
            files_count = self.stats['files_processed']
            new_avg = (current_avg * files_count + result['execution_time']) / (files_count + 1)
            self.stats['avg_processing_time'] = new_avg

class CSVAnalyzer:
    """Analizador avanzado de archivos CSV"""
    
    def __init__(self):
        self.analysis_cache = {}
    
    def analyze_csv(self, file_path: str) -> Dict:
        """Analiza un archivo CSV completamente"""
        if file_path in self.analysis_cache:
            return self.analysis_cache[file_path]
        
        print(f"[BUSCAR] Analizando: {os.path.basename(file_path)}")
        
        start_time = time.time()
        
        # Información básica del archivo
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        
        try:
            # Análisis con Polars (más rápido)
            sample_df = pl.read_csv(
                file_path,
                has_header=False,
                n_rows=50,  # Solo primeras 50 filas para análisis
                dtypes=str,
                ignore_errors=True
            )
            
            # Análisis completo para archivos pequeños
            if file_size_mb < 10:
                full_df = pl.read_csv(file_path, has_header=False, dtypes=str, ignore_errors=True)
                total_rows = full_df.height
                total_cols = full_df.width
            else:
                # Estimación para archivos grandes
                lines_sample = sample_df.height
                estimated_total = int((file_size_mb * 1024 * 1024) / (lines_sample * 100))  # Estimación
                total_rows = estimated_total
                total_cols = sample_df.width
            
            analysis = {
                'file_path': file_path,
                'file_size_mb': file_size_mb,
                'total_rows': total_rows,
                'total_cols': total_cols,
                'analysis_time': time.time() - start_time,
                'format_issues': self._check_format_issues(sample_df),
                'propuesta_analysis': self._analyze_propuesta_column(sample_df),
                'data_quality': self._assess_data_quality(sample_df),
                'performance_recommendation': self._get_performance_recommendation(file_size_mb, total_rows),
                'processing_estimate': self._estimate_processing_time(total_rows)
            }
            
            self.analysis_cache[file_path] = analysis
            return analysis
            
        except Exception as e:
            return {
                'file_path': file_path,
                'error': str(e),
                'file_size_mb': file_size_mb,
                'analysis_time': time.time() - start_time
            }
    
    def _check_format_issues(self, df: pl.DataFrame) -> List[str]:
        """Detecta problemas de formato en el CSV"""
        issues = []
        
        # Verificar inconsistencias en número de columnas
        if df.height > 10:
            col_counts = []
            for i in range(min(20, df.height)):
                row_data = df.row(i)
                col_counts.append(len([x for x in row_data if x is not None]))
            
            if len(set(col_counts)) > 2:
                issues.append("[WARNING] Inconsistencia en número de columnas por fila")
        
        # Verificar caracteres especiales problemáticos
        if df.width > 1:
            sample_col_b = df.get_column("column_1")[:10]
            for val in sample_col_b:
                if val and any(char in str(val) for char in ['"', '\n', '\r']):
                    issues.append("[WARNING] Caracteres especiales detectados en columna B")
                    break
        
        return issues
    
    def _analyze_propuesta_column(self, df: pl.DataFrame) -> Dict:
        """Analiza específicamente la columna de propuestas (columna B)"""
        if df.width < 2:
            return {'error': 'No se encontró columna B'}
        
        propuesta_col = df.get_column("column_1")
        
        # Contar propuestas válidas desde fila 11
        valid_propuestas = 0
        empty_propuestas = 0
        sample_propuestas = []
        
        start_idx = min(10, df.height - 1)  # Fila 11 (índice 10)
        
        for i in range(start_idx, min(start_idx + 20, df.height)):
            if i < len(propuesta_col):
                val = propuesta_col[i]
                if val and str(val).strip():
                    valid_propuestas += 1
                    if len(sample_propuestas) < 5:
                        sample_propuestas.append(str(val).strip())
                else:
                    empty_propuestas += 1
        
        return {
            'valid_propuestas_sample': valid_propuestas,
            'empty_propuestas_sample': empty_propuestas,
            'sample_values': sample_propuestas,
            'data_starts_at_row': 11,
            'propuesta_column': 'B (column_1)'
        }
    
    def _assess_data_quality(self, df: pl.DataFrame) -> Dict:
        """Evalúa la calidad general de los datos"""
        total_cells = df.height * df.width
        empty_cells = 0
        
        # Contar celdas vacías en muestra
        for i in range(min(20, df.height)):
            row_data = df.row(i)
            empty_cells += sum(1 for x in row_data if x is None or str(x).strip() == '')
        
        empty_percentage = (empty_cells / (20 * df.width)) * 100 if df.width > 0 else 0
        
        quality_score = max(0, 100 - empty_percentage)
        
        return {
            'empty_cells_percentage': empty_percentage,
            'quality_score': quality_score,
            'quality_level': (
                'Excelente' if quality_score >= 90 else
                'Buena' if quality_score >= 70 else
                'Regular' if quality_score >= 50 else
                'Necesita revisión'
            )
        }
    
    def _get_performance_recommendation(self, file_size_mb: float, total_rows: int) -> Dict:
        """Sugiere configuración óptima para el procesamiento"""
        if file_size_mb > 100:
            return {
                'strategy': 'streaming',
                'chunk_size': 5000,
                'memory_estimate': f"{file_size_mb * 0.3:.1f} MB",
                'recommendation': 'Usar procesamiento streaming para archivo grande'
            }
        elif file_size_mb > 10:
            return {
                'strategy': 'chunked',
                'chunk_size': 10000,
                'memory_estimate': f"{file_size_mb * 0.8:.1f} MB",
                'recommendation': 'Procesamiento por lotes recomendado'
            }
        else:
            return {
                'strategy': 'standard',
                'chunk_size': total_rows,
                'memory_estimate': f"{file_size_mb * 1.5:.1f} MB",
                'recommendation': 'Procesamiento estándar en memoria'
            }
    
    def _estimate_processing_time(self, total_rows: int) -> Dict:
        """Estima tiempo de procesamiento basado en benchmarks"""
        # Basado en benchmarks con Polars
        base_rate = 3000  # registros por segundo (estimación conservadora)
        
        estimated_seconds = total_rows / base_rate
        
        return {
            'estimated_seconds': estimated_seconds,
            'estimated_minutes': estimated_seconds / 60,
            'human_readable': (
                f"{estimated_seconds:.0f} segundos" if estimated_seconds < 60 else
                f"{estimated_seconds/60:.1f} minutos" if estimated_seconds < 3600 else
                f"{estimated_seconds/3600:.1f} horas"
            )
        }
    
    def generate_report(self, file_path: str) -> str:
        """Genera un reporte completo del análisis"""
        analysis = self.analyze_csv(file_path)
        
        if 'error' in analysis:
            return f"[X] Error analizando {file_path}: {analysis['error']}"
        
        report = []
        report.append("=" * 60)
        report.append(f"[GRAFICO] REPORTE DE ANÁLISIS CSV")
        report.append("=" * 60)
        report.append(f"[CARPETA] Archivo: {os.path.basename(file_path)}")
        report.append(f"[EMOJI] Tamaño: {analysis['file_size_mb']:.1f} MB")
        report.append(f"[GRAFICO] Dimensiones: {analysis['total_rows']:,} filas × {analysis['total_cols']} columnas")
        report.append(f"[TIEMPO] Tiempo de análisis: {analysis['analysis_time']:.2f}s")
        
        report.append("\n[TARGET] ANÁLISIS DE PROPUESTAS (Columna B):")
        prop_analysis = analysis['propuesta_analysis']
        if 'error' not in prop_analysis:
            report.append(f"   [CHECK] Propuestas válidas encontradas: {prop_analysis['valid_propuestas_sample']}")
            report.append(f"   [X] Celdas vacías: {prop_analysis['empty_propuestas_sample']}")
            report.append(f"   [NOTA] Ejemplos: {', '.join(prop_analysis['sample_values'][:3])}")
        
        report.append("\n[CHART] CALIDAD DE DATOS:")
        quality = analysis['data_quality']
        report.append(f"   [TARGET] Puntuación: {quality['quality_score']:.1f}/100 ({quality['quality_level']})")
        report.append(f"   [GRAFICO] Celdas vacías: {quality['empty_cells_percentage']:.1f}%")
        
        report.append("\n[COHETE] RECOMENDACIONES DE RENDIMIENTO:")
        perf = analysis['performance_recommendation']
        report.append(f"   [CONFIG] Estrategia: {perf['strategy'].title()}")
        report.append(f"   [GUARDAR] Memoria estimada: {perf['memory_estimate']}")
        report.append(f"   [INFO] {perf['recommendation']}")
        
        report.append("\n[TIEMPO] ESTIMACIÓN DE PROCESAMIENTO:")
        estimate = analysis['processing_estimate']
        report.append(f"   [EMOJI] Tiempo estimado: {estimate['human_readable']}")
        
        if analysis['format_issues']:
            report.append("\n[WARNING] PROBLEMAS DETECTADOS:")
            for issue in analysis['format_issues']:
                report.append(f"   {issue}")
        
        report.append("\n" + "=" * 60)
        
        return "\n".join(report)

def main_monitor():
    """Función principal para ejecutar el monitor standalone"""
    monitor = SystemMonitor()
    
    try:
        monitor.start_monitoring(interval=2.0)
        
        # Mantener ejecutándose hasta Ctrl+C
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        monitor.stop_monitoring()
        print("\n[SALUDO] Monitor detenido por el usuario")

if __name__ == "__main__":
    # Si se ejecuta directamente, iniciar monitor
    main_monitor()