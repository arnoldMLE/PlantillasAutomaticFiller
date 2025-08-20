# main_polars.py
"""
Script principal usando Polars para máximo rendimiento
"""

import os
import sys
from client_polars import CSVFirebirdClientPolars
from config import DATABASE_CONFIG, CSV_CONFIG

def main():
    print("[COHETE] CSV-Firebird Automation con Polars")
    print("[RAYO] Optimizado para máximo rendimiento")
    print("=" * 60)
    
    # Crear cliente con Polars
    client = CSVFirebirdClientPolars()
    
    # 1. Verificar estado del sistema
    print("\n1[EMOJI]⃣ Verificando estado del sistema...")
    client.health_check()
    
    # 2. Configurar base de datos
    print("\n2[EMOJI]⃣ Configurando base de datos...")
    success = client.configure_database(
        host=DATABASE_CONFIG["host"],
        database_path=DATABASE_CONFIG["database_path"],
        user=DATABASE_CONFIG["user"],
        password=DATABASE_CONFIG["password"],
        charset=DATABASE_CONFIG["charset"]
    )
    
    if not success:
        print("[X] No se pudo configurar la base de datos.")
        print("[INFO] Verifica la configuración en config.py")
        return
    
    # 3. Probar consulta individual
    print("\n3[EMOJI]⃣ Probando consulta individual...")
    test_legajo = "642799"  # Del ejemplo
    client.get_propuesta_info(test_legajo)
    
    # 4. Determinar archivo a procesar
    csv_file = None
    
    # Si se pasó archivo como argumento
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
        if not os.path.exists(csv_file):
            print(f"[X] Archivo no encontrado: {csv_file}")
            return
    else:
        # Buscar archivos CSV en el directorio actual
        csv_files = [f for f in os.listdir('.') if f.endswith('.csv') and not f.startswith('test_')]
        
        if csv_files:
            print(f"\n4[EMOJI]⃣ Archivos CSV encontrados:")
            for i, csv_file_found in enumerate(csv_files, 1):
                size_mb = os.path.getsize(csv_file_found) / (1024 * 1024)
                print(f"   {i}. {csv_file_found} ({size_mb:.1f} MB)")
            
            # Usar el primer archivo encontrado
            csv_file = csv_files[0]
        else:
            print("\n4[EMOJI]⃣ No se encontraron archivos CSV")
            print("[INFO] Coloca tu archivo CSV en este directorio")
            
            # Ofrecer hacer benchmark
            print("\n[TEST] ¿Quieres ejecutar un benchmark de rendimiento? (s/n): ", end="")
            if input().lower().startswith('s'):
                client.benchmark_polars()
            return
    
    # 5. Procesar archivo
    if csv_file:
        print(f"\n[PROCESO] Procesando: {csv_file}")
        
        result = client.process_csv_file(
            file_path=csv_file,
            target_column=CSV_CONFIG["target_column"],
            data_start_row=CSV_CONFIG["data_start_row"],
            use_streaming=CSV_CONFIG["use_streaming"]
        )
        
        if result and result.get('file_path'):
            print(f"\n[CHECK] Archivo procesado guardado en: {result['file_path']}")
            print(f"[GRAFICO] Resumen final:")
            print(f"   • {result['matched_count']:,} de {result['processed_count']:,} registros completados")
            print(f"   • Eficiencia: {(result['matched_count']/result['processed_count']*100):.1f}%")
            print(f"   • Tiempo total: {result['execution_time']:.2f} segundos")
            print(f"   • Velocidad: {result['processed_count']/result['execution_time']:,.0f} registros/seg")

if __name__ == "__main__":
    main()