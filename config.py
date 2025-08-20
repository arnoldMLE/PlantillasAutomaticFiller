# config.py - Configuración del sistema
DATABASE_CONFIG = {
    "host": "localhost",
    "database_path": "C:/Users/Arnold Gonzalez/Desktop/MAUSOLEOS6420250717_NEW.FDB",
    "user": "SYSDBA",
    "password": "masterkey",
    "charset": "UTF8"
}

CSV_CONFIG = {
    "target_column": "CLIENTE",
    "data_start_row": 11,
    "propuesta_column": "B",
    "chunk_size": 10000,
    "use_streaming": True
}

POLARS_CONFIG = {
    "n_threads": None,  # Usar todos los threads disponibles
    "streaming": True,  # Habilitar streaming para archivos grandes
    "string_cache": True,  # Cache de strings para mejor rendimiento
    "fmt_str_lengths": 50,  # Longitud de strings en output
}