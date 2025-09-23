import logging
from typing import Set

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection

from .models import Base


def _add_missing_columns(connection: Connection) -> None:
    inspector = inspect(connection)
    metadata = Base.metadata

    existing_tables: Set[str] = set(inspector.get_table_names())

    for table in metadata.tables.values():
        table_name = table.name
        if table_name not in existing_tables:
            # Tables are created elsewhere via create_all; skip here.
            continue

        existing_columns = {col_info["name"] for col_info in inspector.get_columns(table_name)}

        for desired_column in table.columns:
            if desired_column.name in existing_columns:
                continue

            # Build ADD COLUMN DDL
            preparer = connection.dialect.identifier_preparer
            table_quoted = preparer.format_table(table)
            column_name_quoted = preparer.quote(desired_column.name)
            column_type_sql = desired_column.type.compile(dialect=connection.dialect)

            default_clause = ""
            server_default = getattr(desired_column, "server_default", None)
            if server_default is not None and getattr(server_default, "arg", None) is not None:
                try:
                    compiled_default = str(
                        server_default.arg.compile(dialect=connection.dialect)
                    )
                    default_clause = f" DEFAULT {compiled_default}"
                except Exception:  # best-effort
                    pass

            # For safety, add new columns as NULLable to avoid failures on existing rows
            # If strict NOT NULL is needed, it can be enforced manually later.
            ddl = f"ALTER TABLE {table_quoted} ADD COLUMN {column_name_quoted} {column_type_sql}{default_clause}"

            logging.info(
                f"Migrator: adding missing column {desired_column.name} to table {table_name}"
            )
            connection.execute(text(ddl))


def _run_yandex_tracking_migrations(connection: Connection) -> None:
    """Специфичные миграции для Yandex tracking"""
    try:
        # Проверяем, существует ли таблица yandex_tracking
        inspector = inspect(connection)
        if 'yandex_tracking' not in inspector.get_table_names():
            logging.info("Table yandex_tracking doesn't exist yet, skipping migrations")
            return
            
        # Обновляем существующие записи, где last_visit_time = NULL
        connection.execute(text("""
            UPDATE yandex_tracking 
            SET last_visit_time = first_visit_time 
            WHERE last_visit_time IS NULL
        """))
        
        connection.execute(text("""
            UPDATE yandex_tracking 
            SET visit_count = 1 
            WHERE visit_count IS NULL
        """))
        
        # Создаем индексы, если их нет
        existing_indexes = {idx['name'] for idx in inspector.get_indexes('yandex_tracking')}
        
        if 'idx_yandex_tracking_visit_count' not in existing_indexes:
            connection.execute(text(
                "CREATE INDEX idx_yandex_tracking_visit_count ON yandex_tracking(visit_count)"
            ))
            logging.info("Created index idx_yandex_tracking_visit_count")
            
        if 'idx_yandex_tracking_last_visit' not in existing_indexes:
            connection.execute(text(
                "CREATE INDEX idx_yandex_tracking_last_visit ON yandex_tracking(last_visit_time)"
            ))
            logging.info("Created index idx_yandex_tracking_last_visit")
            
        # Проверяем таблицу yandex_conversions
        if 'yandex_conversions' in inspector.get_table_names():
            existing_conv_indexes = {idx['name'] for idx in inspector.get_indexes('yandex_conversions')}
            
            if 'idx_yandex_conversions_user_id' not in existing_conv_indexes:
                connection.execute(text(
                    "CREATE INDEX idx_yandex_conversions_user_id ON yandex_conversions(user_id)"
                ))
                logging.info("Created index idx_yandex_conversions_user_id")
                
            if 'idx_yandex_conversions_payment_id' not in existing_conv_indexes:
                connection.execute(text(
                    "CREATE INDEX idx_yandex_conversions_payment_id ON yandex_conversions(payment_id)"
                ))
                logging.info("Created index idx_yandex_conversions_payment_id")
                
        logging.info("Yandex tracking migrations completed successfully")
        
    except Exception as e:
        logging.warning(f"Failed to run Yandex tracking migrations: {e}")
        # Не прерываем работу бота из-за неудачных миграций


def run_simple_migrations(connection: Connection) -> None:
    """
    Run lightweight, idempotent migrations:
    - Ensure missing columns are added to existing tables to match models in db/models.py
    - Run specific migrations for Yandex tracking
    Note: Table creation is handled separately via Base.metadata.create_all.
    """
    try:
        _add_missing_columns(connection)
        _run_yandex_tracking_migrations(connection)
        logging.info("Migrator: schema synchronized (columns added as needed).")
    except Exception as e:
        logging.error(f"Migrator: failed to run simple migrations: {e}", exc_info=True)
        raise