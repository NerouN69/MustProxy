import logging
from dataclasses import dataclass
from typing import Callable, List, Set
from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection


@dataclass(frozen=True)
class Migration:
    id: str
    description: str
    upgrade: Callable[[Connection], None]


def _ensure_migrations_table(connection: Connection) -> None:
    connection.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                id VARCHAR(255) PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
    )


def _migration_0001_add_channel_subscription_fields(connection: Connection) -> None:
    inspector = inspect(connection)
    columns: Set[str] = {col["name"] for col in inspector.get_columns("users")}
    
    statements: List[str] = []
    
    if "channel_subscription_verified" not in columns:
        statements.append(
            "ALTER TABLE users ADD COLUMN channel_subscription_verified BOOLEAN"
        )
    
    if "channel_subscription_checked_at" not in columns:
        statements.append(
            "ALTER TABLE users ADD COLUMN channel_subscription_checked_at TIMESTAMPTZ"
        )
    
    if "channel_subscription_verified_for" not in columns:
        statements.append(
            "ALTER TABLE users ADD COLUMN channel_subscription_verified_for BIGINT"
        )
    
    for stmt in statements:
        connection.execute(text(stmt))


def _migration_0002_yandex_tracking(connection: Connection) -> None:
    """Специфичные миграции для Yandex tracking"""
    try:
        # Проверяем, существует ли таблица yandex_tracking
        inspector = inspect(connection)
        if 'yandex_tracking' not in inspector.get_table_names():
            logging.info("Table yandex_tracking doesn't exist yet, skipping migrations")
            return
        
        # Проверяем наличие колонок и добавляем недостающие
        existing_columns = {col['name'] for col in inspector.get_columns('yandex_tracking')}
        
        # Добавляем last_visit_time если её нет
        if 'last_visit_time' not in existing_columns:
            connection.execute(text(
                "ALTER TABLE yandex_tracking ADD COLUMN last_visit_time TIMESTAMP WITH TIME ZONE DEFAULT NOW()"
            ))
            logging.info("Added last_visit_time column to yandex_tracking")
        
        # Добавляем visit_count если её нет
        if 'visit_count' not in existing_columns:
            connection.execute(text(
                "ALTER TABLE yandex_tracking ADD COLUMN visit_count INTEGER DEFAULT 1"
            ))
            logging.info("Added visit_count column to yandex_tracking")
            
        # Обновляем существующие записи, где last_visit_time = NULL
        connection.execute(text("""
            UPDATE yandex_tracking 
            SET last_visit_time = COALESCE(first_visit_time, NOW())
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
            
        # Проверяем и создаем таблицу yandex_conversions если её нет
        if 'yandex_conversions' not in inspector.get_table_names():
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS yandex_conversions (
                    conversion_id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES users(user_id),
                    payment_id VARCHAR NOT NULL,
                    amount FLOAT NOT NULL,
                    sent_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    CONSTRAINT uq_user_payment_conversion UNIQUE (user_id, payment_id)
                )
            """))
            logging.info("Created yandex_conversions table")
            
            # Создаем индексы для yandex_conversions
            connection.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_yandex_conversions_user_id ON yandex_conversions(user_id)"
            ))
            connection.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_yandex_conversions_payment_id ON yandex_conversions(payment_id)"
            ))
            logging.info("Created indexes for yandex_conversions")
        
        logging.info("Yandex tracking migrations completed successfully")
        
    except Exception as e:
        logging.warning(f"Failed to run Yandex tracking migrations: {e}")
        # Не прерываем работу бота из-за неудачных миграций
        raise  # Изменено: теперь пробрасываем исключение для корректной работы с транзакциями


# Список всех миграций в порядке применения
MIGRATIONS: List[Migration] = [
    Migration(
        id="0001_add_channel_subscription_fields",
        description="Add columns to track required channel subscription verification",
        upgrade=_migration_0001_add_channel_subscription_fields,
    ),
    Migration(
        id="0002_yandex_tracking",
        description="Add Yandex tracking tables and columns",
        upgrade=_migration_0002_yandex_tracking,
    ),
]


def run_database_migrations(connection: Connection) -> None:
    """
    Apply pending migrations sequentially. Already applied revisions are skipped.
    """
    _ensure_migrations_table(connection)
    
    applied_revisions: Set[str] = {
        row[0]
        for row in connection.execute(
            text("SELECT id FROM schema_migrations")
        )
    }
    
    for migration in MIGRATIONS:
        if migration.id in applied_revisions:
            continue
        
        logging.info(
            "Migrator: applying %s – %s", migration.id, migration.description
        )
        
        try:
            with connection.begin_nested():
                migration.upgrade(connection)
                connection.execute(
                    text(
                        "INSERT INTO schema_migrations (id) VALUES (:revision)"
                    ),
                    {"revision": migration.id},
                )
        except Exception as exc:
            logging.error(
                "Migrator: failed to apply %s (%s)",
                migration.id,
                migration.description,
                exc_info=True,
            )
            raise exc
        else:
            logging.info("Migrator: migration %s applied successfully", migration.id)