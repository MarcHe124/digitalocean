from app.postgres_repository import PostgresJobRepository
from app.repository import JobRepository
from app.settings import Settings


def create_repository(settings: Settings):
    if settings.database_url:
        if settings.database_url.startswith(("postgres://", "postgresql://")):
            return PostgresJobRepository(settings.database_url)
        raise ValueError("DATABASE_URL must be a postgres:// or postgresql:// URL")
    return JobRepository(settings.database_path)
