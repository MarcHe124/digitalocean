import pytest

from app.postgres_repository import PostgresJobRepository
from app.repository import JobRepository
from app.repository_factory import create_repository
from app.settings import Settings


def test_repository_factory_uses_sqlite_without_database_url(tmp_path):
    settings = Settings(_env_file=None, database_path=str(tmp_path / "pulsequeue.db"), database_url=None)

    repository = create_repository(settings)

    assert isinstance(repository, JobRepository)


def test_repository_factory_rejects_unsupported_database_url():
    settings = Settings(_env_file=None, database_url="mysql://example")

    with pytest.raises(ValueError):
        create_repository(settings)


def test_repository_factory_selects_postgres_for_postgres_url(monkeypatch):
    monkeypatch.setattr(PostgresJobRepository, "__init__", lambda self, database_url: setattr(self, "database_url", database_url))
    settings = Settings(_env_file=None, database_url="postgresql://user:password@example.com/db")

    repository = create_repository(settings)

    assert isinstance(repository, PostgresJobRepository)
    assert repository.database_url == "postgresql://user:password@example.com/db"
