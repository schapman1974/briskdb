import os
import unittest

import psycopg
from sqlalchemy import String, create_engine, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


DSN_ENV = "BRISKDB_POSTGRES_MATRIX_DSN"
URL_ENV = "BRISKDB_POSTGRES_MATRIX_URL"


class Base(DeclarativeBase):
    pass


class Record(Base):
    __tablename__ = "records"
    __table_args__ = {"implicit_returning": False}

    tenant_id: Mapped[str] = mapped_column(String, primary_key=True)
    payload: Mapped[str] = mapped_column(String, nullable=False)


class PostgreSQLClientMatrix(unittest.TestCase):
    def test_psycopg_autocommit_enforces_global_unique_index(self) -> None:
        dsn = os.environ[DSN_ENV]
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute(
                "INSERT INTO indexed_records (tenant_id, payload) VALUES (%s, %s)",
                ("psycopg-index-a", "psycopg-global-key"),
            )
            with self.assertRaises(psycopg.errors.UniqueViolation):
                connection.execute(
                    "INSERT INTO indexed_records (tenant_id, payload) VALUES (%s, %s)",
                    ("psycopg-index-b", "psycopg-global-key"),
                )
            self.assertEqual(connection.execute("SELECT 1").fetchone(), ("1",))

    def test_psycopg_transaction_error_recovery_and_reconnect(self) -> None:
        dsn = os.environ[DSN_ENV]
        with psycopg.connect(dsn) as connection:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO records (tenant_id, payload) VALUES (%s, %s)",
                        ("psycopg-client", "created"),
                    )
                    cursor.execute(
                        "SELECT payload FROM records WHERE tenant_id = %s",
                        ("psycopg-client",),
                    )
                    self.assertEqual(cursor.fetchone(), ("created",))
                    cursor.execute(
                        "UPDATE records SET payload = %s WHERE tenant_id = %s",
                        ("updated", "psycopg-client"),
                    )
                    self.assertEqual(cursor.rowcount, 1)
                    cursor.execute(
                        "DELETE FROM records WHERE tenant_id = %s",
                        ("psycopg-client",),
                    )
                    self.assertEqual(cursor.rowcount, 1)

            with self.assertRaises(psycopg.errors.FeatureNotSupported):
                connection.execute("SHOW work_mem")
            connection.rollback()
            self.assertEqual(connection.execute("SELECT 1").fetchone(), ("1",))

        with psycopg.connect(dsn) as connection:
            self.assertEqual(connection.execute("SELECT 1").fetchone(), ("1",))

    def test_sqlalchemy_orm_transaction_error_recovery_and_reconnect(self) -> None:
        url = os.environ[URL_ENV]
        engine = create_engine(url, use_native_hstore=False)
        with Session(engine, expire_on_commit=False) as session:
            record = Record(tenant_id="sqlalchemy-client", payload="created")
            session.add(record)
            session.flush()
            self.assertEqual(
                session.scalar(
                    select(Record.payload).where(
                        Record.tenant_id == "sqlalchemy-client"
                    )
                ),
                "created",
            )
            record.payload = "updated"
            session.flush()
            self.assertEqual(
                session.scalar(
                    select(Record.payload).where(
                        Record.tenant_id == "sqlalchemy-client"
                    )
                ),
                "updated",
            )
            session.delete(record)
            session.commit()

        with engine.connect() as connection:
            with self.assertRaises(DBAPIError):
                connection.execute(text("SHOW work_mem"))
            connection.rollback()
            self.assertEqual(connection.execute(text("SELECT 1")).scalar_one(), "1")
        engine.dispose()

        engine = create_engine(url, use_native_hstore=False)
        with engine.connect() as connection:
            self.assertEqual(connection.execute(text("SELECT 1")).scalar_one(), "1")
        engine.dispose()


if __name__ == "__main__":
    unittest.main()
