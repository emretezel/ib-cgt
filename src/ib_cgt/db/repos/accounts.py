"""Repository for IB `Account` rows.

Accounts are the foundation the rest of the schema references: trades and
statements both FK to `accounts.account_id`. Kept deliberately tiny —
just upsert / get / all — because ingestion is the only writer and
downstream components only ever read.

Author: Emre Tezel
"""

from __future__ import annotations

import sqlite3

from ib_cgt.domain import Account


class AccountRepo:
    """Thin wrapper over the `accounts` table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Bind this repo to an already-open, already-migrated connection."""
        self._conn = conn

    def upsert(self, account: Account) -> None:
        """Insert or update an account by `account_id`.

        Label changes overwrite the previous value — the IB account code
        is the immutable identity, the label is just human-friendly.
        """
        self._conn.execute(
            "INSERT INTO accounts (account_id, label) VALUES (?, ?) "
            "ON CONFLICT(account_id) DO UPDATE SET label = excluded.label",
            (account.account_id, account.label),
        )

    def get(self, account_id: str) -> Account | None:
        """Return the account with `account_id`, or `None` if absent."""
        row = self._conn.execute(
            "SELECT account_id, label FROM accounts WHERE account_id = ?",
            (account_id,),
        ).fetchone()
        if row is None:
            return None
        return Account(account_id=row["account_id"], label=row["label"])

    def all(self) -> tuple[Account, ...]:
        """Return every account, ordered by `account_id` for stable output."""
        rows = self._conn.execute(
            "SELECT account_id, label FROM accounts ORDER BY account_id"
        ).fetchall()
        return tuple(Account(account_id=r["account_id"], label=r["label"]) for r in rows)
