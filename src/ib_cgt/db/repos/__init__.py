"""Repository classes, one per aggregate.

Each repo takes a `sqlite3.Connection` in its constructor and exposes a
small, intention-revealing surface. No ORM, no query-builder — just
explicit SQL.

Author: Emre Tezel
"""
