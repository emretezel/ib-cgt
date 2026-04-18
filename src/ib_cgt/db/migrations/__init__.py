"""Migration SQL files, discovered at runtime by `ib_cgt.db.migrator`.

Keeping this directory a proper Python package (rather than a namespace
package) ensures `importlib.resources.files("ib_cgt.db.migrations")` works
both from a source checkout and from an installed wheel.

Author: Emre Tezel
"""
