"""Central configuration for table names and storage locations.

On Databricks the pipeline writes to Unity Catalog (``catalog.schema.table``).
Locally (tests) the same names are used as plain temp views.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class LakehouseConfig:
    catalog: str = "workspace"
    schema: str = "retail"
    volume: str = "raw"

    # ---- storage -------------------------------------------------------
    @property
    def raw_path(self) -> str:
        """Unity Catalog volume where raw landing files are written."""
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def checkpoint_path(self) -> str:
        return f"{self.raw_path}/_checkpoints"

    # ---- tables --------------------------------------------------------
    def table(self, layer: str, name: str) -> str:
        """Fully qualified table name, e.g. ``workspace.retail.silver_orders``."""
        return f"{self.catalog}.{self.schema}.{layer}_{name}"


ENTITIES = ("customers", "products", "orders", "order_items")

RAW_FORMATS = {
    "customers": "csv",
    "products": "csv",
    "orders": "json",
    "order_items": "csv",
}
