from dataclasses import dataclass
import sqlite3

from audit.lookup import lookup_result
from audit.provenance import Provenance, trace_origins
from audit.reconcile import ReconciledBook, reconcile_result
from audit.snapshots import PublishedBook


@dataclass(frozen=True)
class Handover:
    book: PublishedBook
    provenance: Provenance
    accounts: ReconciledBook


def collect_handover(connection: sqlite3.Connection, result_id: str) -> Handover:
    book = lookup_result(connection, result_id)
    return Handover(book, trace_origins(book), reconcile_result(book))
