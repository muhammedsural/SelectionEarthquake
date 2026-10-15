"""Dalga formu okuma ve yazma yardimcilari (ObsPy gerektirmez)."""

from .mseed import MSeedError, Trace, read_mseed
from .ascii import write_ascii

__all__ = ["MSeedError", "Trace", "read_mseed", "write_ascii"]
