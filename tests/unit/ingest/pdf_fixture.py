"""A minimal PDF writer for tests of the PDF statement adapter.

The real statements are private, so the adapter's end-to-end path is
exercised on a sanitised page built here from raw PDF syntax: filled
rectangles for the cells and Helvetica text for the words, which is
all IB's layout engine emits and all pdfplumber needs to hand back
`page.rects` and `page.extract_words()`. Coordinates use pdfplumber's
convention (`top` measured from the top edge of a 792 x 612 pt
landscape page) and are converted to PDF's bottom-left origin on
output, so a test reads like the geometry it asserts on.

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

PAGE_WIDTH = 792.0
PAGE_HEIGHT = 612.0

# Helvetica's ascent and descent as fractions of the font size — what
# pdfminer uses to box a glyph, so a word's vertical centre can be
# placed inside a cell deliberately.
_ASCENT = 0.718
_DESCENT = 0.207


@dataclass(slots=True)
class PdfPage:
    """One page under construction: cells and words in pdfplumber coordinates."""

    _ops: list[str] = field(default_factory=list)

    def cell(self, x0: float, top: float, x1: float, bottom: float) -> PdfPage:
        """Draw a filled white rectangle — one table cell."""
        y = PAGE_HEIGHT - bottom
        self._ops.append(f"1 1 1 rg {x0:.2f} {y:.2f} {x1 - x0:.2f} {bottom - top:.2f} re f")
        return self

    def text(self, x: float, top: float, text: str, *, size: float = 7.0) -> PdfPage:
        """Place `text` with its glyph box starting at `top`, in Helvetica at `size` pt."""
        baseline = PAGE_HEIGHT - top - _ASCENT * size
        escaped = text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
        self._ops.append(f"BT /F1 {size:.1f} Tf {x:.2f} {baseline:.2f} Td ({escaped}) Tj ET")
        return self

    def row(
        self,
        top: float,
        bottom: float,
        columns: Sequence[tuple[float, float]],
        texts: Sequence[str],
    ) -> PdfPage:
        """Draw one table row: a cell per column, each text's lines 7 pt apart inside it."""
        for (x0, x1), value in zip(columns, texts, strict=True):
            self.cell(x0, top, x1, bottom)
            for line_no, line in enumerate(value.split("\n")):
                if line:
                    self.text(x0 + 2, top + 2 + 7 * line_no, line)
        return self

    def band(self, top: float, bottom: float, x0: float, x1: float, text: str) -> PdfPage:
        """Draw a one-cell row spanning `x0`..`x1` — a title or a sub-header."""
        self.cell(x0, top, x1, bottom)
        return self.text(x0 + 4, top + 2, text, size=10.0 if bottom - top >= 16 else 7.0)

    def content(self) -> bytes:
        """The page's content stream."""
        return "\n".join(self._ops).encode("latin-1")


def build_pdf(pages: list[PdfPage]) -> bytes:
    """Serialise `pages` as a valid single-font PDF with a correct xref table."""
    objects: list[bytes] = []
    page_ids = [4 + 2 * i for i in range(len(pages))]
    kids = " ".join(f"{pid} 0 R" for pid in page_ids)
    objects.append(b"<< /Type /Catalog /Pages 2 0 R >>")
    objects.append(f"<< /Type /Pages /Kids [{kids}] /Count {len(pages)} >>".encode())
    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    for pid, page in zip(page_ids, pages, strict=True):
        stream = page.content()
        objects.append(
            (
                f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {PAGE_WIDTH:.0f} {PAGE_HEIGHT:.0f}] "
                f"/Resources << /Font << /F1 3 0 R >> >> /Contents {pid + 1} 0 R >>"
            ).encode()
        )
        objects.append(f"<< /Length {len(stream)} >>\nstream\n".encode() + stream + b"\nendstream")

    out = bytearray(b"%PDF-1.4\n")
    offsets: list[int] = []
    for number, body in enumerate(objects, 1):
        offsets.append(len(out))
        out += f"{number} 0 obj\n".encode() + body + b"\nendobj\n"
    xref_at = len(out)
    out += f"xref\n0 {len(objects) + 1}\n".encode()
    out += b"0000000000 65535 f \n"
    for offset in offsets:
        out += f"{offset:010d} 00000 n \n".encode()
    out += (
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_at}\n%%EOF\n"
    ).encode()
    return bytes(out)


__all__ = ["PAGE_HEIGHT", "PAGE_WIDTH", "PdfPage", "build_pdf"]
