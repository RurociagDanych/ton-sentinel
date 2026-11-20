"""Bounded reader for TON bag-of-cells message bodies (DeDust trace recognizer)."""
from __future__ import annotations

import base64
import binascii

BOC_MAGIC = bytes.fromhex('b5ee9c72')
MAX_BOC_BYTES = 64 * 1024
MAX_CELLS = 1024


class Cell:
    __slots__ = ('bits', 'refs')

    def __init__(self, bits: str, refs: list):
        self.bits, self.refs = bits, refs


def parse_boc(text: str) -> Cell:
    """Decode a base64 BOC and return its first root cell."""
    if type(text) is not str:
        raise ValueError('BOC must be base64 text')
    try:
        data = base64.b64decode(text, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError('BOC is not base64') from exc
    if len(data) > MAX_BOC_BYTES or len(data) < 6 or data[:4] != BOC_MAGIC:
        raise ValueError('unsupported BOC')
    flags, off_bytes = data[4], data[5]
    has_idx, size = flags & 0x80, flags & 0x07
    if not 1 <= size <= 4 or not 1 <= off_bytes <= 8:
        raise ValueError('unsupported BOC header')
    pos = 6

    def read(n):
        nonlocal pos
        if pos + n > len(data):
            raise ValueError('truncated BOC')
        value = int.from_bytes(data[pos:pos + n], 'big')
        pos += n
        return value

    cells_n, roots_n, _absent = read(size), read(size), read(size)
    if not 1 <= cells_n <= MAX_CELLS or roots_n < 1:
        raise ValueError('BOC cell count outside bound')
    read(off_bytes)
    roots = [read(size) for _ in range(roots_n)]
    if has_idx:
        pos += cells_n * off_bytes
    raw = []
    for index in range(cells_n):
        d1, d2 = read(1), read(1)
        nrefs, nbytes = d1 & 7, (d2 + 1) // 2
        if nrefs > 4 or d1 & 8:
            raise ValueError('exotic or malformed cell')
        if pos + nbytes > len(data):
            raise ValueError('truncated BOC')
        body = data[pos:pos + nbytes]
        pos += nbytes
        bits = ''.join(f'{x:08b}' for x in body)
        if d2 & 1:
            stripped = bits.rstrip('0')
            if not stripped:
                raise ValueError('malformed completion tag')
            bits = stripped[:-1]
        refs = [read(size) for _ in range(nrefs)]
        if any(not index < r < cells_n for r in refs):
            raise ValueError('cell refs must point forward')
        raw.append((bits, refs))
    if not 0 <= roots[0] < cells_n:
        raise ValueError('root outside BOC')
    cells: list = [None] * cells_n
    for i in range(cells_n - 1, -1, -1):
        bits, refs = raw[i]
        cells[i] = Cell(bits, [cells[r] for r in refs])
    return cells[roots[0]]


class Slice:
    def __init__(self, cell: Cell):
        self.bits, self.refs, self.pos, self.ref_pos = cell.bits, cell.refs, 0, 0

    def remaining(self) -> int:
        return len(self.bits) - self.pos

    def uint(self, n: int) -> int:
        if n == 0:
            return 0
        if self.pos + n > len(self.bits):
            raise ValueError('cell underflow')
        value = int(self.bits[self.pos:self.pos + n], 2)
        self.pos += n
        return value

    def int(self, n: int) -> int:
        value = self.uint(n)
        return value - (1 << n) if value >> (n - 1) else value

    def coins(self) -> int:
        return self.uint(8 * self.uint(4))

    def ref(self) -> 'Slice':
        if self.ref_pos >= len(self.refs):
            raise ValueError('missing cell ref')
        cell = self.refs[self.ref_pos]
        self.ref_pos += 1
        return Slice(cell)

    def address(self) -> str | None:
        """MsgAddress -> 'wc:hex' (std, lower-case), None (addr_none) or 'extern:<bits>'."""
        tag = self.uint(2)
        if tag == 0:
            return None
        if tag == 1:
            n = self.uint(9)
            ext = self.bits[self.pos:self.pos + n]
            self.pos += n
            return 'extern:' + ext
        if tag == 2:
            if self.uint(1):
                raise ValueError('anycast address not supported')
            wc = self.int(8)
            return f'{wc}:{self.uint(256):064x}'
        raise ValueError('addr_var not supported')


def body_slice(text: str) -> Slice:
    return Slice(parse_boc(text))
