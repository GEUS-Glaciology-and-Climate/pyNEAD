import pytest

import nead

def test_read_format():
    df = nead.read("sample_csv.dsv")
    assert(df.attrs["__format__"] == "NEAD 1.0 UTF-8")
