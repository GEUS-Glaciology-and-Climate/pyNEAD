import pytest

import nead

def test_read_format():
    df = nead.read("sample_csv.dsv")
    assert(df.attrs["__format__"] == "NEAD 1.0 UTF-8")

def test_MKS():
    df = nead.read("sample_csv.dsv", MKS=True, index_col=0, parse_dates=True)
    assert( df.iloc[0]['TA'] == (2+273.15) )
