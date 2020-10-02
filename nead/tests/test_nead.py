import pytest

import nead

def test_read_format():
    df = nead.read("sample_csv.dsv")
    assert(df.attrs["__format__"] == "NEAD 1.0 UTF-8")

def test_MKS():
    df = nead.read("sample_csv.dsv", MKS=True)
    assert( df.iloc[0]['TA'].values == (2+273.15) )

def test_pandas_kws():
    df = nead.read("sample_csv.dsv", index_col=0)
    print("")
    print(df)
    print("")
    print(df.values)
    # assert( df.iloc[0]['TA'].values == (2+273.15) )

def test_attrs():
    df = nead.read("sample_csv.dsv")
    print(df.attrs)

def test_multi_index_false():
    df = nead.read("sample_csv.dsv", multi_index=False, index_col=0)
    print("")
    print(df)
    
