import pytest
import numpy as np


import nead

fname = "sample.csv"

def test_read_MKS():
    ds = nead.read(fname, index_col=0, MKS=True)
    assert(np.all(ds['TA'].values == [275.15, 276.15, 275.95]))

def test_read_attrs():
    ds = nead.read(fname, index_col=0, MKS=True)
    assert(ds['RH'].scale_factor == 0.01)
    
def test_print():
    ds = nead.read(fname, index_col=0)
    print(ds)

# def test_read_KAN_L():
#     ds = nead.read("KAN_L-2009.1-raw.txt")
    
def test_write():
    ds = nead.read(fname, index_col=0, MKS=True)
    df = ds.to_dataframe()
    df = df.reset_index()
    nead.write(df, nead_header = 'sample_header.ini', output_path = 'sample_out.csv')

    ds2 = nead.read('sample_out.csv', index_col=0, MKS=True)
    print(ds2)

    
# def test_read_format():
#     df = nead.read("sample_csv.dsv")
#     assert(df.attrs["__format__"] == "NEAD 1.0 UTF-8")

# def test_MKS():
#     df = nead.read("sample_csv.dsv", MKS=True)
#     assert( df.iloc[0]['TA'].values == (2+273.15) )

# def test_pandas_kws():
#     df = nead.read("sample_csv.dsv", index_col=0)
#     print("")
#     print(df)
#     print("")
#     print(df.values)
#     # assert( df.iloc[0]['TA'].values == (2+273.15) )

# def test_multi_index_false():
#     df = nead.read("sample_csv.dsv", multi_index=False, index_col=0)
#     print("")
#     print(df)
    

# def test_write_0():
#     df = nead.read("sample_csv.dsv", index_col=0, multi_index=False)
#     nead.write(df, filename="sample_write.csv", attrs=df.attrs)
    
