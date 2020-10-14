"""
A Python module for reading and writing NEAD files
"""
import numpy as np
import pandas as pd
import xarray as xr
xr.set_options(keep_attrs=True)

def read(neadfile, MKS=None, multi_index=True, index_col=None):
    """Read a NEAD file

    PARAMETERS
    ----------
    file: string
        Path to NEAD-formatted file


    KEYWORDS
    --------
    index_col: integer
        Use column as index
    
    RETURNS
    -------
    An xarray dataset.
    """


    with open(neadfile) as f:
        fmt = f.readline();
        assert(fmt[0] == "#")
        assert(fmt.split("#")[1].split()[0] == "NEAD")
        assert(fmt.split("#")[1].split()[1] == "1.0")
        assert(fmt.split("#")[1].split()[2] == "UTF-8")
        
        line = f.readline()
        assert(line[0] == "#")
        assert(line.split("#")[1].strip() == '[METADATA]')

        meta = {}
        fields = {}
        section = 'meta'
        while True:
            line = f.readline()
            if line == "# [DATA]\n": break # done reading header
            if line == "# [FIELDS]\n":
                section = 'fields'
                continue # done reading header
            
            if line[0] == "\n": continue   # blank line
            assert(line[0] == "#")         # if not blank, must start with "#"
            
            key_eq_val = line.split("#")[1].strip()
            if key_eq_val == "": continue  # Line is just "#" or "# " or "#   #"...
            assert("=" in key_eq_val)
            key = key_eq_val.split("=")[0].strip()
            val = key_eq_val.split("=")[1].strip()

            # Convert from string to number if it is a number
            if val.strip('-').strip('+').replace('.','').isdigit():
                val = np.float(val)
                if val == np.int(val):
                    val = np.int(val)

            if section == 'meta': meta[key] = val
            if section == 'fields': fields[key] = val
    # done reading header

    # Find delimiter and fields for reading NEAD as simple CSV
    assert("field_delimiter" in meta.keys())
    assert("fields" in fields.keys())
    FD = meta["field_delimiter"]
    names = [_.strip() for _ in fields.pop('fields').split(FD)]

    df = pd.read_csv(neadfile,
                     comment = "#",
                     names = names,
                     sep = FD,
                     skip_blank_lines = True)

    # ds = df.to_xarray() # should work
    ## The above line is all that's needed, but this fails on some files - memory fills up
    ## Building the xarray dataset ourself avoids this failure.
    ## This is a strange bug. These are not large files.
    ## KAN_L-2009.1-raw.txt is only 12 MB (!) and my machine has 32 GB RAM
    ds = xr.Dataset({df.columns[0]: xr.DataArray(data=df[df.columns[0]], dims=['index'], coords={'index':df.index})})
    for c in df.columns[1:]:
        ds[c] = (('index'), df[c])
    ### Done building dataset
    
    ds.attrs = meta

    # For each of the per-field properties, add as attributes to that variable.
    for key in fields.keys():
        assert(len(fields[key].split(FD)) == len(names))
        arr = [_.strip() for _ in fields[key].split(FD)]
        # convert to numeric if only contains numbers
        if all([str(s).strip('-').strip('+').replace('.','').isdigit() or str(s) == "" for s in arr]):
            arr = np.array(arr).astype("<U32")
            arr[arr == ""] = 'nan'
            arr = arr.astype(np.float)
            if all(arr == arr.astype(np.int)):
                arr = arr.astype(np.int)
                    
        for i,v in enumerate(ds.data_vars):
            # print(i,v)
            ds[v].attrs[key] = arr[i]
                

    # Convert to MKS if requested
    if MKS == True:
        assert("scale_factor" in fields.keys())
        assert("add_value" in fields.keys())
        for v in list(ds.keys()):
            if ds[v].dtype.kind in ['i','f']:
                ds[v] = (ds[v] * ds[v].scale_factor) + ds[v].add_value


    # Set index_col if requested
    if index_col != None:
        colname = list(ds.keys())[index_col]
        # ds = ds.set_coords(colname)
        ds = ds.swap_dims({'index':colname}).reset_coords(names='index', drop=True)
        ds[colname] = ds[colname].astype(np.datetime64)
            
    # Clean up.
    if('nodata' in ds.attrs.keys()): ds = ds.where(ds != ds.attrs['nodata'])

    return ds
