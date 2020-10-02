
import numpy as np
import pandas as pd

def read(neadfile, MKS=None, multi_index=True, index_col=None, **kw):

    with open(neadfile) as f:
        fmt = f.readline();
        assert(fmt[0] == "#")
        assert(fmt.split("#")[1].strip() == "NEAD 1.0 UTF-8")
        
        hdr = f.readline()
        assert(hdr[0] == "#")
        assert(hdr.split("#")[1].strip() == "[HEADER]")

        line = ""
        attrs = {}
        attrs["__format__"] = fmt.split("#")[1].strip()

        while True:
            line = f.readline()
            if line == "# [DATA]\n": break # done reading header
            if line[0] == "\n": continue   # blank line
            assert(line[0] == "#")         # if not blank, must start with "#"
            
            key_eq_val = line.split("#")[1].strip()
            if key_eq_val == "": continue  # Line is just "#" or "# " or "#   #"...
            assert("=" in key_eq_val)
            key = key_eq_val.split("=")[0].strip()
            val = key_eq_val.split("=")[1].strip()

            if val.strip('-').strip('+').replace('.','').isdigit():
                val = np.float(val)
                if val == np.int(val):
                    val = np.int(val)
            
            attrs[key] = val
        # done reading header

        ## split everything on the field delimiter (FD) that uses or appears to use the FD.
        assert("field_delimiter" in attrs.keys())
        FD = attrs["field_delimiter"]

        # first split the fields field.
        assert("fields" in attrs.keys())
        mi = {} # multi-index collector
        mi['fields'] = [_.strip() for _ in attrs.pop('fields').split(FD)]
        nfields = len(mi['fields'])

        # Now split all other fields that contain FD and the same number of FD as fields
        attr_keys = list(attrs.keys()) # don't iterate on this, so we can pop()
        for key in attr_keys:
            if type(attrs[key]) is not str:
                continue
            if (FD in attrs[key]) & (len(attrs[key].split(FD)) == nfields):
                # probably a column property, because it has the correct number of FDs
                arr = [_.strip() for _ in attrs.pop(key).split(FD)]
                # convert to numeric if only contains numbers
                if all([str(s).strip('-').strip('+').replace('.','').isdigit() for s in arr]):
                    arr = np.array(arr).astype(np.float)
                    if all(arr == arr.astype(np.int)):
                        arr = arr.astype(np.int)
                mi[key] = arr

               
    mindex = pd.MultiIndex.from_arrays([_ for _ in mi.values()],
                                       names=[_ for _ in mi.keys()])
    df = pd.read_csv(neadfile,
                     comment = "#",
                     sep = attrs['field_delimiter'],
                     names = mindex,
                     parse_dates = True)

    # some sort of bug (?) - the colum "names" are dropped from the multiindex,
    # so the follow line adds them back.
    df.columns = mindex

    # convert to MKS by adding add_value and scale_factor to a
    # multi-header, selecting numeric columns, and converting.
    if (MKS == True):
        assert('add_value' in df.columns.names)
        assert('scale_factor' in df.columns.names)
        for i,c in enumerate(df.columns):
            if df[c].dtype.kind in ['i','f']:
                df[c] = (df[c] * df.columns.get_level_values('scale_factor')[i]) + \
                    df.columns.get_level_values('add_value')[i]
        if('nodata' in attrs.keys()): df = df.replace(np.nan, attrs['nodata'])

    # If we pass kws to read_csv above, it causes issues. For example, if index_col
    # is set, I now need to figure out which column(s?) that is/are and handle all
    # the other metadata that is per-column values. Instead, I find it easier to read
    # in the data 1x simply, build the multiindex header, and then write-and-read (via
    # memory) a second time to better parse all of the per-column values. This has
    # memory and speed implications for large files...
    #
    # For now this is only tested on the 'index_col' keyword passed to Pandas.
    # if bool(kw): # some **kw was set.
    if index_col is not None:
        cname = df.columns.get_level_values("fields")[index_col]
        df = df.set_index(df.columns[index_col])
        df.index.name = cname
        # from io import StringIO
        # n = df.columns.names
        # df = pd.read_csv(StringIO(df.to_csv(index=False)),
        #                  header = list(np.arange(df.columns.nlevels)),
        #                  **kw)
        # df.columns.names = n


    if multi_index == False: df.columns = df.columns.droplevel(list(np.arange(1,df.columns.nlevels)))
        
    # Finally, attach all the non-column metadata to the attrs dictionary.
    df.attrs = attrs
    
    return df

# def write(df, filename=None, header=None):

#     if header is None:

#         assert(df.attrs is not None)

#         # convert column delimiter to both NEAD(human) and computer-useful values
#         cds = {'\\s':"space", '\\s+':"whitespace", '\t':"tab"}
#         cd = df.attrs["field_delimiter"]
#         sepstr = cds[cd] if cd in cds.keys() else cd
#         df.attrs.pop("field_delimiter") # we'll write it manually at top

#         header = '# NEAD 1.0 UTF-8\n'
#         header += '# [HEADER]\n'
#         header += '## Written by pyNEAD\n'
#         header += '# field_delimiter = ' + sepstr + '\n'

#         for key in df.attrs:
#             if isinstance(df.attrs[key], list):
#                 header += '# ' + key + ' = ' + " ".join(str(i) for i in df.attrs[key]) + '\n'
#             else:
#                 header += '# ' + key + ' = ' + str(df.attrs[key]) + '\n'
#         header += '# [DATA]\n'

#     # Conert datetime columns to ISO-8601 format
#     for c in df.columns:
#         if df[c].dtype == "datetime64":
#             df[c] = df[c].strftime('%Y-%m-%dT%H:%M:%S')
            
#     with open(filename, "w") as f:
#         f.write(header)
#         f.write(df.to_csv(header=False, sep=sep))
