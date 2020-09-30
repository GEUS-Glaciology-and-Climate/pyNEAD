
import numpy as np
import pandas as pd

def read(neadfile, MKS=None, **kw):

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

        ## split everything on the column delimiter (CD) that uses or appears to use the CD.
        assert("column_delimiter" in attrs.keys())
        CD = attrs["column_delimiter"]

        # first split the fields field.
        assert("fields" in attrs.keys())
        nfields = len(attrs['fields'].split(CD))

        # Now split all other fields that contain CD and the same number of CD as fields
        for key in attrs.keys():
            if type(attrs[key]) is not str:
                continue
            if (CD in attrs[key]) & (len(attrs[key].split(CD)) == nfields):
                # probably a column property, because it has enough CDs
                attrs[key] = [_.strip() for _ in attrs[key].split(CD)]
                # convert to numeric if only contains numbers
                if all([str(s).strip('-').strip('+').replace('.','').isdigit() for s in attrs[key]]):
                    attrs[key] = np.array(attrs[key]).astype(np.float)
                    if all(attrs[key] == attrs[key].astype(np.int)):
                        attrs[key] = attrs[key].astype(np.int)
                else:
                    attrs[key] = np.array(attrs[key])
                # finally, convert from array to dictionary based on the field property
                if key != 'fields':
                    attrs[key] = dict(zip(attrs['fields'],attrs[key]))
                

    df = pd.read_csv(neadfile,
                     comment = "#",
                     sep = attrs['column_delimiter'],
                     names = attrs['fields'],
                     **kw)

    # # convert to MKS by adding units_offset and units_multiplier to a
    # # multi-header, selecting numeric columns, and converting.
    if (MKS != False):
        assert('units_offset' in attrs.keys())
        assert('units_multiplier' in attrs.keys())
        uo = attrs['units_offset'].values()
        um = attrs['units_multiplier'].values()
        df.columns = pd.MultiIndex.from_tuples(list(zip(df.columns, uo, um)), names=['name','uo','um'])
        df_n = df.select_dtypes(include='number')
        uo = df_n.columns.get_level_values('uo')
        um = df_n.columns.get_level_values('um')

        if('nodata' in attrs.keys()): df_n = df_n.replace(attrs['nodata'],np.nan)
        df_n = (df_n  * um) + uo
        if('nodata' in attrs.keys()): df_n = df_n.replace(np.nan,attrs['nodata'])
        
        for c in df_n.columns: df[c] = df_n[c] # move back over
        df.columns = df.columns.get_level_values('name')

    df.attrs = attrs
    return df

# def write(df, filename=None, header=None):

#     if header is None:

#         assert(df.attrs is not None)

#         # convert column delimiter to both NEAD(human) and computer-useful values
#         cds = {'\\s':"space", '\\s+':"whitespace", '\t':"tab"}
#         cd = df.attrs["column_delimiter"]
#         sepstr = cds[cd] if cd in cds.keys() else cd
#         df.attrs.pop("column_delimiter") # we'll write it manually at top

#         header = '# NEAD 1.0 UTF-8\n'
#         header += '# [HEADER]\n'
#         header += '## Written by pyNEAD\n'
#         header += '# column_delimiter = ' + sepstr + '\n'

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
