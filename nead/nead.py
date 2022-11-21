"""
A Python module for reading and writing NEAD files
"""
import numpy as np
import pandas as pd
import xarray as xr
from io import StringIO
import configparser
from pathlib import Path
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
            if line.strip(' ') == '#': continue
            if line == "# [DATA]\n": break # done reading header
            if line == "# [FIELDS]\n":
                section = 'fields'
                continue # done reading header
            
            if line[0] == "\n": continue   # blank line
            assert(line[0] == "#")         # if not blank, must start with "#"
            
            key_eq_val = line.split("#")[1].strip()
            if key_eq_val == '' or key_eq_val == None: continue  # Line is just "#" or "# " or "#   #"...
            assert("=" in key_eq_val), print(line, key_eq_val)
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
                     usecols=np.arange(len(names)),
                     skip_blank_lines = True)

    ds = df.to_xarray()
    ds.attrs = meta

    # For each of the per-field properties, add as attributes to that variable.
    for key in fields.keys():      
        assert(len(fields[key].split(FD)) == len(names)), print('Error reading NEAD file: ',
                                                                key,' has ',
              len(fields[key].split(FD)),'items for ',len(names),' fields')
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

def read_header(header_path: str):
# Writes NEAD file (CSV file with NEAD formatted header)
# Columns written in NEAD output will be the fields designated in the
#   NEAD configuration file setting 'fields' in section 'FIELDS'
# Paths must be absolute
# Paths can use 'r' in front of strings with paths to designate them as a raw string if using Windows
#   style paths with backslashes
# Arguments:
#   data_frame    Pandas dataframe
#   nead_header   string with Path to NEAD header file, ex. r'C:\Users\Icy\gcnet\header\nead_header.ini'
#   output_path   string with Path assigned to output NEAD file,
#                 be sure to designate name for new NEAD file at end of string
#                 ex.  r'C:\Icy\gcnet\output\nead\my_nead_file_name')

    header_file = Path(header_path)

    # Load configuration file
    header = configparser.RawConfigParser(inline_comment_prefixes='#', allow_no_value=True)
    header.read(header_file)

    if len(header.sections()) < 1:
        print("Invalid header file, missing sections")
        return None
    return header

# Assign hash_lines with header lines prepended with '# '
def get_hashed_lines(header):
    hash_lines = []
    for line in header.replace('\r\n', '\n').split('\n'):
        line = '# ' + line + '\n'
        hash_lines.append(line)
    return hash_lines


def write(data_frame, nead_header, output_path):
    # Assign nead_output to output_path with .csv extension
    nead_output = Path('{0}'.format(output_path))

    # Read nead_header into conf
    if isinstance(nead_header, str):
        conf = read_header(Path(nead_header))
    else:
        conf = nead_header

    # Assign fields from nead_header 'fields', convert to list in fields_list
    fields =conf.get('FIELDS', 'fields')
    fields_list = fields.replace(" ", "").split(',')

    # Write conf into buffer
    buffer = StringIO()
    conf.write(buffer)

    # Assign hash_lines with nead_header lines prepended with '# '
    hash_lines = get_hashed_lines(buffer.getvalue())

    # Write hash_lines into nead_header
    with open(nead_output, 'w', newline='\n') as nead_header:
        nead_header.write('# NEAD 1.0 UTF-8\n')
        for row in hash_lines:
            nead_header.write(row)

    # Append data to header, omit indices, omit dataframe header, and output columns in fields_list
    with open(nead_output, 'a') as nead:
        data_frame.to_csv(nead, 
                          index=False,
                          header=False,
                          columns=fields_list,
                          float_format='%.2f',
                          line_terminator='\n')
        

def write_header(header_file_name, df,  metadata = ('metadata_name', 'metadata_value'),
                fields = '', add_value = '', scale_factor = '', units = '',
                display_description = '', database_fields = '', 
                database_fields_data_types = ''):
    # minimalistic NEAD header writing
    # Input: 
    #     header_file_name
    #           REQUIRED, string where the header ini file is saved
    #     df
    #           REQUIRED, dataframe containing the data. timestamp should be column, not index
    #     units
    #           REQUIRED, list of string, one for each column in df
    #     fields
    #           list of strings, length equals the number of columns in df, default is column names in df
    #     display_description
    #           list of strings, length equals the number of columns in df,default is column names in df
    #     database_fields
    #           list of strings, length equals the number of columns in df,default is column names in df
    #     database_fields
    #           list of strings, length equals the number of columns in df,default is column names in df
    #     database_fields_data_types
    #           list of strings, length equals the number of columns in df, default is df.dtypes
    #     add_value
    #           list of strings, length equals the number of columns in df, default is list of '0'
    #     scale_factor
    #           list of strings, length equals the number of columns in df, default is list of '1'
    #
    
    # default values
    if len(fields) == 0:
        fields = df.columns
    if not add_value:
        add_value = [str(n) for n in np.zeros(np.shape(df.columns), dtype=int)]
    if not scale_factor:
        scale_factor = [str(n) for n in np.ones(np.shape(df.columns), dtype=int)]
    if not display_description:
        display_description = df.columns
    if not database_fields:
        database_fields = df.columns
    if not database_fields_data_types:
        database_fields_data_types = ['timestamp'] + [str(s) for s in df.dtypes.values][1:]
                
    assert units, print('units need to be specified for each field')
    # assert database_fields_data_types,\
    #     print('database_fields_data_types need to be specified for each field')
    assert len(add_value) == len(fields),\
        print('add_value has length '+str(len(add_value))+' for '+str(len(fields))+' fields')
    assert len(scale_factor) == len(fields),\
        print('scale_factor has length '+str(len(scale_factor))+' for '+str(len(fields))+' fields')
    assert len(units) == len(fields),\
        print('units has length '+str(len(units))+' for '+str(len(fields))+' fields')
    assert len(display_description) == len(fields),\
        print('display_description has length '+str(len(display_description))+' for '+str(len(fields))+' fields')
    assert len(database_fields) == len(fields),\
        print('database_fields has length '+str(len(database_fields))+' for '+str(len(fields))+' fields')
    assert len(database_fields_data_types) == len(fields),\
        print('database_fields_data_types has length '+str(len(database_fields_data_types))+' for '+str(len(fields))+' fields')
    
    with open(header_file_name, 'w', newline='\n') as nead_header:
        nead_header.write('[METADATA]\n')
        for k in metadata.keys():
            nead_header.write(k+' = '+str(metadata[k])+'\n')
        nead_header.write('[FIELDS]\n')
        nead_header.write('fields = '+','.join(fields)+'\n')
        nead_header.write('add_value = '+','.join(add_value)+'\n')
        nead_header.write('scale_factor = '+','.join(scale_factor)+'\n')
        nead_header.write('units = '+','.join(units)+'\n')
        nead_header.write('display_description = '+','.join(display_description)+'\n')
        nead_header.write('database_fields = '+','.join(database_fields)+'\n')
        nead_header.write('database_fields_data_types = '+','.join(database_fields_data_types)+'\n')
        nead_header.write('[DATA]\n')


def build_header_obj(df,  metadata = ('metadata_name', 'metadata_value'),
                fields = '', add_value = '', scale_factor = '', units = '',
                display_description = '', database_fields = '', 
                database_fields_data_types = ''):
    # minimalistic NEAD header writing
    # REQUIRES: header_file_name, df, units
    
    # default values
    if len(fields) == 0:
        fields = df.columns
    if not add_value:
        add_value = [str(n) for n in np.zeros(np.shape(df.columns), dtype=int)]
    if not scale_factor:
        scale_factor = [str(n) for n in np.ones(np.shape(df.columns), dtype=int)]
    if not display_description:
        display_description = df.columns
    if not database_fields:
        database_fields = df.columns
    if not database_fields_data_types:
        database_fields_data_types = ['timestamp'] + [str(s) for s in df.dtypes.values][1:]
    
    # checking that metadata has appropriate shape
    assert units, print('units need to be specified for each field')
    # assert database_fields_data_types,\
    #     print('database_fields_data_types need to be specified for each field')
    assert len(add_value) == len(fields),\
        print('add_value has length '+str(len(add_value))+' for '+str(len(fields))+' fields')
    assert len(scale_factor) == len(fields),\
        print('scale_factor has length '+str(len(scale_factor))+' for '+str(len(fields))+' fields')
    assert len(units) == len(fields),\
        print('units has length '+str(len(units))+' for '+str(len(fields))+' fields')
    assert len(display_description) == len(fields),\
        print('display_description has length '+str(len(display_description))+' for '+str(len(fields))+' fields')
    assert len(database_fields) == len(fields),\
        print('database_fields has length '+str(len(database_fields))+' for '+str(len(fields))+' fields')
    assert len(database_fields_data_types) == len(fields),\
        print('database_fields_data_types has length '+str(len(database_fields_data_types))+' for '+str(len(fields))+' fields')
    
    config = configparser.ConfigParser(interpolation=None)
    config['METADATA'] = metadata
    config['FIELDS'] =  {
        'fields': ','.join(fields),
        'add_value': ','.join(add_value),
        'scale_factor': ','.join(scale_factor),
        'units': ','.join(units),
        'display_description': ','.join(display_description),
        'database_fields': ','.join(database_fields),
        'database_fields_data_types': ','.join(database_fields_data_types)}
    config['DATA'] = {}
    return config
    
