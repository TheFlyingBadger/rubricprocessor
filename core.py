#!/usr/bin/python3

import pandas   as pd

from   datetime       import datetime
from   distutils.util import strtobool
from   io             import BytesIO             \
                            ,StringIO
from   lxml           import etree
from   os             import getenv              \
                            ,path                \
                            ,remove              \
                            ,stat
from   pathlib        import Path
from   sqlite3        import connect as SQLconnect
from   zipfile        import ZipFile             \
                            ,ZIP_DEFLATED
#
###############################################################################################################################################################
#
GLOBAL_INDEX              = 0
MAX_WORKSHEET_NAME_LENGTH = 31
#
###############################################################################################################################################################
#
def nextGI():
    global GLOBAL_INDEX
    GLOBAL_INDEX += 1
    return GLOBAL_INDEX
#
###############################################################################################################################################################
#
def getFileFromZip (zipFileName, fileNameInZip):
    with ZipFile(zipFileName,'r') as my_zip_file:
       theFile = BytesIO(my_zip_file.read(fileNameInZip))
    #print (type(theFile))
    return theFile
#
###############################################################################################################################################################
#
def removekey(d, key):
    r = dict(d)
    if key in r:
        del r[key]
    return r
#
###############################################################################################################################################################
#
def isNumeric (val):
    return val.replace('.','',1).isdigit()
#
###############################################################################################################################################################
#
def StringToBoolean (val):
    if isinstance(val, (list,pd.core.series.Series)):
        r = []
        for x in val:
            r.append (strtobool(x))
    else:
        r = strtobool(val)
    return r
#
###############################################################################################################################################################
#
def checkDictKeyValueNumeric (d, k):
    r = d
    if (k not in r
     or r[k] == "null"
     or len(r[k]) == 0
     or not isNumeric(r[k])
       ):
          r = removekey(r,k)
    else:
          r[k] = float(r[k])
    return r
#
###############################################################################################################################################################
#
def getXMLFromZip (data, fileName, outFile = ""):
    theFile = (getFileFromZip (data['ZIPFILE'], fileName)).getvalue()
    if  (data['WRITE_DATA_XML']
     and outFile is not None
     and len(outFile) > 0
        ):
        fname = f"{data['TEMP_FOLDER']}\{outFile}"
        with open(fname,'wb') as f:
            f.write (theFile)
        pp = etree.tostring(etree.parse(fname), pretty_print=True)
        with open(fname,'wb') as f:
            f.write (pp)
        pp    = None
        x     = None
        fname = None

    return theFile
#
###############################################################################################################################################################
#
def getAttr (e, attr, ns = "", nsmap = ""):
    path = "@"
    if ns is not None and len(ns) > 0:
        path += f"{ns}:"
    path += attr
    if nsmap is not None and len(nsmap) > 0:
        lst = e.xpath(path, namespaces=nsmap)
    else:
        lst = e.xpath(path)
    return str(lst[0])
#
###############################################################################################################################################################
#
def getElement (e, element, ns = "", nsmap = ""):
    path = "./"
    if ns is not None and len(ns) > 0:
        path += f"{ns}:"
    path += element
    if nsmap is not None and len(nsmap) > 0:
        lst = e.xpath(path, namespaces=nsmap)
    else:
        lst = e.xpath(path)
    #print (type(lst[0]))
    return lst[0]
#
###############################################################################################################################################################
#
def getElementText (e, element, ns = "", nsmap = ""):
    return getElement (e, element, ns, nsmap).text
#
###############################################################################################################################################################
#
def getElementValue (e, element, ns = "", nsmap = ""):
    return getAttr (getElement (e, element, ns, nsmap), "value")
#
###############################################################################################################################################################
#
def getElementID (e, element, ns = "", nsmap = ""):
    return getAttr (getElement (e, element, ns, nsmap), "id")
#
###############################################################################################################################################################
#
def getStudentGUID (conn, username):
    with conn:
        cur  = conn.cursor()
        cur.execute(f"SELECT GUID FROM gradecentre where username = \'{username}\'")
        rows = cur.fetchall()
        cur  = None

    if len(rows) == 0:
        guid = None
        #raise LookupError (f"username \'{username}\' not found")
    elif len(rows) > 1:
        raise LookupError (f">1 username \'{username}\' found")
    else:
        guid = rows[0][0]
    return guid
#
###############################################################################################################################################################
#
def getStudentGUIDfromID (conn, studentid):
    with conn:
        cur  = conn.cursor()
        cur.execute(f"SELECT GUID FROM users where id = \'{studentid}\'")
        rows = cur.fetchall()
        cur  = None

    if len(rows) == 0:
        guid = None
        #raise LookupError (f"id \'{studentid}\' not found")
    elif len(rows) > 1:
        raise LookupError (f">1 id \'{studentid}\' found")
    else:
        guid = rows[0][0]
    return guid
#
###############################################################################################################################################################
#
def to_xml(df, filename=None, mode='w', itemTag = "item", itemsTag = "items"):
    def row_to_xml(row):
        xml = [f'<{itemTag}>']
        for i, col_name in enumerate(row.index):
            xml.append(f'  <{col_name} value=\"{row.iloc[i]}\">')
        xml.append(f'</{itemTag}>')
        return '\n'.join(xml)
    res = f'<{itemsTag}>\n'
    res += '\n'.join(df.apply(row_to_xml, axis=1))
    res += f'\n</{itemsTag}>'

    if filename is None:
        return res
    with open(filename, mode) as f:
        f.write(res)
#
###############################################################################################################################################################
#
def to_JSON (data, df, filename=None):
    # without the reset_index we don't get the index column in the data
    the_JSON = df.reset_index().to_json(orient = "records",index = True)
    from json import dumps, loads
    if  (data['WRITE_DATA_JSON']
     and filename is not None
     and len(filename) > 0
        ):
        # for file output we "pretty-print" the JSON
        with open(filename, 'w') as f:
            f.write(dumps(loads(the_JSON), indent=2))
    return the_JSON
#
###############################################################################################################################################################
#
def to_json (data, df, filename=None):
    # this version just calls the other, but doesn't return a value
    theJSON = to_JSON (data, df, filename)
    theJSON = None
#
###############################################################################################################################################################
#
def backupDB (connSrc, connDest):
    # def progress(status, remaining, total):
        # print(f'Copied {total-remaining} of {total} pages...')
    with connDest:
        connSrc.backup(connDest) #, pages=10, progress=progress)
#
###############################################################################################################################################################
#
def backupDBtoFile (data, conn):
    destDB = data['SQLLITE_OUTPUT']
    # print (destDB)
    # print (f"Backing up to '{destDB}'")
    bck = SQLconnect(destDB)
    backupDB (connSrc = conn, connDest = bck)
    bck.close()
    destDB = None
    bck = None
#
###############################################################################################################################################################
#
def createDBConnection(data,reloadFromFile = True):
    conn = SQLconnect(':memory:')
    if reloadFromFile and path.isfile(data['SQLLITE']):
        src = SQLconnect(data['SQLLITE'])
        backupDB (connSrc = src, connDest = conn)
    return conn
#
###############################################################################################################################################################
#
def removeFolder (folderName):
    if path.exists(folderName):
        from   shutil         import rmtree
        rmtree(folderName)
#
###############################################################################################################################################################
#
def createOrClearFolder (folderName):
    removeFolder(folderName)
    from os import makedirs
    makedirs(folderName)
#
###############################################################################################################################################################
#
def createZipArchive(data, deleteAfterwards = False):
    if path.isfile(data['SQLLITE_OUTPUT']):
        deleteIfExists(data['SQLLITE'])
        from shutil import copyfile
        copyfile (src = data['SQLLITE_OUTPUT']
                 ,dst = data['SQLLITE']
                 )
        if not ("KEEP_SQLLITE" in data and data["KEEP_SQLLITE"]):
            deleteIfExists (data['SQLLITE_OUTPUT'])
    from os import listdir
    with ZipFile (data['processedArchive'],"w",compression = ZIP_DEFLATED) as archive:
        for f in listdir(data['TEMP_FOLDER']):
            fullPath = path.join (data['TEMP_FOLDER'],f)
            fname = Path(fullPath)
            aname = Path(path.join (data['shortname'],f))
            archive.write(filename      = fname
                         ,arcname       = aname
                         )
            if f == "metadata.json":
                # The comment associated with the ZIP file as a bytes object. If assigning a comment to a ZipFile instance created with mode
                #    'w', 'x' or 'a', it should be no longer than 65535 bytes. Comments longer than this will be truncated.
                with open(fullPath,'rb') as comm:
                    comments = comm.read()
                archive.comment = comments
                comments = None
            fname = None
            aname = None

    if deleteAfterwards:
        removeFolder(data['TEMP_FOLDER'])

    return data
#
###############################################################################################################################################################
#
def SQLtoDF (conn, sqlStm):
    return  pd.read_sql_query(sqlStm,conn)
#
###############################################################################################################################################################
#
def tableToDF (conn, table):
    return  SQLtoDF (conn,f'SELECT t.* FROM {table} t')
#
###############################################################################################################################################################
#
def DFtoCSV (df, filename):
    df.to_csv (path_or_buf = filename
              ,sep         = ','
              ,na_rep      = ''
              ,header      = True
              ,index       = False
              )
#
###############################################################################################################################################################
#
def SQLtoCSV (conn, sqlStm, filename):
    DFtoCSV (SQLtoDF(conn,sqlStm),filename)
#
###############################################################################################################################################################
#
def tableToCSV (conn, table, filename):
    DFtoCSV (tableToDF(conn,table),filename)
#
###############################################################################################################################################################
#
def dateTimeFromEpoch (ms):
    return datetime.fromtimestamp(ms)
#
###############################################################################################################################################################
#
def dateTimeStringFromEpoch (ms):
    return dateTimeFromEpoch(ms).strftime('%Y-%m-%d %H:%M:%S')
#
###############################################################################################################################################################
#
def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)
#
###############################################################################################################################################################
#
def DFToSQL (df, table, con):
    # print ('*'*80)
    # print (table)
    defaultVClen = 255
    df = df.dropna(axis=1, how='all')
    from sqlalchemy import types as SQLtypes
    def sqlcol(dfparam):
        dtypedict = {}
        for i,j in zip(dfparam.columns, dfparam.dtypes):
            # print('...', i, str(j))
            if "object" in str(j):
               try:
                  vcLen = dfparam[i].map(len).max()
               except:
                  vcLen = defaultVClen
               dtype = SQLtypes.NVARCHAR(length=vcLen)
               vcLen = None
            elif "datetime" in str(j):
               dtype = SQLtypes.DateTime()
            elif "float" in str(j):
               dtype = SQLtypes.Float(precision=3, asdecimal=True)
            elif "int" in str(j) or "bool" in str(j):
               dtype = SQLtypes.INT()
            else:
               dtype = SQLtypes.NVARCHAR(length=defaultVClen)
            dtypedict.update({i: str(dtype)})
            # print (i, str(dtype))
            dtype = None
        return dtypedict
    def filterUserWarnings (action = "ignore"):
        from   warnings       import filterwarnings
        filterwarnings(action=action, category=UserWarning)
    dfHasColsWithSpaces = len([col for col in df.columns if col.find(' ') > -1]) > 0

    if dfHasColsWithSpaces:
        # Turn off pandas warning about cols with spaces
        filterUserWarnings ()

    df.to_sql(table, con=con,if_exists="replace",dtype = sqlcol(df))

    if dfHasColsWithSpaces:
        filterUserWarnings (action='default')

    dfHasColsWithSpaces = None
    return df
#
###############################################################################################################################################################
#
def dfToSQL (df, table, con):
    df = DFToSQL(df, table, con)
#
###############################################################################################################################################################
#
def replaceBRwithLF (str_in):
    str_out  = str_in
    with_    = '\n'
    for what in ['<br>'
                ,'<br/>'
                ,'<br></br>'
                ]:
        str_out = str_out.replace(what,with_)
        str_out = str_out.replace(what.upper(),with_) # UPPER case variant
        str_out = str_out.replace(what.title(),with_) # Title case variant
    with_    = None
    return str_out
#
###############################################################################################################################################################
#
def guessMimeType (filePath):
    from   mimetypes  import guess_type
    mime = guess_type(filePath)[0]
    if mime is None:
        mime = f"application/unknown{path.splitext(filePath)[1].lower()}"
    return mime
#
###############################################################################################################################################################
#
def ensureFiletype (filePath, mimeTypes):
    if isinstance(mimeTypes,str):
        mimeTypes = [mimeTypes]
    fileMime   = guessMimeType(filePath)
    matches = [_mime for _mime in mimeTypes if _mime == fileMime]
    if matches is None or len(matches) == 0:
        raise ImportError (f"file \'{filePath}\' is of the wrong type (\'{fileMime}\')")
#
###############################################################################################################################################################
#
def checkFileExists (filePath
                    ,ensureRW = False
                    ):
    if not path.exists(filePath):
        # Check the folder exists
        raise FileNotFoundError(f"file \'{filePath}\' not found")
    elif not path.isfile(filePath):
        # Check it's not a file
        raise IsADirectoryError (f"\'{filePath}\' is not a file")
    elif ensureRW:
        try:
            touch (filePath)
        except:
            raise PermissionError(f"file \'{filePath}\' is not writable")
#
###############################################################################################################################################################
#
def checkFileWritable (filePath):
    #
    # this one doesn't care if the file does not exist, only that it's writable
    #
    if path.exists(filePath):
        if not path.isfile(filePath):
            raise IsADirectoryError (f"\'{filePath}\' is not a file")
        else:
            deleteAfterwards = False
    else:
        deleteAfterwards = True

    try:
        touch (filePath)
    except:
        raise PermissionError(f"file \'{filePath}\' is not writable")

    if deleteAfterwards:
        deleteIfExists(filePath)
#
###############################################################################################################################################################
#
def touch(fname, mode=0o666, dir_fd=None, **kwargs):
    from os import open, O_CREAT, O_APPEND, fdopen, supports_fd, utime
    flags = O_CREAT | O_APPEND
    with fdopen(open(fname, flags=flags, mode=mode, dir_fd=dir_fd)) as f:
        utime(f.fileno() if utime in supports_fd else fname, dir_fd=None if supports_fd else dir_fd, **kwargs)
#
###############################################################################################################################################################
#
def ensureFiletypeCSV (filePath
                      ,headersFirstRow = True
                      ):
    ensureFiletype (filePath  = filePath
                   ,mimeTypes = ['text/comma-separated-values'
                                ,'text/csv'
                                ,'text/anytext'
                                ,'application/csv'
                                ,'application/excel'
                                ,'application/vnd.msexcel'
                                ,'application/vnd.ms-excel'
                                ,'text/plain'
                                ,'application/text'
                                ]
                   )

#
###############################################################################################################################################################
#
def gradeCentreCheck (gcFilePath):
    ensureFiletypeCSV (filePath  = gcFilePath)
    _gradeCentre ({'GRADECENTRE_CSV' : gcFilePath
                  }
                 )
#
###############################################################################################################################################################
#
def _gradeCentre (data):
    # ### read the GradeCentre CSV file
    gc = pd.read_csv(data['GRADECENTRE_CSV'])

    renamedCols = []
    for ix, col in enumerate(gc.columns):
        if col.find('[') > 0:
            arr = col.split('[')
            col = (arr[0]).strip()
        col = col.replace('_',' ').lower().replace(' ','')
        renamedCols.append(col)

    gc.columns = renamedCols
    gc.dropna(subset=['studentid'], inplace=True)

    cols = ["username"
           ,"studentid"
           ,"firstname"
           ,"lastname"
           ,"course"
           ,"unitattemptstatus"
           ,"location"
           ,"lastaccess"
           ,"availability"
           ]
    toDrop = gc.columns.difference(cols)
    gc.drop(toDrop, axis=1, inplace = True)
    missingCols = list(set(cols) - set(gc.columns))
    if missingCols is not None and len(missingCols) > 0:
        nl = '\n,'
        raise Exception (f"Not all expected columns in file\n({nl.join(missingCols)}) are missing")

    guids = []
    ix = []
    if "Unit Code" in data:
        prefix = data["Unit Code"]
    else:
        prefix = ""
    for d in range(len(gc.index)):
        studentIX = nextGI()
        ix.append(studentIX)
        guids.append(f'{prefix}_{str(studentIX).zfill(4)}')
    gc['GUID'] = guids
    gc['ix'] = ix
    gc['availability'] = StringToBoolean(gc['availability'])
    guids = None
    ix = None

    gc = gc.set_index("username")

    return gc
#
###############################################################################################################################################################
#
def gradeCentre (data, conn):
    gc = _gradeCentre (data)
    gc.to_sql ('gradecentre', con=conn)
    to_json(data,gc,path.join(data['TEMP_FOLDER'],"gradecentre.json"))
#
###############################################################################################################################################################
#
def imsManifestCheck (zipFileName):
    ensureFiletype (filePath  = zipFileName
                   ,mimeTypes = ['application/zip'
                                ,'application/x-compressed'
                                ,'application/x-zip-compressed'
                                ,'multipart/x-zip'
                                ]
                   )
    _imsManifest ({'ZIPFILE'        : zipFileName
                  ,'WRITE_DATA_XML' : False
                  }
                 )

#
###############################################################################################################################################################
#
def _imsManifest (data):
    types = {}
    wantedTypes = ["course/x-bb-coursesetting"
                  ,"course/x-bb-rubrics"
                  ,"course/x-bb-crsrubricassocation"
                  ,"course/x-bb-crsrubriceval"
                  ,"course/x-bb-gradebook"
                  ,"course/x-bb-user"
                  ,"membership/x-bb-coursemembership"
                  ]
    # ### Get and loop through "imsmanifest.xml" and find the files we're interested in
    xml = etree.XML(getXMLFromZip(data,'imsmanifest.xml',"manifest.xml"))
    for e in xml.xpath('//resource'):
        type_ = getAttr(e,"type")
        if type_ in wantedTypes:
            file_  = getAttr (e,"file","bb",xml.nsmap)
            title_ = getAttr (e,"title","bb",xml.nsmap)
            if type_ not in types:
                types[type_] = []
            types[type_].append({"file"     : file_
                                ,"title"    : title_
                                ,"outfile"  : (type_.split('-')[-1])+".xml"
                                }
                               )

    if len(types.items()) < len(wantedTypes):
        raise LookupError (f"Missing Types")

    simpleTypes = []

    zipContents = []
    with ZipFile(data['ZIPFILE'], 'r') as zip:
        for z in zip.infolist():
            zipContents.append (z.filename)

    missingFiles = []

    for type_, typeDictArr in types.items():
        if len(typeDictArr) != 1:
            raise LookupError (f"> 1 file for '{type_}'")
        thisSimple = {"fileDef"  : type_
                     ,"fileName" : typeDictArr[0]["file"]
                     ,"title"    : typeDictArr[0]["title"]
                     }
        thisSimple["inZip"] = (thisSimple["fileName"]  in zipContents)
        simpleTypes.append(thisSimple)
    missingFiles = [type_ for type_ in simpleTypes if type_["inZip"] == False]
    if len(missingFiles) > 0:
        message = "\nThe following source files (as specified in \'imsmanifest.xml\') are missing from the ZIP file :"
        for missing in missingFiles:
            message += f"\n{missing['fileDef'].ljust(32, ' ')} : \'{missing['fileName']}\'"
        raise Exception (message)


    df = pd.DataFrame(simpleTypes)
    df = df.set_index("fileDef")
    simpleTypes = None
    xml         = None
    wantedTypes = None
    return {"types"   : types
           ,"df"      : df
           }
#
###############################################################################################################################################################
#
def imsmanifest (data, conn):

    manifest = _imsManifest (data)
    dfToSQL (manifest["df"], 'manifest', con=conn)
    to_json(data,manifest["df"],path.join(data['TEMP_FOLDER'],"manifest.json"))

    return manifest["types"]
#
###############################################################################################################################################################
#
def deleteIfExists(filePath):
    if path.isfile(filePath):
        try:
            remove(filePath)
        except Exception as e:
            raise Exception(str(e))
#
###############################################################################################################################################################
#
def createBatchMetadata (data, conn, types):
    from   getpass        import getuser
    batchMetadata = data
    batchMetadata["Source Filename"] = data['ZIPFILE']
    batchMetadata["unitname"]        = types["course/x-bb-coursesetting"][0]["title"]
    batchMetadata["shortname"]       = batchMetadata["unitname"].split(' ')[0]
    batchMetadata["Unit Code"]       = batchMetadata["shortname"].split('.')[0]
    batchMetadata["processeddate"]   = datetime.now()
    batchMetadata["processedby"]     = getuser()


    batchMetadata["outputPrefix"]     = path.join(batchMetadata["OUTPUT_FOLDER"],batchMetadata["shortname"].replace("_","."))
    batchMetadata["processedArchive"] = f'{batchMetadata["outputPrefix"]}.zip'
    deleteIfExists(data['processedArchive'])
    batchMetadata["SQLLITE"]          = path.join(data['TEMP_FOLDER'],"sqlite.db")
    batchMetadata["SQLLITE_OUTPUT"]   = f'{batchMetadata["outputPrefix"]}.db'
    deleteIfExists(data['SQLLITE_OUTPUT'])

    batchMetadata["EXCEL_OUTPUT"]   = f'{batchMetadata["outputPrefix"]}.xlsx'
    deleteIfExists(data['EXCEL_OUTPUT'])

    info = stat(data['ZIPFILE'])
    batchMetadata["Source Size"]                      = sizeof_fmt(info.st_size)
    batchMetadata["Source Size (bytes)"]              = info.st_size
    batchMetadata["Source Time of last access"]       = dateTimeFromEpoch(info.st_atime)
    batchMetadata["Source Time of last modification"] = dateTimeFromEpoch(info.st_mtime)
    batchMetadata["Source Created time"]              = dateTimeFromEpoch(info.st_ctime)
    info = None

    df = pd.DataFrame([batchMetadata])
    df = df.set_index("ZIPFILE")
    dfToSQL (df, 'metadata', con=conn)
    to_json(data,df,path.join(data['TEMP_FOLDER'],"metadata.json"))
    return batchMetadata
#
###############################################################################################################################################################
#
def getFileForType(name, data, types):
    if name not in types:
        raise Exception(f"{name} not defined")
    files = types[name]
    if files is None or len(files) == 0:
        raise Exception(f"No {name} found in archive")
    elif len(files) > 1:
        raise Exception(f"Too many {name} files found in archive ({len(files)})")
    return etree.XML(getXMLFromZip(data,files[0]["file"],files[0]["outfile"]))
#
###############################################################################################################################################################
#
def x_bb_coursesetting (data, conn, types):

    xml       = getFileForType('course/x-bb-coursesetting', data, types)
    courseXML = xml.xpath("/COURSE")
    courses   = []
    for c in courseXML:
        courses.append ( {"ix"       : nextGI()
                         ,"id"       : getAttr(c,"id")
                         ,"title"    : getElementValue(c,"TITLE")
                         ,"courseid" : getElementValue(c,"COURSEID")
                         ,"UUID"     : getElementValue(c,"UUID")
                         }
                       )
    df = pd.DataFrame(courses)
    df = df.set_index("id")
    dfToSQL (df, 'course', con=conn)
    to_json(data,df,path.join(data['TEMP_FOLDER'],"course.json"))

    df        = None
    courses   = None
    courseXML = None
    xml       = None
#
###############################################################################################################################################################
#
def x_bb_user (data, conn, types):
    xml  = getFileForType("course/x-bb-user", data, types)
    users = []
    for userXML in xml.xpath('/USERS/USER'):
        user = {"ix"        : nextGI()
               ,"id"        : getAttr(userXML,'id')
               ,"username"  : getElementValue(userXML,'USERNAME')
               ,"studentid" : getElementValue(userXML,'STUDENTID')
               ,"email"     : getElementValue(userXML,'EMAILADDRESS')
               ,"GUID"      : getStudentGUID(conn,getElementValue(userXML,'USERNAME'))
               }
        for name in userXML.xpath('./NAMES'):
            user["nameGiven"]  = getElementValue (name,"GIVEN")
            user["nameFamily"] = getElementValue (name,"FAMILY")

        users.append (user)

    df = pd.DataFrame(users)
    df.dropna(subset=['GUID'], inplace=True)
    df = df.set_index("id")
    dfToSQL (df, 'users', con=conn)
    to_json(data,df,path.join(data['TEMP_FOLDER'],"users.json"))

    df       = None
    users    = None
    usersXML = None
    xml      = None
#
###############################################################################################################################################################
#
def x_bb_coursemembership (data, conn, types):
    xml  = getFileForType("membership/x-bb-coursemembership", data, types)
    membsXML = xml.xpath('/COURSEMEMBERSHIPS/COURSEMEMBERSHIP')
    membs = []
    for membXML in membsXML:
        membs.append ({"ix"          : nextGI()
                      ,"id"          : getAttr(membXML,'id')
                      ,"userid"      : getElementValue(membXML,'USERID')
                      ,"studentGUID" : getStudentGUIDfromID(conn,getElementValue(membXML,'USERID'))
                      ,"role"        : getElementValue(membXML,'ROLE')
                      }
                     )
    df = pd.DataFrame(membs)
    df = df.set_index("id")
    df.to_sql('coursememberships', con=conn)
    to_json(data,df,path.join(data['TEMP_FOLDER'],"coursememberships.json"))

    df       = None
    membs    = None
    membsXML = None
    xml      = None
#
###############################################################################################################################################################
#
def x_bb_rubrics (data, conn, types):
    xml               = getFileForType("course/x-bb-rubrics", data, types)
    rubricsXML        = xml.xpath('/LEARNRUBRICS/Rubric')

    rubrics           = []
    rubricRows        = []
    rubricColumns     = []
    rubricColumnCells = []

    for rubricXML in rubricsXML:
        rubric = {"ix"          : nextGI()
                 ,"id"          : getAttr(rubricXML,'id')
                 ,"title"       : getElementValue(rubricXML,'Title')
                 ,"description" : getElementValue(rubricXML,'Description')
                 ,"type"        : getElementValue(rubricXML,'Type')
                 ,"maxvalue"    : float(getElementValue(rubricXML,'MaxValue'))
                 }
        # worksheet name needs to be <= 31 chars in length. Otherwise excel gets unhappy
        rubric["workSheetName"] = applyAbbreviations(inStr     = rubric["title"]
                                                    ,targetLen = MAX_WORKSHEET_NAME_LENGTH
                                                    ,truncate  = True # Truncate the string as a last resort
                                                    )
        rubrics.append (rubric)
        for rrXML in rubricXML.xpath('./RubricRows/Row'):
            rr = {"ix"          : nextGI()
                 ,"id"          : getAttr(rrXML,'id')
                 ,"rubric"      : rubric["id"]
                 ,"seq"         : int(getElementValue(rrXML,'Position'))
                 ,"header"      : replaceBRwithLF(getElementValue(rrXML,'Header').replace('-:',""))
                 ,"percentage"  : float(getElementValue(rrXML,'Percentage'))
                 }
            rubricRows.append (rr)
            for rcXML in rrXML.xpath('./RubricColumns/Column'):
                rc = {"ix"          : nextGI()
                     ,"id"          : ""
                     ,"seq"         : int(getElementValue(rcXML,'Position'))
                     ,"rubricrow"   : rr["id"]
                     }
                rc["id"] = f'{rc["rubricrow"]}_{rc["seq"]}'
                rubricColumns.append (rc)
                for rccXML in rcXML.xpath('./Cell'):
                    rcc = {"ix"                     : nextGI()
                          ,"id"                     : getAttr(rccXML,'id')
                          ,"column"                 : rc["id"]
                          ,"description"            : getElementValue(rccXML,'CellDescription')
                          ,"numericpoints"          : float(getElementValue(rccXML,'NumericPoints'))
                          ,"numericstartpointrange" : float(getElementValue(rccXML,'NumericStartPointRange'))
                          ,"numericendpointrange"   : float(getElementValue(rccXML,'NumericEndPointRange'))
                          ,"percentage"             : float(getElementValue(rccXML,'Percentage'))
                          ,"percentagemin"          : float(getElementValue(rccXML,'Percentagemin'))
                          ,"percentagemax"          : float(getElementValue(rccXML,'PercentageMax'))
                          }
                    rubricColumnCells.append (rcc)
                    rcc = None
                rc = None
            rr = None
        rubric = None

    dframes = []
    def add_df (data, table):
        dframes.append ({"ix" : len(dframes), "table" :table, "df" : pd.DataFrame(data)})

    add_df (rubrics,"rubrics")
    add_df (rubricRows,"rubricrows")
    add_df (rubricColumns,"rubriccolumns")
    add_df (rubricColumnCells,"rubriccells")

    xml               = None
    rubricsXML        = None
    rubrics           = None
    rubricRows        = None
    rubricColumns     = None
    rubricColumnCells = None

    #for df in dframes:
        #df["df"].set_index("id")
       # dframes[df["ix"]] = df


    for df in dframes:
        dfToSQL (df["df"], df["table"], con=conn)
        to_json(data,df["df"],path.join(data['TEMP_FOLDER'],f'{df["table"]}.json'))
        dframes[df["ix"]] = df

    dframes           = None
#
###############################################################################################################################################################
#
def x_bb_crsrubricassocation (data, conn, types):
    xml               = getFileForType("course/x-bb-crsrubricassocation", data, types)
    rassocsXML        = xml.xpath('/COURSERUBRICASSOCIATIONS/ASSOCIATION')
    ra = []

    for raXML in rassocsXML:
        assoc = {"ix"                    : nextGI()
                ,"id"                    : getElementValue(raXML,'ASSOCIATION_ID')
                ,"rubric"                : getElementValue(raXML,'LEARNRUBRICID')
                ,"gradebookcol"          : getElementValue(raXML,'GRADEBOOKCOLID')
                ,"qtiasidataid"          : getElementValue(raXML,'QTIASIDATAID')
                ,"displaybeforegrading"  : strtobool(getElementValue(raXML,'DISPLAYBEFOREGRADING'))
                ,"displayaftergrading"   : strtobool(getElementValue(raXML,'DISPLAYAFTERGRADING'))
                ,"displaygraderubric"    : strtobool(getElementValue(raXML,'DISPLAYGRADEDRUBRIC'))
                ,"displaypercentofgrade" : getElementValue(raXML,'DISPLAYPERCENTOFGRADE')
                }
        if assoc["qtiasidataid"] == "{unset id}":
            assoc["qtiasidataid"] = None
        ra.append (assoc)
        assoc = None

    df = pd.DataFrame(ra)
    df = df.set_index("id")
    dfToSQL (df, 'rubricassocation', con=conn)
    to_json(data,df,path.join(data['TEMP_FOLDER'],"rubricassocation.json"))

    df         = None
    ra         = None
    rassocsXML = None
    xml        = None
#
###############################################################################################################################################################
#
def x_bb_crsrubriceval (data, conn, types):
    xml               = getFileForType("course/x-bb-crsrubriceval", data, types)
    revalsXML        = xml.xpath('/RUBRIC_EVALUATION_COLLECTION/RUBRIC_EVALUATION')
    revals = []
    revalcells = []

    for reXML in revalsXML:

        re = {"ix" : nextGI()
             ,"id" : getAttr(reXML,'id')
             }

        def getValue (ele):
            key = ele.replace('_','').lower()
            re[key] = getElementValue(reXML,ele)


        getValue("USED_FOR_GRADING")
        re["usedforgrading"] = strtobool(re["usedforgrading"])
        getValue("RUBRIC_ID")
        getValue("GRADEBOOK_GRADE_ID")
        getValue("ATTEMPT_ID")
        getValue("GROUP_ATTEMPT_ID")
        getValue("QTI_RESULT_DATA_ID")
        getValue("GRADEBOOK_LOG_ID")
        getValue("STAGED_ATTEMPT_ID")
        getValue("STAGED_GROUP_ATTEMPT_ID")
        re["rubricevalid"] = getElementID(reXML, "RUBRIC_EVAL")
        getValue("REVIEWER_USER_ID")
        getValue("RESPONDENT_USER_ID")

        for revalInner in reXML.xpath('./RUBRIC_EVAL'):
            def getValue2 (ele):
                key = ele.replace('_','').lower()
                re[key] = getElementValue(revalInner,ele)
            def getValue2Float (ele):
                key = ele.replace('_','').lower()
                re[key] = getElementValue(revalInner,ele)
                try:
                    numeric = float(re[key])
                    re[key] = numeric
                    numeric = None
                except ValueError:
                    pass
            def getValue2Bool (ele):
                key = ele.replace('_','').lower()
                re[key] = strtobool(getElementValue(revalInner,ele))
            getValue2("REVIEWER_USER_NAME")
            getValue2("RESPONDENT_USER_NAME")
            getValue2Bool("EDITABLE")
            getValue2Bool("PUBLISHED")
            getValue2Bool("COMPLETED")
            getValue2("SUBMISSION_DATE")
            getValue2Float("TOTAL_VALUE")
            getValue2Float("OVERRIDE_VALUE")
            getValue2Float("CALCULATED_PERCENT")
            getValue2Float("MAX_VALUE")
            for revalInner2 in revalInner.xpath('./RUBRIC_CELL_EVAL'):
                revalcell = {"ix"    : nextGI()
                            ,"re"    : re["id"]
                            ,"id"    : getAttr(revalInner2,'id')
                            ,"value" : getElementValue (revalInner2, '.')
                            }
                def getValue3 (ele):
                    key = ele.replace('_','').lower()
                    revalcell[key] = getElementValue(revalInner2,ele)
                def getValue3Float (ele):
                    key = ele.replace('_','').lower()
                    revalcell[key] = getElementValue(revalInner2,ele)
                    try:
                        numeric = float(revalcell[key])
                        revalcell[key] = numeric
                    except ValueError:
                        pass
                getValue3("RUBRIC_ROW_ID")
                getValue3("RUBRIC_CELL_ID")
                getValue3Float("SELECTED_PERCENT")
                revalcells.append (revalcell)
                revalcell = None

        revals.append (re)
        re = None


    df = pd.DataFrame(revals)
    df = df.set_index("id")
    df["overridevalue"] = pd.to_numeric(df["overridevalue"])
    dfToSQL (df, 'rubricevaluation', con=conn)
    to_json(data,df,path.join(data['TEMP_FOLDER'],"rubricevaluation.json"))

    df = pd.DataFrame(revalcells)
    df = df.set_index("id")
    dfToSQL (df, 'rubricevaluationcells', con=conn)
    to_json(data,df,path.join(data['TEMP_FOLDER'],"rubricevaluationcells.json"))

    df        = None
    revals    = None
    revalcells = None
    revalsXML = None
    xml       = None
#
###############################################################################################################################################################
#
def x_bb_gradebook (data, conn, types):
    xml               = getFileForType("course/x-bb-gradebook", data, types)

    # cats = []

    # for catXML in xml.xpath('/GRADEBOOK/CATEGORIES/CATEGORY'):
        # cat = {"ix" : nextGI()
              # ,"id" : getAttr(catXML,'id')
              # }

        # def getValue (ele):
            # key = ele.replace('_','').lower()
            # cat[key] = getElementValue(catXML,ele)
        # def getValueBool (ele):
            # key = ele.replace('_','').lower()
            # cat[key] = strtobool(getElementValue(catXML,ele))
        # getValue("TITLE")
        # cat["description"]    = getElementText(catXML,'DESCRIPTION')
        # getValueBool("ISUSERDEFINED")
        # getValueBool("ISCALCULATED")
        # getValueBool("ISSCORABLE")

        # cats.append (cat)
        # cat = None

    # scales       = []
    # scaleSymbols = []
    # for scaleXML in xml.xpath('/GRADEBOOK/SCALES/SCALE'):
        # scale = {"ix" : nextGI()
                # ,"id" : getAttr(scaleXML,'id')
                # }
        # def getValue (ele):
            # key = ele.replace('_','').lower()
            # scale[key] = getElementValue(scaleXML,ele)
        # def getValueBool (ele):
            # key = ele.replace('_','').lower()
            # scale[key] = strtobool(getElementValue(scaleXML,ele))

        # getValue("TITLE")
        # getValueBool("ISUSERDEFINED")
        # getValueBool("ISTABULARSCALE")
        # getValueBool("ISPERCENTAGE")
        # getValueBool("ISNUMERIC")
        # getValue("TYPE")
        # getValue("VERSION")
        # scale["scaleSymbolCnt"] = 0

        # for ssXML in scaleXML.xpath('./SYMBOLS/SYMBOL'):
            # scaleSymbol = {"id" : getAttr(ssXML,'id')
                          # ,"scaleid" : scale["id"]
                          # }
            # scale["scaleSymbolCnt"] += 1
            # def getValue2 (ele):
                # key = ele.replace('_','').lower()
                # scaleSymbol[key] = getElementValue(ssXML,ele)
            # getValue("TITLE")
            # #getValue("LOWERBOUND")
            # #getValue("UPPERBOUND")
            # #getValue("ABSOLUTETRANSLATION")

            # scaleSymbols.append (scaleSymbol)
            # scaleSymbol = None

        # scales.append(scale)
        # scale = None

    # outcomeDefns = []
    # for defnXML in xml.xpath('/GRADEBOOK/OUTCOMEDEFINITIONS/OUTCOMEDEFINITION'):
        # outcomeDefn = {"ix" : nextGI()
                      # ,"id" : getAttr(defnXML,'id')
                      # }
        # def getValue (ele):
            # key = ele.replace('_','').lower()
            # outcomeDefn[key] = getElementValue(defnXML,ele)
        # def getValueBool (ele):
            # key = ele.replace('_','').lower()
            # outcomeDefn[key] = strtobool(getElementValue(defnXML,ele))
        # getValue ("CATEGORYID")
        # getValue ("SCALEID")
        # getValue ("SECONDARY_SCALEID")
        # getValue ("CONTENTID")
        # getValue ("GRADING_PERIODID")
        # getValue ("ASIDATAID")
        # getValue ("TITLE")
        # getValue ("DISPLAY_TITLE")
        # getValue ("POSITION")
        # getValue ("VERSION")
        # getValue ("DELETED")
        # getValue ("EXTERNALREF")
        # getValue ("HANDLERURL")
        # getValue ("ANALYSISURL")
        # getValue ("WEIGHT")
        # getValue ("POINTSPOSSIBLE")
        # getValueBool ("ISVISIBLE")
        # getValueBool ("VISIBLE_BOOK")
        # getValueBool ("VISIBLE_ALL_TERMS")
        # getValueBool ("SHOW_STATS_TO_STUDENT")
        # getValueBool ("HIDEATTEMPT")
        # getValue ("AGGREGATIONMODEL")
        # getValue ("SCORE_PROVIDER_HANDLE")
        # getValueBool ("SINGLE_ATTEMPT")
        # getValue ("CALCULATIONTYPE")
        # getValueBool ("ISCALCULATED")
        # getValueBool ("ISSCORABLE")
        # getValueBool ("ISUSERCREATED")
        # getValue ("MULTIPLEATTEMPTS")
        # getValue ("IS_DELEGATED_GRADING")
        # getValue ("IS_ANONYMOUS_GRADING")
      # #      <DATES>
       # #     <CREATED value=""/>
        # #    <UPDATED value="2019-11-25 18:06:44 AWST"/>
         # #   <DUE value=""/>
          # #  <ANON_GRADING_REL_DATE value=""/>
          # #</DATES>
          # #<DESCRIPTION>
    # #        <TEXT>&lt;p&gt;The unweighted sum of all grades for a user.&lt;/p&gt;</TEXT>
    # #        <TYPE value="H"/>
     # #     </DESCRIPTION>
    # #
     # #     <ACTIVITY_COUNT_COL_DEFS/>
      # #    <IS_DELEGATED_GRADING value="false"/>
       # #   <IS_ANONYMOUS_GRADING value="false"/>
        # #  <GROUPATTEMPTS/>
         # # <OUTCOMES/>

        # outcomeDefns.append(outcomeDefn)
        # outcomeDefn = None


    gradeHistEntries = []
    for gheXML in xml.xpath('/GRADEBOOK/GRADE_HISTORY_ENTRIES/GRADE_HISTORY_ENTRY'):
        gradeHistEntry = {"ix" : nextGI()
                         ,"id" : getAttr(gheXML,'id')
                         }
        def getValue (ele,raiseErr = False):
            key = ele.replace('_','').lower()
            if raiseErr:
                val = getElementValue(gheXML,ele)
            else:
                try:
                    val = getElementValue(gheXML,ele)
                except IndexError:
                    val = ""
            gradeHistEntry[key] = val
        def getValueBool (ele):
            key = ele.replace('_','').lower()
            gradeHistEntry[key] = strtobool(getElementValue(gheXML,ele))
        getValue ("USERID")
        gradeHistEntry["studentGUID"] = getStudentGUIDfromID(conn,gradeHistEntry["userid"])
        #gradeHistEntry = removekey(gradeHistEntry,"userid")
        getValue ("GRADABLE_ITEM_ID")
        getValueBool ("DELETE")
        getValueBool ("GRADED_ANONYMOUSLY")
        getValue ("ANONYMIZING_ID")
        gradeHistEntry["numericgrade"]    = getElementText(gheXML,'NUMERIC_GRADE')
        gradeHistEntry = checkDictKeyValueNumeric  (gradeHistEntry,'numericgrade')
        gradeHistEntry["hasNumericGrade"] = ("numericgrade" in gradeHistEntry)
        try:
            getValue ("GRADE",raiseErr=True)
            if ("grade" not in gradeHistEntry
             or len (gradeHistEntry["grade"]) == 0
             or gradeHistEntry["grade"] == '-'
               ):
                gradeHistEntry = removekey(gradeHistEntry,"grade")
                gradeHistEntry["hasGrade"] = False
            else:
                gradeHistEntry["hasGrade"] = True
        except IndexError as e:
            gradeHistEntry["grade"]    = None
            gradeHistEntry["hasGrade"] = False

        getValue ("ATTEMPT_ID")
        getValue ("DATEATTEMPTED")
        getValue ("DATE_LOGGED")
        getValue ("MODIFIER_ADDRESS")
        getValue ("MODIFIER_USERNAME")
        getValue ("MODIFIER_ID")
        getValue ("MODIFIER_FIRSTNAME")
        getValue ("MODIFIER_LASTNAME")
        getValue ("MODIFIER_USERNAME")
        getValue ("MODIFIER_ROLE")

        useThisOne = ((gradeHistEntry["hasGrade"] > 0)
                 and  ("attemptid" in gradeHistEntry and len(gradeHistEntry["attemptid"]) > 0)
                 and  ("delete" not in gradeHistEntry or gradeHistEntry["delete"] == 0)
                     )
        if useThisOne:
            gradeHistEntries.append (gradeHistEntry)
        useThisOne = None
        gradeHistEntry = None

    xml = None


    dframes = []
    def add_df (data, table):
        dframes.append ({"ix" : len(dframes), "table" :table, "df" : pd.DataFrame(data)})

    # add_df (cats,"categories")
    # add_df (scales,"scales")
    # add_df (scaleSymbols,"scalesymbols")
    # add_df (outcomeDefns,"outcomedefinitions")
    add_df (gradeHistEntries,"gradehistoryentries")

    cats             = None
    scales           = None
    scaleSymbols     = None
    outcomeDefns     = None
    gradeHistEntries = None

    for df in dframes:
        #df["df"].set_index("id")
        dfToSQL (df["df"], df["table"], con=conn)
        to_json(data,df["df"],path.join(data['TEMP_FOLDER'],f"{df['table']}.json"))
        dframes[df["ix"]] = df

    dframes = None
#
###############################################################################################################################################################
#
def processZipFile(data, conn):

    types = imsmanifest (data, conn)

    ### create batch metadata table
    data = createBatchMetadata (data, conn, types)

    ## Read the gradeCentre file
    gradeCentre (data, conn)


    ### course/x-bb-coursesetting
    x_bb_coursesetting (data, conn, types)

    ### course/x-bb-user
    x_bb_user (data, conn, types)


    # ### membership/x-bb-coursemembership
    x_bb_coursemembership (data, conn, types)


    # ### course/x-bb-rubrics
    x_bb_rubrics (data, conn, types)


    # ### course/x-bb-crsrubricassocation
    x_bb_crsrubricassocation (data, conn, types)


    # ### course/x-bb-crsrubriceval
    x_bb_crsrubriceval (data, conn, types)

    # ### course/x-bb-gradebook
    x_bb_gradebook (data, conn, types)
#
    return data
#
###############################################################################################################################################################
#
def applyAbbreviations (inStr, targetLen = -1, truncate = True):

    # targetLen of Zero means that we'll just shorten as much as possible (but truncating makes no sense)
    lastResortTruncate = (truncate and not (targetLen <= 0))

    def removeDoubleSpaces (str_):
        return " ".join([word for word in str_.split(" ") if len(word) > 0])

    # Take out dodgy characters that excel doesnt like (replace with hyphen)
    from   re import split as reSplit
    outStr = "-".join(reSplit("[/\\*'?\[\]:]+", inStr))

    abbrevs = (("-"," ") # Always first
                # some standard ones
              ,("Marking Rubric","Rubric")
              ,("Semester","Sem")
              ,("and","&")
              ,("Part","Pt")
              ,("Rubric","")
               # Year abbrevs
              ,("2019","19")
              ,("2020","20")
              ,("2021","21")
              ,("2022","22")
              ,("2023","23")
              ,("2024","24")
              ,("2025","25")
               # A bit more specific
              ,("Accounting","Acc")
              ,("Information","Info")
              ,("Systems","Sys")
              ,("Microsoft","MS")
              ,("Scenario","Scen")
              ,("Analysis","Anlys")
              ,("Decision","Dec")
              ,("Support","Sup")
              ,("Problem","Prob")
              )

    # print (f"outStr = \'{outStr}\'")
    outStr  = removeDoubleSpaces(outStr)

    if len(outStr) > targetLen:
        # If there's a - in there, make it more significant
        outStr = "-".join([word for word in outStr.split('-')[::-1]])

        for abbr in abbrevs:
            outStr = outStr.replace(abbr[0],abbr[1])
            outStr = outStr.replace(abbr[0].upper(),abbr[1].upper())
            outStr = outStr.replace(abbr[0].lower(),abbr[1].lower())
            outStr = outStr.replace(abbr[0].title(),abbr[1].title())
            outStr = removeDoubleSpaces(outStr)
            if len(outStr) <= targetLen: # stop once we're short enough
                break

        if len(outStr) > targetLen:
            # Shorten words
            words = outStr.split(' ')
            for ix,w in enumerate(words):
                if len(w) > 5: # Words > 5 chars, cut to first 3
                    words[ix] = w[:3]
                    #print (f"\'{w}\' -> \'{words[ix]}\'")
                    outStr = " ".join(words)
                    if len(outStr) <= targetLen:
                        break
            words  = None

        if len(outStr) > targetLen:
            outStr = outStr.replace(" ","") # Take spaces out

        if len(outStr) > targetLen and lastResortTruncate:
            outStr = outStr[:targetLen]

    return outStr
#
###############################################################################################################################################################
#
def createViewsAndOutput (data, conn):
    outputExcel = []


    def checkSheetName (sheetname):
        return applyAbbreviations(sheetname,MAX_WORKSHEET_NAME_LENGTH)

    def ExcelSheet(sheetName, viewName,colFreeze = 1, rowFreeze = 1, transposeDF = False):
        sheetName_ = checkSheetName(sheetName)
        sht = {"sheet"    : sheetName_
              ,"viewName" : viewName
              ,"df"       : tableToDF (conn,viewName)
              ,"freeze"   : (rowFreeze,colFreeze) # (0,0) is nonsense, but we catch it when we write it out
              ,"header"   : not transposeDF
              ,"index"    : transposeDF
              }
        if transposeDF:
            sht["df"] = sht["df"].transpose()
        return sht


    sqlStr = """
    CREATE VIEW v_studentdetails
          ("UnitCode"
          ,"StudentID"
          ,"Course"
          ,"AttemptStatus"
          ,"Location"
          ,"Availability"
          )
    AS
    SELECT m."Unit Code"
          ,g.StudentID
          ,SUBSTR(g.course,1,LENGTH(g.course))
          ,g.unitattemptstatus
          ,g.location
          ,CASE g.availability WHEN 1 THEN 'Yes' ELSE 'No' END
     FROM  metadata    m
          ,gradecentre g
    ORDER BY g.unitattemptstatus DESC, g.studentid"""

    cur = conn.cursor()
    cur.execute(sqlStr)
    cur.close()

    sqlStr = """
    CREATE VIEW v_rubricsWithData AS
    SELECT v.*
     FROM (SELECT r.ix
                 ,r."index"
                 ,r.id
                 ,r.title
                 ,r.description
                 ,r.type
                 ,r.maxvalue
                 ,(SELECT COUNT(*)
                    FROM  rubricassocation    ra
                         ,gradehistoryentries ghe
                   WHERE  ra.rubric = r.id
                    AND   ghe.gradableitemid = ra.gradebookcol
                  ) numResults
                 ,r."workSheetName"
            FROM  rubrics r
           ) v
    WHERE numResults > 0"""

    cur = conn.cursor()
    cur.execute(sqlStr)
    cur.close()

    sqlStr = """
    CREATE VIEW v_LatestRubricResults AS
    SELECT r.id, r.type
          ,ghe.userid
          ,u.studentid
          ,u.nameFamily||', '||u.nameGiven||' ('||u.username||')' user_
          ,ghe.datelogged
          ,rec.rubricrowid
          ,rec.rubriccellid
          ,rr.header
          ,CASE r.type
            WHEN 'NUMERIC'          THEN rec.selectedpercent
            WHEN 'PERCENTAGE_RANGE' THEN rec.selectedpercent*100
            WHEN 'NUMERIC_RANGE'    THEN round(re.maxvalue * rec.selectedpercent,2)
            ELSE Null
           END rawScore
          ,CASE r.type
            WHEN 'PERCENTAGE_RANGE' THEN ((rec.selectedpercent*100)/rr.percentage)*100
            ELSE Null
           END weightedPct
          ,ghe.modifierLastName||', '||ghe.modifierFirstName||' ('||ghe.modifierUsername||')' grader
     FROM  rubrics               r
          ,rubricrows            rr
          ,rubricevaluation      re
          ,gradehistoryentries   ghe
          ,users                 u
          ,rubricevaluationcells rec
    WHERE  rr.rubric         = r.id
     AND   re.rubricid       = r.id
     AND   rr.id             = rec.rubricrowid
     AND   rec.re            = re.id
     AND   re.gradebooklogid = ghe.id
     AND   ghe.userid        = u.id
     AND   ghe.datelogged    = (SELECT MAX(ghex.datelogged)
                                 FROM  gradehistoryentries ghex
                                      ,rubricevaluation    rex
                                      ,rubricrows          rrx
                                WHERE  ghex.userid  = u.id
                                 AND   ghex.id      = rex.gradebooklogid
                                 AND   rex.rubricid = re.rubricid
                                 AND   rrx.id       = rr.id
                               )"""

    cur = conn.cursor()
    cur.execute(sqlStr)
    cur.close()

    def createRubricView (rubric,type_):
        cur = conn.cursor()
        cur.execute(f"SELECT rr.seq, rr.header, rr.percentage, rr.id FROM rubricrows rr WHERE rr.rubric = \'{rubric}\' ORDER BY rr.seq")
        rowset = cur.fetchall()
        cur.close()
        rubricView = {"rubric" : rubric
                     ,"view"   : f"v_rubric{rubric}"
                     ,"sql"    : ""
                     ,"rows"   : []
                     }
        if not (rowset is None or len(rowset) == 0):
            rubricView["sql"] += f"CREATE VIEW {rubricView['view']} AS"
            rubricView["sql"] += f"\nWITH res AS (SELECT studentid, rawScore, weightedPct, rubricrowid, id, datelogged, grader FROM v_LatestRubricResults l WHERE l.id = \'{rubric}\')"
            rubricView["sql"] += "\nSELECT s.*"
            rubricView["sql"] += "\n      ,(SELECT MAX(datelogged) FROM res r WHERE r.studentid = s.studentid) \"Date Logged\""
            rubricView["sql"] += "\n      ,(SELECT MAX(grader)     FROM res r WHERE r.studentid = s.studentid) \"Grader\""
            for row in rowset:
                rubricRow = {"seq"        : row[0]
                            ,"header"     : row[1]
                            ,"percentage" : row[2]
                            ,"id"         : row[3]
                            }
                rubricView["sql"] += f"\n      ,(SELECT r.rawScore    FROM res r WHERE r.studentid = s.studentid AND r.rubricrowid  =  \'{rubricRow['id']}\') \"{rubricRow['header']}"
                if type_ == 'PERCENTAGE_RANGE':
                    rubricView["sql"] +=  " Raw %"
                rubricView["sql"] +=  "\""

                if type_ == 'PERCENTAGE_RANGE':
                    rubricView["sql"] += f"\n      ,(SELECT r.weightedPct FROM res r WHERE r.studentid = s.studentid AND r.rubricrowid  =  \'{rubricRow['id']}\') \"{rubricRow['header']} Weighted %\""
                rubricView["rows"].append(rubricRow)
            rubricView["sql"] += "\n FROM v_studentdetails s"

            cur = conn.cursor()
            # print (rubricView["sql"])
            cur.execute(rubricView["sql"])
            cur.close()
            cur = None
        return rubricView

    def createRubricViews():

        cur = conn.cursor()
        cur.execute('SELECT r.id, r.title, r."workSheetName", r.type FROM v_rubricsWithData r')
        rowset = cur.fetchall()
        cur.close()
        views = []
        if not (rowset is None or len(rowset) == 0):
            for row in rowset:
                thisview                  = createRubricView (row[0],row[3])
                thisview["title"]         = row[1]
                thisview["workSheetName"] = row[2]
                thisview["type"]          = row[3]
                views.append (thisview)
            for v in views:
                outputExcel.append (ExcelSheet(v["workSheetName"],v["view"],colFreeze=2))

    # #### Create our output Excel list
    outputExcel.append (ExcelSheet("Rubrics With Data","v_rubricsWithData",colFreeze=0))
    # outputExcel.append (ExcelSheet("Student Details","v_studentdetails",colFreeze=0))
    createRubricViews()
    outputExcel.append (ExcelSheet("Metadata","metadata",colFreeze=1,rowFreeze=0,transposeDF=True))

    # ### Write excel file
    # >excel_writer : str or ExcelWriter object
    # >File path or existing ExcelWriter.
    # >
    # >sheet_name : str, default ‘Sheet1’
    # >Name of sheet which will contain DataFrame.
    # >
    # >na_rep : str, default ‘’
    # >Missing data representation.
    # >
    # >float_format : str, optional
    # >Format string for floating point numbers. For example float_format="%.2f" will format 0.1234 to 0.12.
    # >
    # >columns : sequence or list of str, optional
    # >Columns to write.
    # >
    # >header : bool or list of str, default True
    # >Write out the column names. If a list of string is given it is assumed to be aliases for the column names.
    # >
    # >index : bool, default True
    # >Write row names (index).
    # >
    # >index_label : str or sequence, optional
    # >Column label for index column(s) if desired. If not specified, and header and index are True, then the index names are >used. A sequence should be given if the DataFrame uses MultiIndex.
    # >
    # >startrow : int, default 0
    # >Upper left cell row to dump data frame.
    # >
    # >startcol : int, default 0
    # >Upper left cell column to dump data frame.
    # >
    # >engine : str, optional
    # >Write engine to use, ‘openpyxl’ or ‘xlsxwriter’. You can also set this via the options io.excel.xlsx.writer, >io.excel.xls.writer, and io.excel.xlsm.writer.
    # >
    # >merge_cells : bool, default True
    # >Write MultiIndex and Hierarchical Rows as merged cells.
    # >
    # >encoding : str, optional
    # >Encoding of the resulting excel file. Only necessary for xlwt, other writers support unicode natively.
    # >
    # >inf_rep : str, default ‘inf’
    # >Representation for infinity (there is no native representation for infinity in Excel).
    # >
    # >verbose : bool, default True
    # >Display more information in the error logs.
    # >
    # >freeze_panes : tuple of int (length 2), optional
    # >Specifies the one-based bottommost row and rightmost column that is to be frozen.
    # >
    # >New in version 0.20.0..

    excelOutputArchive = path.join(data["TEMP_FOLDER"],'excel.xlsx')
    with pd.ExcelWriter(excelOutputArchive) as writer:
        for ix,o in enumerate(outputExcel):
            if o["freeze"] == (0,0):
                o["freeze"] = None
            o["df"].to_excel (excel_writer = writer
                             ,sheet_name   = o["sheet"]
                             ,float_format = "%.3f"
                             ,header       = o["header"]
                             ,index        = o["index"]
                             ,freeze_panes = o["freeze"]
                             )
            outputExcel[ix] = o
    # copy the one from the temp folder to the real folder
    from shutil import copy
    fname = copy (excelOutputArchive,data['EXCEL_OUTPUT'])
#
###############################################################################################################################################################
#
def doAllProcessing (data):

    # print (data)

    # ### Create the folder for the outputs if it doesn't exist already
    # #### Delete any files that are in there otherwise

    if 'TEMP_FOLDER' not in data or data['TEMP_FOLDER'] is None or len(data['TEMP_FOLDER']) == 0:
        tempParent = getenv('TEMP')
        if tempParent is None or len(tempParent) == 0 or not path.exists(tempParent):
            tempParent = data['OUTPUT_FOLDER']
        from   uuid           import uuid4
        data['TEMP_FOLDER'] = path.join(tempParent,str(uuid4()))

    createOrClearFolder (data['TEMP_FOLDER'])

    # ### Create the DB Connection
    conn = createDBConnection(data,reloadFromFile=False)

    data = processZipFile (data, conn)

    createViewsAndOutput (data, conn)

    #
    # Do our finishing up
    #

    # ### Write inMemory DB to Disk
    backupDBtoFile(data, conn)
    # ### Close connection to inMemory DB
    conn.close()

    data = createZipArchive(data, deleteAfterwards = True)

    return data
#
#############################################s##################################################################################################################
#
def process (zipfile, gradecentreCSV, outputFolder, writeXML = True, writeJSON = True, preserveDB = True):

    ddict = {"ZIPFILE"         : zipfile
            ,"GRADECENTRE_CSV" : gradecentreCSV
            ,"OUTPUT_FOLDER"   : outputFolder
            ,"WRITE_DATA_XML"  : writeXML
            ,"WRITE_DATA_JSON" : writeJSON
            ,"KEEP_SQLLITE"    : preserveDB
            }

    ddict = doAllProcessing(data = ddict)
    # print (ddict)
    dictMap = [('OUTPUT_FOLDER','Folder',False)
              ,('EXCEL_OUTPUT','Excel',True)
              ,('SQLLITE_OUTPUT','Database',True)
              ,('processedArchive','Archive',True)
              ]

    rdict = {}
    for quay in dictMap:
        if quay[0] in ddict:
            dictVal = ddict[quay[0]]
            if quay[2] and dictVal is not None and len(dictVal) > 0:
                from pathlib import Path
                dictVal = str(Path(dictVal).relative_to(outputFolder))
            rdict[quay[1]] = dictVal
        else:
            rdict[quay[1]] = None
    # print (rdict)
    return rdict
    # dlist = [{"key" : k, "value" : v} for k, v in ddict.items()]
    # print ((dlist))
    # # rdict = {'EXCEL_OUTPUT' : ddict['EXCEL_OUTPUT']
            # # ,
            # # }

# if __name__ == '__main__':
    # data = {"ZIPFILE"            : 'ArchiveFile_ACC3201.2019.2.ONCAMPUS_OFFCAMPUS_20191126085012.zip'
           # ,"GRADECENTRE_CSV"    : "gc_ACC3201_2019_2_ONCAMPUS_OFFCA.csv"
           # ,"WRITE_DATA_XML"     : True
           # ,"WRITE_DATA_JSON"    : True
           # }
    # data["OUTPUT_FOLDER"]   = f"."
    # data["TEMP_FOLDER"]     = f"{path.splitext(data['ZIPFILE'])[0]}"
    # data = doAllProcessing (data)
    # print (data)