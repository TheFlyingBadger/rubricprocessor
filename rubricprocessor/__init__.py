# RubricProcessor - a python library for processing Blackboard
#                   rubric archives
# @author: Jonathan Mills <jon@badger.shoes>
#


import argparse, os
from . import core, gui, batch

__all__ = ['runFromCLI','guiLaunch','processSingle','processBatch']


def isNotNull (arg):
    return not isNull (arg)

def isNull (arg):
    return (arg is None or len(arg) == 0)

def isWritable(path):
    # import os
    # return os.access(path, os.W_OK)

    writable = True

    import errno
    try:
        #
        # this should be done with tempfile, but there's an ancient python bug on windows
        #   https://bugs.python.org/issue22107
        #
        fname    = os.path.join(path,os.urandom(32).hex())
        with open(fname,'w') as f:
            f.write(fname)
        os.remove (fname)
    except OSError as e:
        if e.errno == errno.EACCES:  # 13
            writable   = False
        else:
            e.filename = path
            raise
    return writable

def touch(fname, mode=0o666, dir_fd=None, **kwargs):
    flags = os.O_CREAT | os.O_APPEND
    with os.fdopen(os.open(fname, flags=flags, mode=mode, dir_fd=dir_fd)) as f:
        os.utime(f.fileno() if os.utime in os.supports_fd else fname, dir_fd=None if os.supports_fd else dir_fd, **kwargs)

def _setTheCommonArgs (args, outputFolder, writeXML = True, writeJSON = True, preserveDB = True):
    args.output      = outputFolder
    args.writeNoXML  = not (writeXML)
    args.writeNoJSON = not (writeJSON)
    args.keepDB      = preserveDB
    return processArgs(args)

def processSingle (zipfile, gradecentreCSV, outputFolder, writeXML = True, writeJSON = True, preserveDB = True):
    args = argparse.Namespace()
    args.mode        = 'cli'
    args.rubric      = zipfile
    args.gc          = gradecentreCSV
    return _setTheCommonArgs(args, outputFolder,writeXML,writeJSON,preserveDB)

def processBatch (control, outputFolder, writeXML = True, writeJSON = True, preserveDB = True):
    args = argparse.Namespace()
    args.mode        = 'batch'
    args.control     = control
    return _setTheCommonArgs(args, outputFolder,writeXML,writeJSON,preserveDB)

def guiLaunch (zipfile = None, gradecentreCSV = None, outputFolder = None, writeXML = True, writeJSON = True, preserveDB = True):
    args = argparse.Namespace()
    args.mode        = 'gui'
    args.rubric      = zipfile
    args.gc          = gradecentreCSV
    return _setTheCommonArgs(args, outputFolder,writeXML,writeJSON,preserveDB)

def processArgs (args):
    vArgs = vars(args)

    # "NVL" args
    if isNull(args.mode):
        args.mode = "gui"
    if ("rubric" not in vArgs):
        args.rubric = None
    if ("gc" not in vArgs):
        args.gc = None
    if ("output" not in vArgs):
        args.output = None
    if ("writeNoXML" not in vArgs):
        args.writeNoXML = False
    if ("writeNoJSON" not in vArgs):
        args.writeNoJSON = False
    if ("keepDB" not in vArgs):
        args.keepDB = True

    for f in ["control","rubric","gc"]: # check the files exist (if arguments specified)
        if  (f in vArgs
         and isNotNull(vArgs[f])
            ):
            if not os.path.isfile(vArgs[f]):
                raise FileNotFoundError (f"{f} - file \'{vArgs[f]}\' not found")
            elif f == "rubric":
                core.imsManifestCheck (vArgs[f])
            elif f == "gc":
                core.gradeCentreCheck (vArgs[f])

    if isNotNull(args.output): # output is specified
        if not os.path.exists(args.output):
            # Check the folder exists
            raise NotADirectoryError(f"output - folder \'{args.output}\' not found")
        elif os.path.isfile(args.output):
            # Check it's not a file
            raise NotADirectoryError (f"output - \'{args.output}\' is a file, not a folder")
        elif not (isWritable(args.output)):
            raise PermissionError (f"output - folder \'{args.output}\' not writable")
    #

    def printIfThere (dict,label, key=""):
        def wrapVar (var):
            retval = var
            if type(retval) == str and var.find(' ') >= 0:
                retval = f"\"{var}\""
            return retval
        if len(key) == 0:
            key = label
        if key in dict:
            print (f"{label.ljust(15)} : {wrapVar(dict[key])}")

    sep = '-'*80

    if args.mode == "gui":
        # print (args)
        gui.launch (zipfile        = args.rubric
                   ,gradecentreCSV = args.gc
                   ,outputFolder   = args.output
                   ,writeXML       = not (args.writeNoXML)
                   ,writeJSON      = not (args.writeNoJSON)
                   ,preserveDB     = args.keepDB
                   )
        rdict = {}
    elif args.mode == "cli":
        rdict = core.process (zipfile        = args.rubric
                             ,gradecentreCSV = args.gc
                             ,outputFolder   = args.output
                             ,writeXML       = not (args.writeNoXML)
                             ,writeJSON      = not (args.writeNoJSON)
                             ,preserveDB     = args.keepDB
                             )
    else: # batch
        rdict = batch.process (control        = args.control
                              ,outputFolder   = args.output
                              ,writeXML       = not (args.writeNoXML)
                              ,writeJSON      = not (args.writeNoJSON)
                              ,preserveDB     = args.keepDB
                              )
    print (sep)
    print ("Processing complete :- ")
    printIfThere(rdict,"Folder")
    printIfThere(rdict,"Excel")
    printIfThere(rdict,"Database")
    printIfThere(rdict,"Archive")
    printIfThere(rdict,"timeBegin")
    printIfThere(rdict,"timeEnd")
    printIfThere(rdict,"rowsControl")
    printIfThere(rdict,"rowsProcessed")
    printIfThere(rdict,"rowsError")
    print (sep)

    return rdict

def runFromCLI():

    parser = argparse.ArgumentParser (description = 'Process Rubric extracts'
                                     ,epilog      = 'call with mode for additional help'
                                     ,add_help    = True
                                     )

    subparsers = parser.add_subparsers(dest='mode')
    parser.add_argument ('--version','-v', action='version', version='%(prog)s 1.0')
    subparser  = {}
    subparser["C"] = subparsers.add_parser('cli', help='process single export from command line')
    subparser["G"] = subparsers.add_parser('gui', help='Launch GUI')
    subparser["B"] = subparsers.add_parser('batch', help='Process several exports from a control CSV file')
    #
    for typ in ["C","G","B"]:
        if typ == "B":
            subparser[typ].add_argument('--control',type=str, metavar='fullpathControl', help="File/Path batch control CSV file",required=True)
        else:
            subparser[typ].add_argument('--rubric',type=str, metavar='fullpathZip', help="File/Path of the Rubric zip file",required=(typ == "C"))
            subparser[typ].add_argument('--gc',type=str, metavar='fullpathCSV', help="File/Path of the Gradecentre CSV file",required=(typ == "C"))
        subparser[typ].add_argument('--output', '-o', type=str, metavar='folderPath', help="Output Folder", required=(not typ == "G"))
        subparser[typ].add_argument('--writeNoXML', help="write XML from Rubric to output", action="store_false", default = True)
        subparser[typ].add_argument('--writeNoJSON', help="write JSON from Rubric to output", action="store_false", default = True)
        subparser[typ].add_argument('--keepDB', help="retain SqlLite DB", action="store_true", default=False)
    try:
        processArgs(parser.parse_args())
    except Exception as e:
        parser.error (str(e))
    #

if __name__ == '__main__':
    runFromCLI()