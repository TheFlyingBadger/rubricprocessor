#!/usr/bin/python3
import datetime     \
      ,os           \
      ,json         \
      ,qdarkstyle   \
      ,sys          \
      ,traceback

from . import core, icons
import rubricprocessor

from PyQt5.QtWidgets import QApplication     \
                           ,QDesktopWidget   \
                           ,QFileDialog      \
                           ,QFrame           \
                           ,QLabel           \
                           ,QMessageBox      \
                           ,QPlainTextEdit   \
                           ,QPushButton      \
                           ,QTextEdit        \
                           ,QWidget
from PyQt5.QtGui     import QCursor
from PyQt5.QtCore    import QSize            \
                           ,Qt

winTitleDefault = 'Rubric Processor'

iconDict = {}

def getIconDict (usage,key):
   if usage not in iconDict:
      raise KeyError (f"\'{usage}\' not specified")
   elif key not in iconDict[usage]:
      raise KeyError (f"\'{key}\' not valid for \'{usage}\'")
   return iconDict[usage][key]

def getIconIcon (usage):
   return getIconDict(usage,"icon")

def getIconPixmap (usage):
   return getIconDict(usage,"pixmap")



def getShortenedDirForDisplay (inDir):
    if inDir is None:
        outDir = ""
    else:
        # print (f"len(inDir)  = {len(inDir)}")
        maxLen = 75
        outDir = inDir.replace('/','\\')
        try:
            if len(outDir) > maxLen:
                words = outDir.split('\\')
                midPoint = int((len(words)/2))
                while len(outDir) > maxLen:
                    # print (f"........")
                    # print (f"midPoint    = {midPoint}")
                    # print (f"len(outDir) = {len(outDir)}")
                    # print (words)

                    words[midPoint] = "..."

                    outDir = '\\'.join(words)
                    if len(outDir) > maxLen:
                        del words[(midPoint-1)]
                        print (words)
                        midPoint -= 1
                    if len(outDir) > maxLen:
                        del words[(midPoint+1)]
                        print (words)
        except:
            # print ("yeah, that didn't work")
            pass
    # print (f"len(outDir) = {len(outDir)}")
    return outDir

cssFilename  = f"{os.path.splitext(os.path.basename(__file__))[0]}.css"
jsonFilename = f"{os.path.splitext(os.path.basename(__file__))[0]}.json"

class MyMessageBox(QMessageBox):

    # This is a much better way to extend __init__
    def __init__(self, *args, **kwargs):
        super(MyMessageBox, self).__init__(*args, **kwargs)
        # Anything else you want goes below

    # We only need to extend resizeEvent, not every event.
    def resizeEvent(self, event):

        # print ('resizeEvent')
        result = super(MyMessageBox, self).resizeEvent(event)

        details_box = self.findChild(QTextEdit)
        # 'is not' is better style than '!=' for None
        if details_box is not None:
            lines = self.detailedText().split('\n')
            maxLen = 0
            for l in lines:
               if len(l) > maxLen:
                  maxLen = len(l)
            calcWidth = int((maxLen*9.5)+0.5)
            if calcWidth > 1920:
               calcWidth = 1920
            size = details_box.sizeHint()
            size.setWidth (calcWidth)
            details_box.setFixedSize(size)

        return result

class App(QWidget):

    def checkGUIConfig(self):

        defaults = self.initGUIConfig()

        # always set these every check
        self.config["widthScreen"]  = defaults["widthScreen"]
        self.config["heightScreen"] = defaults["heightScreen"]

        if "width" not in self.config or self.config["width"] < defaults["width"]:
            self.config["width"]  = defaults["width"]
        if "height" not in self.config or self.config["height"] < defaults["height"]:
            self.config["height"] = defaults["height"]
        if "left"  not in self.config:
            self.config["left"]   = defaults["left"]
        elif ( self.config["left"]  < 0
           or ((self.config["left"] + self.config["width"]) > self.config["widthScreen"])
             ):
            self.config["left"] = int((self.config["widthScreen"]  - self.config["width"])/2)
        if "top" not in self.config:
            self.config["top"]   = defaults["top"]
        elif ( self.config["top"]  < 0
           or ((self.config["top"] + self.config["height"]) > self.config["heightScreen"])
             ):
            self.config["top"]    = int((self.config["heightScreen"] - self.config["height"])/2)

        def checkDir (key):
            if (key not in self.config
             or  len(self.config[key]) == 0
             or (len(self.config[key]) > 0 and not os.path.exists(self.config[key]))
               ):
                self.config[key] = defaults[key]

        checkDir ("zipDir")
        checkDir ("csvDir")
        checkDir ("outDir")

        if "winTitle" not in self.config:
            self.config["winTitle"] = defaults["winTitle"]
        if "lastData" not in self.config:
            self.config["lastData"] = defaults["lastData"]

        self.writeGUIConfig()


    def initGUIConfig(self):
        # print ('initGUIConfig')
        availableGeometry = QDesktopWidget().availableGeometry()
        config = {}
        config["width"]        = 960
        config["height"]       = 560
        config["widthScreen"]  = availableGeometry.width()
        config["heightScreen"] = availableGeometry.height()
        config["left"]         = int((config["widthScreen"]  - config["width"])/2)
        config["top"]          = int((config["heightScreen"] - config["height"])/2)
        config["zipDir"]       = ""
        config["csvDir"]       = ""
        config["outDir"]       = os.path.dirname(os.path.abspath(__file__))
        config["winTitle"]     = winTitleDefault
        config["lastData"]     = {}
        return config

    def getGUIConfig(self):
        if os.path.isfile(jsonFilename):
            try:
                with open(jsonFilename, encoding='utf-8') as f:
                    config = json.loads(f.read())
            except:
                # print ('except')
                config = self.initGUIConfig()
        else:
            # print ('else')
            config = self.initGUIConfig()
        return config

    def __init__(self):
        super().__init__()


        # Centre the window
        self.config  = self.getGUIConfig()
        self.checkGUIConfig()

        self.title   = self.config["winTitle"]
        self.width   = self.config["width"]
        self.height  = self.config["height"]
        self.left    = self.config["left"]
        self.top     = self.config["top"]
        self.setMinimumSize(self.width, self.height)
        self.setMaximumSize(self.width, self.height)
        self.writeXML   = False
        self.writeJSON  = True
        self.preserveDB = True
        self.initUI()

    def waiting_effects(function):
        def new_function(self):
            QApplication.setOverrideCursor(QCursor(Qt.WaitCursor))
            try:
                function(self)
            except Exception as e:
                print("Error {}".format(e.args[0]))
                raise e
            finally:
                QApplication.restoreOverrideCursor()
        return new_function

    def initUI(self):

        buttonWidth  = 100
        buttonHeight = 100
        frameStartX  = 10
        labelStartX  = frameStartX+10
        labelWidth   = self.config["width"]-(buttonWidth+50)
        buttonStartX = labelStartX+labelWidth+10
        iconSize     = QSize((buttonWidth-10),(buttonHeight-10))
        frameHeight  = buttonHeight + 20
        labelHeight  = int((buttonHeight/2)+.5)
        frameWidth   = buttonStartX+buttonWidth


        self.setWindowIcon(getIconIcon("window"))
        self.setWindowFlags(Qt.WindowMinimizeButtonHint)

        self.zipPicked = False
        self.csvPicked = False
        self.setWindowTitle(self.title)
        self.setGeometry(self.left, self.top, self.width, self.height)
        self.filePathZip = ""
        self.filePathCSV = ""

        startY = 10
        self.zipframe = QFrame(self)
        self.zipframe.move(frameStartX, startY)
        self.zipframe.resize(frameWidth,frameHeight)
        self.zipframe.setFrameShape(QFrame.Box)
        self.zipframe.setFrameShadow(QFrame.Plain)
        self.zipframe.setLineWidth(1)

        self.zipPath = QPlainTextEdit (self)
        self.zipPath.setReadOnly(True)
        self.zipPath.setLineWrapMode(QPlainTextEdit.NoWrap)
        self.zipPath.move(labelStartX, startY + 10)
        self.zipPath.resize(labelWidth,labelHeight)
        self.zipFile = QPlainTextEdit(self)
        self.zipFile.setReadOnly(True)
        self.zipFile.setLineWrapMode(QPlainTextEdit.NoWrap)
        self.zipFile.move(labelStartX, startY + labelHeight + 10)
        self.zipFile.resize(labelWidth,labelHeight)
        # Create a button in the window
        self.zipbutton = QPushButton('', self)
        self.zipbutton.setIcon (getIconIcon("zip"))
        self.zipbutton.setIconSize (iconSize)
        self.zipbutton.setToolTip('Select ZIP file')
        self.zipbutton.move(buttonStartX,startY + 10)
        self.zipbutton.resize(buttonWidth,buttonHeight)


        startY += (frameHeight+20)
        self.csvframe = QFrame(self)
        self.csvframe.move(frameStartX, startY)
        self.csvframe.resize(frameWidth,frameHeight)
        self.csvframe.setFrameShape(QFrame.Box)
        self.csvframe.setFrameShadow(QFrame.Plain)
        self.csvframe.setLineWidth(1)

        self.csvPath = QPlainTextEdit(self)
        self.csvPath.setReadOnly(True)
        self.csvPath.setLineWrapMode(QPlainTextEdit.NoWrap)
        self.csvPath.move(labelStartX, startY + 10)
        self.csvPath.resize(labelWidth,labelHeight)
        self.csvFile = QPlainTextEdit(self)
        self.csvFile.setReadOnly(True)
        self.csvFile.setLineWrapMode(QPlainTextEdit.NoWrap)
        self.csvFile.move(labelStartX, startY + labelHeight + 10)
        self.csvFile.resize(labelWidth,labelHeight)
        # Create a button in the window
        self.csvbutton = QPushButton('', self)
        self.csvbutton.setIcon (getIconIcon("csv"))
        self.csvbutton.setIconSize (iconSize)
        self.csvbutton.setToolTip('Select CSV file')
        self.csvbutton.move(buttonStartX,startY + 10)
        self.csvbutton.resize(buttonWidth,buttonHeight)

        startY += (frameHeight+20)
        self.outframe = QFrame(self)
        self.outframe.move(frameStartX, startY)
        self.outframe.resize(frameWidth,frameHeight)
        self.outframe.setFrameShape(QFrame.Box)
        self.outframe.setFrameShadow(QFrame.Plain)
        self.outframe.setLineWidth(1)

        startY += 10
        self.outPath = QPlainTextEdit(self)
        self.outPath.setReadOnly(True)
        self.outPath.setLineWrapMode(QPlainTextEdit.NoWrap)
        self.outPath.move(labelStartX, startY)
        self.outPath.setPlainText (getShortenedDirForDisplay(self.config["outDir"]))
        self.outPath.resize(labelWidth,labelHeight)
        # Create a button in the window
        self.outbutton = QPushButton('', self)
        self.outbutton.setIcon (getIconIcon("output"))
        self.outbutton.setIconSize (iconSize)
        self.outbutton.setToolTip('Select output folder')
        self.outbutton.move(buttonStartX,startY)
        self.outbutton.resize(buttonWidth,buttonHeight)

        startY += (frameHeight+10)
        self.btnframe = QFrame(self)
        self.btnframe.move(buttonStartX-(buttonWidth+30),startY)
        self.btnframe.resize(((buttonWidth+20)*2),(buttonHeight+20))
        self.btnframe.setFrameShape(QFrame.Box)
        self.btnframe.setFrameShadow(QFrame.Plain)
        self.btnframe.setLineWidth(1)
        # print (startY+(buttonWidth+20))


        # Create a button in the window
        startY += 10
        self.runbutton = QPushButton('', self)
        self.runbutton.setIcon (getIconIcon("process"))
        self.runbutton.setIconSize (iconSize)
        self.runbutton.setToolTip('Process')
        self.runbutton.move(buttonStartX-(buttonWidth+20),startY)

        self.setTheRunButton()

        self.runbutton.resize(buttonWidth,buttonHeight)
        self.exitbutton = QPushButton('', self)
        self.exitbutton.setIcon (getIconIcon("exit"))
        self.exitbutton.setIconSize (iconSize)
        self.exitbutton.setToolTip('Go on, run away then')
        self.exitbutton.move(buttonStartX,startY)
        self.exitbutton.resize(buttonWidth,buttonHeight)

        self.zipbutton.clicked.connect(self.openZIPFileDialog)
        self.csvbutton.clicked.connect(self.openCSVFileDialog)
        self.runbutton.clicked.connect(self.process)
        self.outbutton.clicked.connect(self.outFolderDialog)
        self.exitbutton.clicked.connect(self.quit)

        self.logo = QLabel(self)
        self.logo.move(frameStartX, startY)
        self.logo.resize(buttonWidth,buttonHeight)
        self.logo.setPixmap(getIconPixmap("image").scaled(iconSize.width(),iconSize.height(), Qt.KeepAspectRatio, Qt.SmoothTransformation))

        self.resetFiles()

        self.show()


    def setTheRunButton (self):
        enabled = (self.zipPicked
                and self.csvPicked
                and "outDir" in self.config
                and len(self.config["outDir"]) > 0
                and os.path.exists(self.config["outDir"])
                  )
        self.runbutton.setEnabled(enabled)

    def quit(self):
        # print ("quit")
        self.setConfig()
        self.writeGUIConfig()
        self.close()

    def resetFiles(self):
        self.csvPath.setPlaceholderText ("=====>")
        self.csvFile.setPlaceholderText ("Select Gradecentre CSV file")
        self.zipPath.setPlaceholderText ("=====>")
        self.zipFile.setPlaceholderText ("Select Blackboard extract ZIP")
        self.csvPath.clear()
        self.csvFile.clear()
        self.zipPath.clear()
        self.zipFile.clear()
        self.zipPicked = False
        self.csvPicked = False
        self.setTheRunButton()

    @waiting_effects
    def callProcess(self):
        data  = RubricProcessor.processSingle (zipfile        = self.filePathZip[0]
                                              ,gradecentreCSV = self.filePathCSV[0]
                                              ,outputFolder   = self.config['outDir']
                                              ,writeXML       = self.writeXML
                                              ,writeJSON      = self.writeJSON
                                              ,preserveDB     = self.preserveDB
                                              )
        self.config["lastData"] = data
        for key, value in self.config["lastData"].items():
            if isinstance(value, datetime.datetime):
                self.config["lastData"][key] = value.strftime('%Y-%m-%d %H:%M:%S')

    def process(self):
        self.callProcess()
        self.showMsgBox(self.config["lastData"])
        self.resetFiles()

    def setZipPath(self):
        core.imsManifestCheck (self.filePathZip[0])
        self.zipPath.setPlainText (getShortenedDirForDisplay(self.filePathZip[1]))
        self.zipFile.setPlainText (self.filePathZip[2])
        self.config["zipDir"] = self.filePathZip[1]
        self.zipPicked = True
        self.setTheRunButton()

    def openZIPFileDialog(self):
        filter = "Zip Files (*.zip)"
        self.filePathZip = self.openFileNameDialog("Select Rubric ZIP extract",filter,self.config["zipDir"])
        if self.filePathZip is not None:
             self.setZipPath()

    def setOutDir(self,folder):
        self.config["outDir"] = folder
        self.outPath.setPlainText (getShortenedDirForDisplay(folder))
        self.setTheRunButton()

    def outFolderDialog(self):
        folder =  QFileDialog.getExistingDirectory(self, "Select Output Directory",self.config["outDir"],QFileDialog.ShowDirsOnly)
        if folder:
            self.setOutDir(folder)

    def setCSVPath(self):
        core.gradeCentreCheck (self.filePathCSV[0])
        self.csvPath.setPlainText (getShortenedDirForDisplay(self.filePathCSV[1]))
        self.csvFile.setPlainText (self.filePathCSV[2])
        self.config["csvDir"] = self.filePathCSV[1]
        self.csvPicked = True
        self.setTheRunButton()

    def openCSVFileDialog(self):
        filter = "CSV Files (*.csv)"
        self.filePathCSV = self.openFileNameDialog("Select GradeCentre CSV",filter,self.config["csvDir"])
        if self.filePathCSV is not None:
            self.setCSVPath()

    def openFileNameDialog(self,prompt, filter, defaultPath = ""):
        options  = QFileDialog.Options()
        options |= QFileDialog.ExistingFile
        options |= QFileDialog.ReadOnly
        options |= QFileDialog.DontUseCustomDirectoryIcons
        if   (defaultPath is not None
            and len(defaultPath) > 0
            and not os.path.exists(defaultPath)
            ):
            defaultPath = ""
        fileName, _ = QFileDialog.getOpenFileName(self,prompt, defaultPath,filter, options=options)
        if  (fileName is not None
        and len(fileName) > 0
        and os.path.isfile(fileName)
            ):
            thePath, theFile = os.path.split(fileName)
            retval = (fileName,thePath,theFile)
        else:
            retval = None
        return retval

    def showMsgBox(self,data):
        msg = MyMessageBox()
        msg.setIcon(QMessageBox.Information)
        msg.setWindowIcon(self.windowIcon())
        msg.setText("Processing Complete")
        # msg.setInformativeText("This is additional information")
        msg.setWindowTitle(self.title)
        detailText = ""
        maxKeyLen = 0
        for key, value in data.items():
            if len(key) > maxKeyLen:
                maxKeyLen = len(key)
        for key, value in data.items():
            detailText += f"{key.ljust(maxKeyLen, ' ')} : "
            if type(value) == str:
                detailText += f"\"{value}\"\n"
            else:
                detailText += f"{value}\n"
        msg.setDetailedText(detailText)
        msg.setStandardButtons(QMessageBox.Ok)
        msg.setEscapeButton(QMessageBox.Ok)
        msg.setDefaultButton(QMessageBox.Ok)
        retval = msg.exec_()

    def setConfig(self):
        # print ("setConfig")
        posn = self.pos()
        # print (posn)
        self.config["left"]   = posn.x()
        self.config["top"]    = posn.y()
        posn = None

    def writeGUIConfig(self):
        try:
            with open(jsonFilename, 'w') as f:
                f.write(json.dumps(self.config, indent=2))
        except PermissionError:
            pass

def reverseErrorStack (etype, value, tb):
   # leave out the "traceback" line, and then return in reverse order
   return '\n'.join(traceback.format_exception(etype, value, tb)[:0:-1])

def error_handler(etype, value, tb):
   msg = MyMessageBox()
   msg.setIcon(QMessageBox.Critical)
   msg.setWindowIcon(iconFromBase64(windowIconBase64))
   msg.setText("An exception has occurred")
   msg.setWindowTitle(winTitleDefault)
   msg.setDetailedText(reverseErrorStack(etype, value, tb))
   msg.setStandardButtons(QMessageBox.Ok)
   msg.setEscapeButton(QMessageBox.Ok)
   msg.setDefaultButton(QMessageBox.Ok)
   retval = msg.exec_()

def launch (zipfile = None, gradecentreCSV = None, outputFolder = None, writeXML = True, writeJSON = True, preserveDB = True):
   global iconDict
   sys.excepthook = error_handler  # redirect std error
   app    = QApplication(sys.argv)

   if iconDict == {}:
        iconDict = icons.getIconDict()
        # print (iconDict)

   if os.path.isfile(cssFilename):
      with open(cssFilename, 'r') as f:
         styleSheet = f.read()
   else:
      styleSheet  = qdarkstyle.load_stylesheet_pyqt5()
      styleSheet += "QTextEdit { font-family: Courier; }"
      styleSheet += "QLabel {font-family: system-ui; font-size: 18px;}"
      with open(cssFilename, 'w') as f:
         f.write(styleSheet)

   app.setStyleSheet(styleSheet)
   ex = App()

   def chopIt (fullpath):
        thePath, theFile = os.path.split(fullpath)
        return (fullpath,thePath,theFile)

   if zipfile is not None:
        ex.filePathZip = chopIt(zipfile)
        ex.setZipPath()
   if gradecentreCSV is not None:
        ex.filePathCSV = chopIt(gradecentreCSV)
        ex.setCSVPath()
   if outputFolder is not None:
        ex.setOutDir(outputFolder)

   ex.writeXML   = writeXML
   ex.writeJSON  = writeJSON
   ex.preserveDB = preserveDB

   sys.exit(app.exec_())
