# Rubric Processor
 
 ## Blackboard Rubric processor


This python package processes [Blackboard](https://www.blackboard.com/en-apac) rubric export archives, and produces a XLSX file with tabs for each of the rubrics. Each tab has the **latest** score (both weighted and raw) for every student


The processor can be run in 3 different modes
* `cli` - call with parameters for a single rubric export55223db8a62cbe07fb122aa4bdedda16a734d3cc55223db8a62cbe07fb122aa4bdedda16a734d3cc
* `gui` - a pyqt5 gui to allow the user to select and process files from the ui
* `batch` - process multiple rubric exports as specified in a control file (csv format - expects "zipfile" and "gradecentre" columns to be present)

