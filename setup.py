import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup (name                          = "rubricprocessor"
                 ,version                       = "0.0.2"
                 ,author                        = "Jonathan Mills"
                 ,author_email                  = "jon@badger.shoes"
                 ,description                   = "Process Blackboard rubric archives"
                 ,long_description              = long_description
                 ,long_description_content_type = "text/markdown"
                 ,url                           = "https://github.com/theflyingbadger/rubricprocessor"
                 ,packages                      = setuptools.find_packages()
                 ,classifiers                   = ["Programming Language :: Python :: 3"
                                                  ,"License :: OSI Approved :: MIT License"
                                                  ,"Operating System :: OS Independent"
                                                  ]
                 , install_requires             = ['pandas>=0.25.3'
                                                  ,'openpyxl'
                                                  ,'PyQt5'
                                                  ,'qdarkstyle'
                                                  ,'pillow'
                                                  ,'lxml'
                                                  ,'sqlalchemy'
                                                  ,'filetype'
                                                  ]
                 ,python_requires               = '>=3.6'
                 )