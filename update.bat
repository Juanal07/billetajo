@echo off

set basedir=%CD%
set venv=%basedir%\venv
set scripts=%venv%\Scripts

call "%scripts%"\activate.bat
python -m pip install --upgrade --force-reinstall wheel setuptools
python -m pip install --upgrade --force-reinstall -r "%basedir%"\requirements.txt
call "%scripts%"\deactivate.bat

@echo on
