@echo off

set base=%CD%
set venv=%base%\venv
set scripts=%venv%\Scripts

call %scripts%\activate.bat
python -m pip install --upgrade --force-reinstall wheel setuptools
python -m pip install --upgrade --force-reinstall -r "%base%"\requirements.txt
call %scripts%\deactivate.bat

@echo on