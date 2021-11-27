@echo off

set base=%CD%
set venv=%base%\venv
set scripts=%venv%\Scripts

python -m pip install virtualenv

rmdir /s /q "%venv%"
python -m venv --upgrade-deps --clear "%venv%"
call %scripts%\activate.bat
python -m pip install wheel setuptools
python -m pip install --upgrade --force-reinstall -r "%base%"\requirements.txt
call %scripts%\deactivate.bat

@echo on
