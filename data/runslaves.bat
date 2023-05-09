for /l %%A in (1,1,24) do @xcopy "Tascadero_Desembocadura" "Tascadero_Desembocadura_%%A" /i
for /l %%A in (1,1,24) do cd %~dp0Tascadero_Desembocadura_%%A & start cmd /k RunSlave.bat
