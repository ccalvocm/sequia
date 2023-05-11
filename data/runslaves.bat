for /l %%A in (1,1,24) do @xcopy "Combarbala_Ramadillas" "Combarbala_Ramadillas_%%A" /i
for /l %%A in (1,1,24) do cd %~dp0Combarbala_Ramadillas_%%A & start cmd /k RunSlave.bat