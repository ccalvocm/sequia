for /l %%A in (1,1,24) do @xcopy "Chalinga_Palmilla" "Chalinga_Palmilla_%%A" /i
for /l %%A in (1,1,24) do cd %~dp0Chalinga_Palmilla_%%A & start cmd /k RunSlave.bat
