for /l %%A in (1,1,24) do @xcopy "Choapa_Cuncumen" "Choapa_Cuncumen_%%A" /i
for /l %%A in (1,1,24) do cd %~dp0Choapa_Cuncumen_%%A & start cmd /k RunSlave.bat
