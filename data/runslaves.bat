for /l %%A in (1,1,24) do @xcopy "Cogoti_Embalse_Cogoti" "Cogoti_Embalse_Cogoti_%%A" /i
for /l %%A in (1,1,24) do cd %~dp0Cogoti_Embalse_Cogoti_%%A & start cmd /k RunSlave.bat
