for /l %%A in (1,1,24) do @xcopy "Mostazal_Cuestecita" "Mostazal_Cuestecita_%%A" /i
for /l %%A in (1,1,24) do cd %~dp0Mostazal_Cuestecita_%%A & start cmd /k RunSlave.bat
