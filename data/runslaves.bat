for /l %%A in (1,1,24) do @xcopy "Illapel_Las_Burras" "Illapel_Las_Burras_%%A" /i
for /l %%A in (1,1,24) do cd %~dp0Illapel_Las_Burras_%%A & start cmd /k RunSlave.bat
