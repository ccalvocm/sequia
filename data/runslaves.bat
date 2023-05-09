for /l %%A in (1,1,24) do @xcopy "Grande_Las_Ramadas" "Grande_Las_Ramadas_%%A" /i
for /l %%A in (1,1,24) do cd %~dp0Grande_Las_Ramadas_%%A & start cmd /k RunSlave.bat
