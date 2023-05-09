for /l %%A in (1,1,24) do @xcopy "Hurtado_San_Agustin" "Hurtado_San_Agustin_%%A" /i
for /l %%A in (1,1,24) do cd %~dp0Hurtado_San_Agustin_%%A & start cmd /k RunSlave.bat
