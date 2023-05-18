# sequia
 Streamflow forecast model in northern Chilean semi-arid basins. 
 
## Installation
### Linux
 sudo bash ./install/Dockerfile.sh
### Windows 
 cd install

 docker build -t ccalvocm/sequia --no-cache --pull -f app_v_0_0.Dockerfile .

## Execution
 docker run -t ccalvocm/sequia
