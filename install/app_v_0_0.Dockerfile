# Define base image                                  
FROM --platform=linux/amd64 continuumio/miniconda3                          
                                                     
# Set working directory for the project
WORKDIR .

RUN apt-get update && apt-get install cron -y && apt-get install vim -y && apt-get install ssh -y
RUN echo "root:anid-sequia2023"

# Create Conda environment from the YAML file
COPY environment.yaml .
RUN conda env create -f environment.yaml || echo "entorno ya creado"
 
# Override default shell and use bash
SHELL ["conda", "run", "-n", "sequia", "/bin/bash", "-c"]
 
# Activate Conda environment and check if it is working properly
RUN echo "Making sure flask is installed correctly..."

#git clone
RUN if [ -d "sequia" ]; then rm -Rf sequia; fi
RUN git clone https://github.com/ccalvocm/sequia.git
 
# Python program to run in the container
ENTRYPOINT ["conda", "run", "-n", "sequia", "python", "./sequia/src/MasterForecast.py"]