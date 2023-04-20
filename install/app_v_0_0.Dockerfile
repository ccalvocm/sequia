# Define base image
FROM continuumio/miniconda3
 
# Set working directory for the project
WORKDIR .
 
# Create Conda environment from the YAML file
COPY environment.yaml .
RUN conda env create -f environment.yaml
 
# Override default shell and use bash
SHELL ["conda", "run", "-n", "sequia", "/bin/bash", "-c"]
 
# Activate Conda environment and check if it is working properly
RUN echo "Making sure flask is installed correctly..."
RUN python -c "print('EL MEMOS')"

#git clone
RUN git clone https://github.com/ccalvocm/sequia.git
 
# Python program to run in the container
RUN pwd
ENTRYPOINT ["conda", "run", "-n", "sequia", "python", "./sequia/src/MasterForecast.py"]
