FROM asia-docker.pkg.dev/vertex-ai/training/pytorch-gpu.1-11:latest

WORKDIR /home

RUN git clone https://github.com/HazyResearch/state-spaces
WORKDIR /home/state-spaces
RUN git checkout simple

# Install additional packages
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Sets up the entry point to invoke the trainer.
ENTRYPOINT ["python", "-m", "train", "experiment=s4-simple-cifar", "wandb=null"]
