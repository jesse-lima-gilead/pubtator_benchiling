# Use TensorFlow 2.3.1 as the base image
FROM tensorflow/tensorflow:2.3.1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    default-jdk \
    curl \
    git \
    unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Download AWS CLI installer
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install
RUN rm awscliv2.zip

# Set the working directory in the container
WORKDIR /app

# Clone the AIONER repository from GitHub
RUN git clone https://github.com/ncbi/AIONER.git

# Set working directory to the cloned repo
WORKDIR /app/AIONER

# Download and unzip pretrained models, then remove the zip file
RUN curl -L -o pretrained_models.zip https://huggingface.co/lingbionlp/AIONER-0415/resolve/main/pretrained_models.zip && \
    unzip pretrained_models.zip && \
    rm pretrained_models.zip

# Remove all files in the AIONER/example/input directory
RUN rm -rf /app/AIONER/example/input/*

# Copy aioner_script.sh
COPY aioner_script.sh /app/AIONER/aioner_script.sh

# Give execute permissions to the script
RUN chmod +x /app/AIONER/aioner_script.sh

# Install the required Python packages
RUN pip install --no-cache-dir stanza==1.4.0 \
                bioc==2.0.post4 \
                tensorflow-addons==0.12.1 \
                spacy==3.2.4 \
                transformers==4.18.0

# Execute aioner_script.sh as the container's default command to keep it running
CMD ["/app/AIONER/aioner_script.sh"]
