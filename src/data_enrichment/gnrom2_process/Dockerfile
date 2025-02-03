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

# Download and extract GNorm2 code, then remove the tar.gz file
RUN curl -O https://www.ncbi.nlm.nih.gov/CBBresearch/Lu/Demo/tmTools/download/GNorm2/GNorm2.tar.gz && \
    tar -xvzf GNorm2.tar.gz && \
    rm GNorm2.tar.gz

# Set working directory to the downloaded repo
WORKDIR /app/GNorm2

# Remove all files in specified directories
RUN rm -rf /app/GNorm2/input/* \
           /app/GNorm2/output/* \
           /app/GNorm2/tmp_SR/* \
           /app/GNorm2/tmp_GNR/* \
           /app/GNorm2/tmp_SA/*

# Copy gnorm2_script.sh
COPY gnorm2_script.sh /app/GNorm2/gnorm2_script.sh

# Give execute permissions to the GNorm2 script
RUN chmod +x /app/GNorm2/gnorm2_script.sh

# Install the required Python packages
RUN pip install --no-cache-dir transformers==4.18.0 \
            stanza==1.4.0 \
            spacy==3.2.4 \
            bioc==2.0.post4 \
            protobuf==4.0.0rc1

# Execute gnorm2_script.sh as the container's default command to keep it running
CMD ["/app/GNorm2/gnorm2_script.sh"]
