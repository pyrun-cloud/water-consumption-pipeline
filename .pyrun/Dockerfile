# Python 3.9
FROM public.ecr.aws/docker/library/python:3.10-slim-buster

RUN apt-get update \
    # Install aws-lambda-cpp build dependencies
    && apt-get install -y \
      g++ \
      make \
      cmake \
      unzip \
      build-essential \
      gcc \
      grass \
      grass-core \
      grass-dev \
      libc-dev \
      libxslt-dev \
      libxml2-dev \
      libffi-dev \
      libssl-dev \
      zip \
      unzip \
      vim \
      libgdal-dev \
      gdal-bin \
      git \
    && rm -rf /var/lib/apt/lists/* \
    && apt-cache search linux-headers-generic


ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
ENV GRASSBIN=grass76
ENV HOME=/tmp
ENV PYTHONPATH="/utils"

ARG FUNCTION_DIR="/function"

# Copy function code
RUN mkdir -p ${FUNCTION_DIR}

# Update pip
ARG LITHOPS

RUN pip install --upgrade setuptools==65.6.0 && \
    pip install --upgrade pip && \
    pip install --upgrade requests && \
    pip install --upgrade packaging wheel
    
RUN pip install --upgrade --ignore-installed pip wheel six \
    && pip install --upgrade --no-cache-dir --ignore-installed \
        awslambdaric \
        boto3 \
        redis \
        httplib2 \
        requests \
        numpy \
        scipy \
        pandas \
        pika \
        kafka-python \
        cloudpickle \
        ps-mem \
        tblib \
        fiona \
        grass-session \
        rasterio \
        tenacity \
        psutil \
        ipykernel \
        ipywidgets \
        jupyter-kernel-gateway \
        ipyleaflet \
        utm \
        matplotlib \
        gql==3.4.1 \
        requests_toolbelt \
        git+https://github.com/CLOUDLAB-URV/dataplug@7a411e6949729d078a62dcb83a77d217bcdaa91a \
        "$LITHOPS"

COPY requirements.txt requirements.txt
COPY ./cloudbutton_geospatial /utils/cloudbutton_geospatial

# Actualiza pip, setuptools y wheel en una capa separada
RUN pip install --upgrade pip setuptools wheel

# Instala los paquetes listados en requirements.txt sin forzar --no-dependencies ni --no-cache-dir
RUN pip install -r requirements.txt
RUN pip install --upgrade typing_extensions


# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}

# Put your dependencies here, using RUN pip install... or RUN apt install...

#ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
#CMD [ "handler.entry_point.lambda_handler" ]