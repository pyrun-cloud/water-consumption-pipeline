# Python 3.9
FROM public.ecr.aws/docker/library/python:3.10-slim-buster

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal \
    C_INCLUDE_PATH=/usr/include/gdal \
    GRASSBIN=grass76 \
    HOME=/tmp \
    PYTHONPATH="/utils"

ARG FUNCTION_DIR="/function"
ARG LITHOPS

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      g++ make cmake unzip build-essential gcc \
      grass grass-core grass-dev libc-dev \
      libxslt-dev libxml2-dev libffi-dev libssl-dev \
      zip vim libgdal-dev gdal-bin git \
    && rm -rf /var/lib/apt/lists/*


# Copy function code
RUN mkdir -p ${FUNCTION_DIR}

RUN pip install --upgrade pip setuptools wheel typing_extensions

RUN pip install --no-cache-dir \
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

RUN pip install --no-cache-dir -r requirements.txt

RUN pip install --no-cache-dir --upgrade "typing_extensions>=4.5.0"

# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}


# Put your dependencies here, using RUN pip install... or RUN apt install...

#ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
#CMD [ "handler.entry_point.lambda_handler" ]