ARG BUILD_FROM
FROM $BUILD_FROM

# Build dependencies for xknx / cryptography / ifaddr on Alpine
RUN apk add --no-cache \
    python3 \
    py3-pip \
    gcc \
    musl-dev \
    python3-dev \
    libffi-dev \
    openssl-dev \
    cargo \
    rust \
    make

# Create app directory
WORKDIR /usr/share/knx-field-tool

# Copy requirements first (layer cache)
COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

# Copy application files
COPY app.py .
COPY knx_parser.py .
COPY static/ ./static/

# Copy run script
COPY run.sh /run.sh
RUN chmod +x /run.sh

CMD [ "/run.sh" ]
