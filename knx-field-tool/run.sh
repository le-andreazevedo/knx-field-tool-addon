#!/usr/bin/with-contenv bashio

KNX_HOST=$(bashio::config 'knx_gateway_host')
KNX_PORT=$(bashio::config 'knx_gateway_port')
KNX_FOLDER=$(bashio::config 'knx_project_folder')
KNX_FILE=$(bashio::config 'knx_project_file')

bashio::log.info "A iniciar KNX Field Tool..."
bashio::log.info "Gateway KNX: ${KNX_HOST}:${KNX_PORT}"
bashio::log.info "Pasta projectos: /config/${KNX_FOLDER}/"

if [ -n "${KNX_FILE}" ]; then
    bashio::log.info "Ficheiro ETS: ${KNX_FILE}"
else
    bashio::log.warning "knx_project_file nao definido nas opcoes do add-on"
fi

export PORT=8765
export HOST="0.0.0.0"
export KNX_DEFAULT_HOST="${KNX_HOST}"
export KNX_DEFAULT_PORT="${KNX_PORT}"
export KNX_PROJECT_FOLDER="${KNX_FOLDER}"
export KNX_PROJECT_FILE="${KNX_FILE}"

# /config e o directorio raiz de configuracao do HA
export KNX_CONFIG_DIR="/config"

export PROJECTS_DIR="/data/projects"
mkdir -p "${PROJECTS_DIR}"

# Cria a pasta dos projectos dentro de /config se nao existir
# (config esta montado ro, mas a pasta pode ja existir)
if [ -n "${KNX_FOLDER}" ] && [ ! -d "/config/${KNX_FOLDER}" ]; then
    bashio::log.warning "A pasta /config/${KNX_FOLDER}/ nao existe. Cria-a e coloca os ficheiros .knxproj la dentro."
fi

bashio::log.info "A verificar dependencias Python..."
python3 -c "import flask, flask_socketio, flask_cors, xknx" 2>&1 \
  || { bashio::log.error "Dependencia Python em falta!"; exit 1; }

bashio::log.info "Dependencias OK -- a lancar servidor..."

cd /usr/share/knx-field-tool
python3 -u app.py
EXIT_CODE=$?
bashio::log.error "Python terminou inesperadamente com codigo: ${EXIT_CODE}"
exit ${EXIT_CODE}
