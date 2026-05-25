# KNX Field Tool — Home Assistant Add-on Repository

[![Home Assistant](https://img.shields.io/badge/Home%20Assistant-Add--on-blue)](https://www.home-assistant.io/)

Repositório de add-ons para Home Assistant.

## Instalação

1. No Home Assistant, vai a **Definições → Add-ons → Loja de Add-ons**
2. Clica em **⋮ (menu) → Repositórios**
3. Adiciona o URL:
   ```
   https://github.com/le-andreazevedo/knx-field-tool-addon
   ```
4. O add-on **KNX Field Tool** aparece na secção *Add-ons locais / repositórios personalizados*
5. Clica em **Instalar** (o HA faz o build Docker — pode demorar 2-3 min)

## Add-ons disponíveis

### KNX Field Tool

Ferramenta de campo KNX integrada no Home Assistant:

- 📂 **Explorador de projecto ETS** — carrega e analisa ficheiros `.knxproj`
- 📡 **Group Monitor** — monitorização em tempo real de telegramas KNX via IP Tunneling
- 🔍 **Diagnóstico** — verifica quais os dispositivos online/offline
- ✏️ **Escrita de GA** — envia valores para endereços de grupo

## Configuração

Após instalação, define nas opções do add-on:

| Opção | Descrição |
|---|---|
| `knx_gateway_host` | IP do gateway KNX/IP (ex: `192.168.1.100`) |
| `knx_gateway_port` | Porto UDP (padrão: `3671`) |
| `knx_project_folder` | Pasta dentro de `/config` com os ficheiros ETS (padrão: `knx-field-tool`) |
| `knx_project_file` | Nome do ficheiro `.knxproj` a carregar automaticamente |

## Atualização

Quando uma nova versão é publicada neste repositório, o Home Assistant mostra uma notificação de atualização no painel do add-on.

## Licença

MIT — André Azevedo / Life Emotions
