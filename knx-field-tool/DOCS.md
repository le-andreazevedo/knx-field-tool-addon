# KNX Field Tool — Add-on para Home Assistant

Ferramenta de campo KNX integrada no Home Assistant. Permite:

- Carregar e explorar ficheiros de projecto ETS (`.knxproj`)
- Monitorizar telegramas de grupo em tempo real
- Fazer ping a dispositivos individuais
- Diagnóstico geral da instalação (online/offline)
- Escrever valores em endereços de grupo

## Configuração

| Opção | Descrição | Exemplo |
|---|---|---|
| `knx_gateway_host` | IP do gateway KNX/IP | `192.168.1.100` |
| `knx_gateway_port` | Porto UDP do gateway (padrão: 3671) | `3671` |

Se configurares o `knx_gateway_host`, o campo de IP é automaticamente preenchido no arranque da ferramenta.

## Utilização

1. Instala e arranca o add-on
2. Abre a interface através do painel lateral do Home Assistant (ícone de broadcast)
3. Carrega o teu ficheiro `.knxproj` na tab **Projecto**
4. O ficheiro é guardado automaticamente — não precisas de o voltar a carregar após reinício

## Notas

- O projecto ETS é guardado em `/data/projects/last_project.knxproj` e recarregado automaticamente ao reiniciar
- A comunicação KNX é feita directamente da rede do container para o gateway — o HA não interfere
