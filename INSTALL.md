# Como instalar o KNX Field Tool no Home Assistant

## Método 1 — Instalação Local (mais simples, sem GitHub)

### Pré-requisitos
- Home Assistant OS ou Home Assistant Supervised
- Acesso SSH ou Samba ao teu HA

### Passos

1. **Copia os ficheiros para o HA**

   Via Samba (pasta `addons` na partilha de rede do HA):
   ```
   \\<IP_DO_HA>\addons\knx-field-tool\
   ```
   Ou via SSH:
   ```bash
   scp -r knx-field-tool/ root@<IP_DO_HA>:/addons/
   ```
   A estrutura dentro de `/addons/knx-field-tool/` deve ser:
   ```
   config.yaml
   Dockerfile
   build.yaml
   run.sh
   requirements.txt
   app.py
   knx_parser.py
   static/
     index.html
   DOCS.md
   ```

2. **No Home Assistant, vai a:**
   `Definições → Add-ons → Loja de Add-ons → ⋮ (menu) → Verificar atualizações`
   O add-on **KNX Field Tool** aparece em *Add-ons locais*.

3. **Instala e configura:**
   - Clica em *Instalar* (o HA faz o build Docker — pode demorar 2-3 min)
   - Na tab *Configuração*, define o IP do teu gateway KNX/IP
   - Clica em *Iniciar*
   - Activa *Mostrar na barra lateral* para acesso rápido

---

## Método 2 — Repositório GitHub

1. Faz fork deste repositório para a tua conta GitHub
2. No HA, vai a `Definições → Add-ons → Loja → ⋮ → Repositórios`
3. Adiciona o URL: `https://github.com/SEU_UTILIZADOR/knx-field-tool-addon`
4. O add-on aparece na loja — instala normalmente

---

## Estrutura de ficheiros no HA

Após instalação, o HA guarda o projecto ETS em:
```
/data/projects/last_project.knxproj
```
Este ficheiro persiste entre reinícios do add-on.
