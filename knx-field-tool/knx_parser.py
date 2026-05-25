"""
KNX Field Tool - .knxproj Parser
"""
import zipfile, xml.etree.ElementTree as ET, re, json
from io import BytesIO

NS = 'http://knx.org/xml/project/23'

def find_ns(elem, name):
    return elem.find(f'.//{{{NS}}}{name}')

def ga_to_str(addr_int, style='ThreeLevel'):
    a = int(addr_int)
    if style == 'ThreeLevel':  return f"{a>>11}/{(a>>8)&7}/{a&255}"
    if style == 'TwoLevel':    return f"{a>>11}/{a&2047}"
    return str(a)

def clean_tmpl(text):
    return re.sub(r'\s*\{\{\d+\}\}', '', text or '').strip()


class AppCache:
    """Per-application-XML cache of Channel texts and ComObject definitions."""
    def __init__(self):
        self.channels        = {}   # "CH-5"          -> clean channel text
        self.comobjects      = {}   # o_num             -> {name,text,fn,dpt}
        self.comobject_refs  = {}   # "O-31_R-19"      -> {text, fn}
                                    # "MD-4_O-2-0_R-10" -> {text,fn,tag,modular:True}
        self.mod_comobjects  = {}   # "MD-4_O-2-4"     -> {fn, dpt}  (base CO in module def)

    @classmethod
    def load(cls, raw):
        c = cls()
        try:
            root = ET.parse(BytesIO(raw)).getroot()
            for e in root.iter():
                tag = e.tag.split('}')[-1]
                if tag == 'Channel':
                    m = re.search(r'_(CH-\d+)$', e.get('Id',''))
                    if m:
                        t = clean_tmpl(e.get('Text','') or e.get('Name',''))
                        if t: c.channels[m.group(1)] = t

                elif tag == 'ComObjectRef':
                    eid = e.get('Id','')
                    # Standard: ends with _O-N_R-N
                    m = re.search(r'_(O-\d+_R-\d+)$', eid)
                    if m:
                        t  = clean_tmpl(e.get('Text',''))
                        fn = (e.get('FunctionText','') or '').strip()
                        if t or fn:
                            c.comobject_refs[m.group(1)] = {'text': t, 'fn': fn}
                    else:
                        # Modular template: ends with _MD-N_O-N[-N...]_R-N
                        mm = re.search(r'_(MD-\d+_O-[\d-]+_R-\d+)$', eid)
                        if mm:
                            t      = e.get('Text','') or ''  # keep {{}} vars raw
                            fn     = (e.get('FunctionText','') or '').strip()
                            tag_no = e.get('Tag','')
                            if t or fn:
                                c.comobject_refs[mm.group(1)] = {
                                    'text': t, 'fn': fn, 'tag': tag_no, 'modular': True,
                                }

                elif tag == 'ComObject':
                    eid = e.get('Id','')
                    if re.search(r'_O-\d+_R-\d+$', eid): continue   # instance record
                    # Modular base ComObject: ends with _MD-N_O-N-N (no _R-N suffix)
                    mm = re.search(r'_(MD-\d+_O-[\d-]+)$', eid)
                    if mm:
                        fn  = (e.get('FunctionText','') or '').strip()
                        dpt = e.get('DatapointType','')
                        if fn or dpt:
                            c.mod_comobjects[mm.group(1)] = {'fn': fn, 'dpt': dpt}
                        continue   # don't also try standard O-N match
                    bm = re.search(r'_O-(\d+)$', eid)
                    if bm:
                        n = int(bm.group(1))
                        c.comobjects[n] = {
                            'name': e.get('Name',''), 'text': e.get('Text',''),
                            'fn':   e.get('FunctionText',''), 'dpt': e.get('DatapointType',''),
                        }
        except Exception:
            pass
        return c

    def co_name(self, ref_id, ch_short=None, co_num=None):
        """Return 'Object Name - Function (Number)', e.g. 'Master Output 1 - Switch (31)'."""

        # ── Modular refs: MD-N_M-N_MI-N_O-N[-N]_R-N ─────────────────────
        mod_m = re.match(r'MD-(\d+)_M-\d+_MI-(\d+)_(O-[\d-]+_R-\d+)$', ref_id)
        if mod_m:
            md_num       = mod_m.group(1)
            mi_num       = int(mod_m.group(2))
            obj_ref      = mod_m.group(3)
            template_key = f"MD-{md_num}_{obj_ref}"
            ref = self.comobject_refs.get(template_key)
            if ref and ref.get('modular'):
                text   = ref.get('text', '')
                fn     = (ref.get('fn','') or '').strip()
                tag_no = ref.get('tag','')

                # Substitute {{WORD}} (e.g. {{ECG_NO}}) with MI instance number;
                # remove positional {{N}} args (CO-number / value placeholders)
                def _subst(s):
                    s = re.sub(r'\{\{\d+\}\}', '', s)            # drop {{0}}, {{1}}, ...
                    s = re.sub(r'\{\{[^}]+\}\}', str(mi_num), s) # {{ECG_NO}} -> 1
                    s = re.sub(r',\s*,', ',', s)                 # fix double commas
                    s = re.sub(r'[,\s]+$', '', s).strip()        # trailing comma/space
                    return s

                text = _subst(text)
                fn   = _subst(fn)

                # If the template ref has no FunctionText, fall back to the base
                # ComObject's FunctionText (e.g. 'On/Off', 'Value')
                if not fn:
                    o_base    = re.sub(r'_R-\d+$', '', obj_ref)   # O-2-4_R-13 -> O-2-4
                    mod_co_key = f"MD-{md_num}_{o_base}"
                    fn = self.mod_comobjects.get(mod_co_key, {}).get('fn', '')

                # CO number: use value from 0.xml if present, else Tag from template
                num_str = str(co_num) if co_num is not None else (tag_no or str(mi_num))

                if text and fn:
                    return f"{text} - {fn} ({num_str})"
                if text:
                    return f"{text} ({num_str})"
                if fn:
                    return f"{fn} ({num_str})"
            # No template found — return a readable fallback
            return f"MD-{md_num}/MI-{mi_num} ({co_num or obj_ref})"

        # ── Standard path ─────────────────────────────────────────────────
        m = re.match(r'O-(\d+)', ref_id) or re.search(r'_O-(\d+(?:-\d+)?)', ref_id)
        if not m: return ref_id
        o_num = int(m.group(1).split('-')[0])
        num   = str(co_num) if co_num is not None else str(o_num)

        # 1. ComObjectRef — most specific: has real Text + FunctionText per role
        ref = self.comobject_refs.get(ref_id)
        if ref:
            text = ref.get('text','')
            fn   = ref.get('fn','')
            if text and fn:
                return f"{text} - {fn} ({num})"
            if text:
                return f"{text} ({num})"
            if fn:
                return f"{fn} ({num})"

        # 2. Base ComObject fallback (non-generic names only)
        info = self.comobjects.get(o_num, {})
        base = info.get('text') or info.get('name') or ''
        fn   = info.get('fn','')
        if base and not re.match(r'GO_BASE_', base):
            fn_clean = fn if fn and not re.match(r'GO_BASE_', fn) and fn != base else ''
            label = f"{base} - {fn_clean}" if fn_clean else base
            return f"{label} ({num})"

        # 3. Channel fallback — when base CO is generic (GO_BASE_*), use the channel name
        if ch_short and ch_short in self.channels:
            return f"{self.channels[ch_short]} ({num})"

        return f"GO{num}"


class KNXProjectParser:
    def __init__(self, src):
        if isinstance(src, str):
            with open(src,'rb') as f: src = f.read()
        self.zf           = zipfile.ZipFile(BytesIO(src) if isinstance(src,bytes) else src)
        self.project_id   = None
        self.project_info = {}
        self.ga_style     = 'ThreeLevel'
        self.mfrs         = {}
        self.hw_map       = {}
        self.app_cache    = {}
        self.gas          = {}   # short GA-NNN -> info
        self.device_map   = {}   # full device Id -> dev dict
        self.topology     = []
        self.buildings    = []
        self.cabinets     = []   # flat list of DistributionBoard spaces
        self.ip_conns     = []

    def parse(self):
        self._load_mfrs()
        self._find_project()
        self._parse_hw()
        self._parse_install()
        return self._result()

    # ── Manufacturers ──────────────────────────────────────────
    def _load_mfrs(self):
        try:
            with self.zf.open('knx_master.xml') as f:
                root = ET.parse(f).getroot()
            for e in root.iter():
                if e.tag.split('}')[-1]=='Manufacturer' and e.get('Id') and e.get('Name'):
                    self.mfrs[e.get('Id')] = e.get('Name')
        except Exception: pass

    # ── Project metadata ───────────────────────────────────────
    def _find_project(self):
        for n in self.zf.namelist():
            m = re.match(r'^(P-[0-9A-Fa-f]+)/project\.xml$', n)
            if not m: continue
            self.project_id = m.group(1)
            with self.zf.open(n) as f:
                root = ET.parse(f).getroot()
            pi = find_ns(root,'ProjectInformation')
            if pi is not None:
                self.project_info = {
                    'name':          pi.get('Name',''),
                    'last_modified': pi.get('LastModified',''),
                    'project_start': pi.get('ProjectStart',''),
                    'comment':       pi.get('Comment',''),
                    'tool_version':  root.get('ToolVersion',''),
                }
                self.ga_style = pi.get('GroupAddressStyle','ThreeLevel')
            break
        for n in self.zf.namelist():
            if n.endswith('.info') and '/' not in n:
                try:
                    with self.zf.open(n) as f: info = json.load(f)
                    if 'ProjectName' in info and not self.project_info.get('name'):
                        self.project_info['name'] = info['ProjectName']
                except Exception: pass

    # ── Hardware / models ──────────────────────────────────────
    def _parse_hw(self):
        for n in self.zf.namelist():
            m = re.match(r'^(M-[0-9A-Fa-f]+)/Hardware\.xml$', n)
            if not m: continue
            mid = m.group(1); mn = self.mfrs.get(mid, mid)
            try:
                with self.zf.open(n) as f: raw = f.read()
                if not raw: continue
                root = ET.parse(BytesIO(raw)).getroot()
                for hw in root.iter():
                    if hw.tag.split('}')[-1]!='Hardware' or not hw.get('Id'): continue
                    hn = hw.get('Name',''); hs = hw.get('SerialNumber','')
                    for p in hw.iter():
                        if p.tag.split('}')[-1]=='Product' and p.get('Id'):
                            self.hw_map[p.get('Id')] = {
                                'mfr_id': mid, 'manufacturer': mn,
                                'hw_name': hn, 'product': p.get('Text', hn),
                                'order': p.get('OrderNumber',''), 'serial': hs,
                            }
            except Exception: pass

    # ── Application XML (CO names + channels) ─────────────────
    def _app_id(self, hw2p):
        if not hw2p: return None
        pts = hw2p.split('_')
        if len(pts) < 3: return None
        hp = pts[-1]
        return f"{pts[0]}_A-{hp[3:]}" if hp.startswith('HP-') else None

    def _ensure_app(self, hw2p):
        app_id = self._app_id(hw2p)
        if not app_id or app_id in self.app_cache: return app_id
        mid = hw2p.split('_')[0]
        fn  = f"{mid}/{app_id}.xml"
        if fn not in self.zf.namelist():
            fn = next((x for x in self.zf.namelist()
                       if x.startswith(f"{mid}/{app_id}") and x.endswith('.xml')), None)
        if fn is None:
            fn = next((x for x in self.zf.namelist()
                       if x.startswith(f"{mid}/") and x.endswith('.xml')
                       and app_id.startswith(x[len(mid)+1:-4])), None)
        if fn:
            try:
                with self.zf.open(fn) as f: raw = f.read()
                self.app_cache[app_id] = AppCache.load(raw)
            except Exception:
                self.app_cache[app_id] = AppCache()
        else:
            self.app_cache[app_id] = AppCache()
        return app_id

    def _co_name(self, hw2p, ref_id, ch_attr, co_num=None):
        app_id = self._ensure_app(hw2p)
        cache  = self.app_cache.get(app_id) if app_id else None
        if not cache: return ref_id
        ch_short = None
        if ch_attr:
            mm = re.search(r'(CH-\d+)$', ch_attr)
            ch_short = mm.group(1) if mm else ch_attr
        return cache.co_name(ref_id, ch_short, co_num)

    # ── Installation XML ───────────────────────────────────────
    def _parse_install(self):
        if not self.project_id: return
        p = f"{self.project_id}/0.xml"
        if p not in self.zf.namelist(): return
        with self.zf.open(p) as f:
            root = ET.parse(f).getroot()
        self._build_ga_map(root)
        self._parse_topology(root)
        self._parse_buildings(root)

    def _build_ga_map(self, root):
        for e in root.iter():
            if e.tag.split('}')[-1] != 'GroupAddress': continue
            fid = e.get('Id',''); sid = fid.split('_')[-1]
            ai  = int(e.get('Address', 0))
            self.gas[sid] = {
                'id': fid, 'short_id': sid,
                'address': ga_to_str(ai, self.ga_style), 'address_int': ai,
                'name': e.get('Name',''), 'dpt': e.get('DatapointType',''),
                'description': e.get('Description',''),
            }

    def _build_ga_tree(self, root):
        def rec(e):
            tag = e.tag.split('}')[-1]
            if tag == 'GroupRange':
                node = {'type':'range','name':e.get('Name',''),
                        'range_start':int(e.get('RangeStart',0)),
                        'range_end':int(e.get('RangeEnd',0)),'children':[]}
                for ch in e:
                    r = rec(ch)
                    if r: node['children'].append(r)
                return node
            if tag == 'GroupAddress':
                ai = int(e.get('Address',0)); fid = e.get('Id','')
                return {'type':'address','id':fid.split('_')[-1],
                        'address':ga_to_str(ai,self.ga_style),'address_int':ai,
                        'name':e.get('Name',''),'dpt':e.get('DatapointType',''),
                        'description':e.get('Description','')}
            return None
        result = []
        for e in root.iter():
            if e.tag.split('}')[-1] == 'GroupRanges':
                for ch in e:
                    r = rec(ch)
                    if r: result.append(r)
                break
        return result

    def _parse_topology(self, root):
        for e in root.iter():
            if e.tag.split('}')[-1]=='BusAccess':
                p = e.get('Parameter','')
                if 'Ip' in p: self.ip_conns.append(self._parse_bus(p))
        for area in root.iter():
            if area.tag.split('}')[-1] != 'Area': continue
            aa = int(area.get('Address',0))
            anode = {'address':aa,'name':area.get('Name',f'Area {aa}'),'lines':[]}
            for line in area:
                if line.tag.split('}')[-1] != 'Line': continue
                la = int(line.get('Address',0))
                lnode = {'address':la,'name':line.get('Name',f'Line {la}'),
                         'individual_address':f"{aa}.{la}",'devices':[]}
                for dev_e in line.iter():
                    if dev_e.tag.split('}')[-1] == 'DeviceInstance':
                        d = self._parse_dev(dev_e, aa, la)
                        if d:
                            lnode['devices'].append(d)
                            self.device_map[dev_e.get('Id','')] = d
                lnode['devices'].sort(key=lambda x: x['address'])
                anode['lines'].append(lnode)
            self.topology.append(anode)

    def _parse_dev(self, e, aa, la):
        da   = int(e.get('Address',0))
        prod = e.get('ProductRefId','')
        hw2p = e.get('Hardware2ProgramRefId','')
        hw   = self.hw_map.get(prod, {})
        cos  = []
        for refs in e:
            if refs.tag.split('}')[-1] != 'ComObjectInstanceRefs': continue
            for co in refs:
                if co.tag.split('}')[-1] != 'ComObjectInstanceRef': continue
                ref    = co.get('RefId','')
                chid   = co.get('ChannelId', None)
                lnks   = co.get('Links','')
                raw_num = co.get('Number', None)
                co_num  = int(raw_num) if raw_num is not None and raw_num.isdigit() else None
                name = self._co_name(hw2p, ref, chid, co_num)
                gas  = []
                for sid in lnks.split():
                    gi = self.gas.get(sid)
                    if gi:
                        gas.append({'id':sid,'address':gi['address'],
                                    'address_int':gi['address_int'],
                                    'name':gi['name'],'dpt':gi['dpt']})
                if gas: cos.append({'ref_id':ref,'name':name,'linked_gas':gas})
        # Sort COs by object number: standard (O-N) first, then modular (by MI then R)
        def _co_key(c):
            r = c['ref_id']
            mod = re.match(r'MD-(\d+)_M-\d+_MI-(\d+)_.*_R-(\d+)$', r)
            if mod:
                return (1, int(mod.group(1)), int(mod.group(2)), int(mod.group(3)))
            m = re.search(r'_O-(\d+)', r) or re.match(r'O-(\d+)', r)
            return (0, int(m.group(1)), 0, 0) if m else (2, 0, 0, 0)
        cos.sort(key=_co_key)
        def flag(attr): return e.get(attr,'false').lower() == 'true'
        return {
            'id':                 e.get('Id',''),
            'address':            da,
            'individual_address': f"{aa}.{la}.{da}",
            'name':               e.get('Name',''),
            'description':        e.get('Description',''),
            'manufacturer':       hw.get('manufacturer',''),
            'model':              hw.get('product', hw.get('hw_name','')),
            'order_number':       hw.get('order',''),
            'application_loaded': flag('ApplicationProgramLoaded'),
            'flags': {
                'Adr': flag('IndividualAddressLoaded'),
                'Par': flag('ParametersLoaded'),
                'Grp': flag('CommunicationPartLoaded'),
                'Cfg': flag('ApplicationProgramLoaded'),
            },
            'com_objects':        cos,
        }

    def _parse_buildings(self, root):
        ICONS = {
            'Building':'building','BuildingPart':'building-part',
            'Floor':'floor','Room':'room','Corridor':'corridor',
            'DistributionBoard':'board','Staircase':'staircase','Area':'area',
        }
        def parse_space(e, breadcrumb=None):
            if breadcrumb is None:
                breadcrumb = []
            st   = e.get('Type', e.tag.split('}')[-1])
            name = e.get('Name','')
            devs = []
            kids = []
            for ch in e:
                ct = ch.tag.split('}')[-1]
                if ct == 'DeviceInstanceRef':
                    d = self.device_map.get(ch.get('RefId',''))
                    if d: devs.append(d)
                elif ct == 'Space':
                    k = parse_space(ch, breadcrumb + [name])
                    if k: kids.append(k)
            # Sort devices by individual address (numeric)
            devs.sort(key=lambda d: d['address'])
            if st == 'DistributionBoard':
                self.cabinets.append({
                    'id':         e.get('Id',''),
                    'name':       name,
                    'breadcrumb': breadcrumb,
                    'devices':    devs,
                })
            return {'id':e.get('Id',''),'type':st,'icon':ICONS.get(st,'folder'),
                    'name':name,'children':kids,'devices':devs}
        for e in root.iter():
            if e.tag.split('}')[-1] == 'Locations':
                for ch in e:
                    if ch.tag.split('}')[-1] == 'Space':
                        self.buildings.append(parse_space(ch))
                break

    def _parse_bus(self, p):
        params = {}
        for kv in p.split(';'):
            if '=' in kv:
                k,v = kv.split('=',1); params[k.strip()] = v.strip()
        port = 3671
        try: port = int(params.get('Port',3671))
        except Exception: pass
        return {'type':params.get('Type','Unknown'),'host':params.get('HostAddress',''),
                'port':port,'protocol':params.get('ProtocolType','Udp'),
                'name':params.get('Name',''),'raw':p}

    # ── Final result ───────────────────────────────────────────
    def _result(self):
        p = f"{self.project_id}/0.xml"
        ga_tree = []
        if self.project_id and p in self.zf.namelist():
            with self.zf.open(p) as f:
                root = ET.parse(f).getroot()
            ga_tree = self._build_ga_tree(root)
        total = sum(len(l['devices']) for a in self.topology for l in a['lines'])
        return {
            'project': {**self.project_info,'ga_style':self.ga_style,
                        'total_devices':total,'total_group_addresses':len(self.gas),
                        'ip_connections':self.ip_conns},
            'topology':self.topology,'buildings':self.buildings,
            'cabinets':self.cabinets,
            'group_addresses':list(self.gas.values()),
            'ga_tree':ga_tree,'ip_connections':self.ip_conns,
        }


def parse_knxproj(src):
    return KNXProjectParser(src).parse()
