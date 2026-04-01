import asyncio
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from playwright.async_api import async_playwright

# ─────────────────────────────────────────────────────────────────
#  CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────────
PROMOTOR         = 'MUNICIPIO DE TEIXEIRA DE FREITAS'
LIMITE_PROCESSOS = 9999    # reduza para testar (ex: 15)
ARQUIVO_JSON     = 'dados.json'

# Modo de execução do browser — detectado automaticamente:
#   - Windows          → headless=False  (abre janela normalmente)
#   - Linux com DISPLAY → headless=False  (display real ou Xvfb já ativo)
#   - Linux sem DISPLAY → inicia Xvfb internamente e usa headless=False
# Para forçar um modo específico use as flags:
#   python extrator_bll_teixeira.py --headless     (força headless puro)
#   python extrator_bll_teixeira.py --no-headless  (força janela visível)
_CI        = os.getenv('CI', '').lower() == 'true'
_HEADLESS_ARG = '--headless' in sys.argv
_NO_HEADLESS_ARG = '--no-headless' in sys.argv
_LINUX     = sys.platform.startswith('linux')
_TEM_DISPLAY = bool(os.environ.get('DISPLAY', ''))

# ─────────────────────────────────────────────────────────────────
#  XVFB — display virtual para Linux sem tela física
# ─────────────────────────────────────────────────────────────────
_xvfb_proc = None   # processo Xvfb iniciado por este script

def iniciar_xvfb(display=':99', resolucao='1920x1080x24'):
    """
    Inicia um servidor Xvfb (display virtual) se ainda não houver DISPLAY.
    Retorna True se bem-sucedido ou se já havia DISPLAY disponível.
    Exige: sudo apt-get install -y xvfb  (já incluso no GitHub Actions)
    """
    global _xvfb_proc

    if not _LINUX:
        return True   # Windows/Mac não precisam de Xvfb

    if _TEM_DISPLAY:
        print(f"   🖥️  DISPLAY já disponível: {os.environ['DISPLAY']}")
        return True

    if not shutil.which('Xvfb'):
        print("   ⚠️  Xvfb não encontrado — instale com: sudo apt-get install -y xvfb")
        return False

    try:
        # Mata qualquer Xvfb residual no mesmo display
        subprocess.run(['pkill', '-f', f'Xvfb {display}'],
                       capture_output=True, timeout=5)

        _xvfb_proc = subprocess.Popen(
            ['Xvfb', display, '-screen', '0', resolucao, '-ac', '+extension', 'GLX'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        import time; time.sleep(1.5)   # aguarda o servidor inicializar

        if _xvfb_proc.poll() is not None:
            print(f"   ❌ Xvfb falhou ao iniciar (código {_xvfb_proc.returncode})")
            return False

        os.environ['DISPLAY'] = display
        print(f"   🖥️  Xvfb iniciado em DISPLAY={display}  ({resolucao})")
        return True

    except Exception as e:
        print(f"   ❌ Erro ao iniciar Xvfb: {e}")
        return False


def encerrar_xvfb():
    """Encerra o Xvfb iniciado por este script (se houver)."""
    global _xvfb_proc
    if _xvfb_proc and _xvfb_proc.poll() is None:
        _xvfb_proc.terminate()
        try:
            _xvfb_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _xvfb_proc.kill()
        print("   🖥️  Xvfb encerrado.")
        _xvfb_proc = None


def determinar_modo_headless():
    """
    Decide se o browser vai rodar em modo headless ou com display.
    Retorna (headless: bool, descricao: str).
    """
    if _HEADLESS_ARG:
        return True, 'headless forçado por --headless'
    if _NO_HEADLESS_ARG:
        return False, 'janela visível forçada por --no-headless'
    if _CI:
        # No CI usa Xvfb + headless=False
        return False, 'CI detectado → headless=False com Xvfb'
    if _LINUX and not _TEM_DISPLAY:
        # Linux sem tela → tenta Xvfb
        return False, 'Linux sem DISPLAY → headless=False com Xvfb'
    # Windows ou Linux com display real
    return False, 'headless=False (janela visível)'

# ─────────────────────────────────────────────────────────────────
#  SANITIZAÇÃO
# ─────────────────────────────────────────────────────────────────
_CTRL = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]')

def _clean(v):
    return _CTRL.sub('', v) if isinstance(v, str) else v

def _clean_obj(obj):
    if isinstance(obj, dict):  return {k: _clean_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):  return [_clean_obj(i) for i in obj]
    if isinstance(obj, str):   return _clean(obj)
    return obj

# ─────────────────────────────────────────────────────────────────
#  JAVASCRIPT REUTILIZÁVEL
# ─────────────────────────────────────────────────────────────────
JS_LER_LOTE = """
() => {
    const area = document.getElementById('ProcessViewInfo');
    if (!area) return null;
    function getCampo(name) {
        const el = area.querySelector('input[name="' + name + '"]');
        return el ? el.value.trim() : '';
    }
    function getItens() {
        const tabela = area.querySelector(
            'table.table-striped.table-defaultsys.table-bordered.table-data'
        );
        if (!tabela) return [];
        const headers = Array.from(tabela.querySelectorAll('th')).map(th => th.innerText.trim());
        const itens = [];
        tabela.querySelectorAll('tbody tr').forEach(row => {
            const tds = row.querySelectorAll('td');
            if (tds.length === headers.length) {
                const item = {};
                headers.forEach((h, i) => { item[h] = tds[i].innerText.trim(); });
                itens.push(item);
            }
        });
        return itens;
    }
    return {
        numero:        getCampo('Number'),
        titulo:        getCampo('Title'),
        fase:          getCampo('Status'),
        tipo:          getCampo('BidKind'),
        quantidade:    getCampo('Quantity'),
        intervalo:     getCampo('BidMargin'),
        exclusivo:     getCampo('IsMeExclusive'),
        entrega:       getCampo('DeliveryPlace'),
        garantia:      getCampo('Warranty'),
        valor_ref:     getCampo('BaseValue'),
        vencedor:      getCampo('WinnerName'),
        melhor_oferta: getCampo('WinnerBidValue'),
        itens:         getItens()
    };
}
"""

JS_TOTAL_LOTES = """
() => {
    const area = document.getElementById('ProcessViewInfo');
    if (!area) return 1;
    const menu = area.querySelector(
        'table.table-hover.table-defaultsys.table-striped.table-bordered'
    );
    if (!menu) return 1;
    const rows = Array.from(menu.querySelectorAll('tbody tr'))
        .filter(tr => /^\\d+/.test(tr.innerText.trim()));
    return rows.length > 0 ? rows.length : 1;
}
"""

# ─────────────────────────────────────────────────────────────────
#  CARREGAMENTO DO JSON EXISTENTE
# ─────────────────────────────────────────────────────────────────
def carregar_json_existente():
    """
    Carrega o dados.json e indexa por id como lista.
    Usa lista porque o BLL pode ter o mesmo número de edital
    com situações diferentes (processos distintos, mesmo id).
    Retorna: { id -> [proc1, proc2, ...] }
    """
    if not os.path.exists(ARQUIVO_JSON):
        print(f"📂 '{ARQUIVO_JSON}' não encontrado — será criado do zero.")
        return {}, []
    try:
        from collections import defaultdict
        with open(ARQUIVO_JSON, 'r', encoding='utf-8') as f:
            dados = json.load(f)
        processos = dados.get('processos', [])
        indice = defaultdict(list)
        for p in processos:
            pid = p.get('id', '').strip()
            if pid:
                indice[pid].append(p)
        total = sum(len(v) for v in indice.values())
        print(f"📂 '{ARQUIVO_JSON}' carregado — {total} processo(s) em {len(indice)} id(s) únicos.")
        return indice, processos   # processos = lista completa para manter ordem
    except Exception as e:
        print(f"⚠️  Erro ao ler '{ARQUIVO_JSON}': {e} — será recriado do zero.")
        return {}, []

# ─────────────────────────────────────────────────────────────────
#  SCROLL INFINITO
# ─────────────────────────────────────────────────────────────────
async def scroll_para_carregar_todos(page):
    print("📜 Carregando todos os registros via scroll...")
    anterior = sem_mudanca = 0
    while True:
        atual = await page.locator('#tableProcessDataBody tr').count()
        print(f"   Registros visíveis: {atual}", end='\r')
        if atual == anterior:
            sem_mudanca += 1
            if sem_mudanca >= 3:
                print(f"\n✅ Total carregado: {atual} registros")
                break
        else:
            sem_mudanca = 0
            anterior = atual
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(2500)

# ─────────────────────────────────────────────────────────────────
#  COLETA DE LINKS + SITUAÇÃO DA LISTAGEM
# ─────────────────────────────────────────────────────────────────
async def coletar_links_e_situacoes(page, limite):
    total_disp = await page.locator('#tableProcessDataBody tr').count()
    total      = min(limite, total_disp)
    print(f"\n📋 {total} processo(s) encontrado(s) na listagem (de {total_disp} disponíveis)")

    dados_js = await page.evaluate(f"""
        () => {{
            const linhas = Array.from(
                document.querySelectorAll('#tableProcessDataBody tr')
            ).slice(0, {total});
            return linhas.map(tr => {{
                const link = tr.querySelector('a');
                const tds  = Array.from(tr.querySelectorAll('td'));
                return {{
                    href:    link ? link.getAttribute('href') : '',
                    colunas: tds.map(td => td.innerText.trim())
                }};
            }}).filter(r => r.href);
        }}
    """)

    if not dados_js:
        print("   ⚠️  Nenhum registro lido da tabela!")
        return []

    # Mostra mapa das 2 primeiras linhas para diagnóstico
    print("   Mapa de colunas (2 primeiros registros):")
    for r in dados_js[:2]:
        print(f"   href: ...{r['href'][-40:]}")
        for i, col in enumerate(r['colunas']):
            print(f"     td[{i}] = {col!r}")

    # Detecta coluna do número do edital (padrão: letras-número-ano, ex: PE-001-2025)
    # e coluna da situação (texto da lista de situações conhecidas do BLL)
    re_edital = re.compile(r'^(\d+-)?[A-Za-z]{2,}-\d+-\d{4}$')
    situacoes_bll = {
        'RECEPÇÃO DE PROPOSTAS', 'HOMOLOGADO', 'FRACASSADO', 'DESERTO',
        'ADJUDICADO', 'ANULADO', 'ANÁLISE DE PROPOSTAS', 'EM FINALIZAÇÃO',
        'HABILITAÇÃO', 'REVOGADO', 'SUSPENSO', 'PUBLICADO', 'CANCELADO',
        'EM DISPUTA', 'ENCERRADO', 'EM JULGAMENTO', 'NEGOCIAÇÃO'
    }

    amostra  = [r['colunas'] for r in dados_js[:20] if r['colunas']]
    n_cols   = max(len(c) for c in amostra) if amostra else 0
    col_edital   = -1
    col_situacao = -1

    for ci in range(n_cols):
        valores = [row[ci] for row in amostra if ci < len(row) and row[ci]]
        if not valores:
            continue
        if col_edital == -1:
            hits = sum(1 for v in valores if re_edital.match(v))
            if hits >= len(valores) * 0.6:
                col_edital = ci
        if col_situacao == -1:
            hits = sum(1 for v in valores if v.upper() in situacoes_bll)
            if hits >= len(valores) * 0.6:
                col_situacao = ci

    print(f"   Colunas detectadas — Nº Edital: td[{col_edital}]  |  Situação: td[{col_situacao}]")

    registros = []
    for r in dados_js:
        cols          = r['colunas']
        link_completo = f"https://bllcompras.com{r['href']}"
        num_edital    = cols[col_edital].strip()   if col_edital   >= 0 and col_edital   < len(cols) else ''
        situacao      = cols[col_situacao].strip()  if col_situacao >= 0 and col_situacao < len(cols) else ''
        registros.append({
            'link':              link_completo,
            'id':                num_edital,
            'situacao_listagem': situacao,
        })

    print("   Amostra final (3 primeiros):")
    for r in registros[:3]:
        print(f"     id={r['id']!r:25s}  sit={r['situacao_listagem']!r}")

    return registros

# ─────────────────────────────────────────────────────────────────
#  LÓGICA DE ATUALIZAÇÃO INCREMENTAL
# ─────────────────────────────────────────────────────────────────
def decidir_o_que_processar(registros_bll, indice_existente, todos_existentes):
    """
    Compara listagem BLL com JSON existente.
    indice_existente: { id -> [proc, proc, ...] }  (lista por id)
    todos_existentes: lista completa original

    Regras:
    - ID não existe no JSON            → NOVO        → extrai
    - ID existe, situação já no JSON   → sem mudança → mantém
    - ID existe, situação é nova       → ATUALIZADO  → remove antigo + extrai novo
    """
    a_processar   = []
    links_remover = set()   # links dos processos antigos a serem removidos do JSON
    sem_id = novos = atualizados = ignorados = 0

    for reg in registros_bll:
        pid     = reg.get('id', '').strip()
        sit_bll = (reg.get('situacao_listagem') or '').strip().upper()

        if not pid:
            a_processar.append({'link': reg['link'], 'motivo': 'SEM ID'})
            sem_id += 1
            continue

        entradas_json = indice_existente.get(pid, [])

        if not entradas_json:
            # ID nunca visto → processo genuinamente novo
            a_processar.append({'link': reg['link'], 'motivo': f'NOVO ({pid})'})
            novos += 1
        else:
            sits_json = {e.get('situacao','').strip().upper() for e in entradas_json}
            if sit_bll and sit_bll not in sits_json:
                # Situação mudou → extrai processo atualizado
                # e marca o(s) antigo(s) com esse id+sit_antiga para remoção
                # (remove todos com esse id pois o processo foi substituído)
                for e in entradas_json:
                    sit_e = e.get('situacao','').strip().upper()
                    # Só remove se era a "versão anterior" — ou seja,
                    # se havia apenas 1 entrada para esse id, remove ela.
                    # Se havia múltiplas (mesmo id, situações diferentes),
                    # mantém as outras e só remove a que tinha situação diferente da BLL.
                    if sit_e != sit_bll:
                        links_remover.add(e.get('link',''))
                a_processar.append({
                    'link':   reg['link'],
                    'motivo': f'ATUALIZADO {pid} ({" / ".join(sits_json)} → {sit_bll})'
                })
                atualizados += 1
            else:
                ignorados += 1

    # Monta a_manter excluindo os processos que serão reextraídos
    a_manter = [p for p in todos_existentes if p.get('link','') not in links_remover]

    print(f"\n📊 Resumo da comparação:")
    print(f"   🆕 Novos:        {novos}")
    print(f"   🔄 Atualizados:  {atualizados}  ({len(links_remover)} versão(ões) antiga(s) removida(s))")
    print(f"   ✅ Sem mudança:  {ignorados}")
    if sem_id:
        print(f"   ⚠️  Sem ID:       {sem_id}")
    print(f"   🔍 A extrair:    {len(a_processar)}")
    return a_processar, a_manter

# ─────────────────────────────────────────────────────────────────
#  EXTRAÇÃO DE LOTES
# ─────────────────────────────────────────────────────────────────
async def extrair_lotes(page):
    lotes = []
    await page.locator('#ProcessViewInfo', has_text='LOTES DO PROCESSO').wait_for(timeout=10000)
    await page.wait_for_timeout(1200)
    total_lotes = await page.evaluate(JS_TOTAL_LOTES)
    print(f"      → {total_lotes} lote(s)")
    lote1 = await page.evaluate(JS_LER_LOTE)
    if lote1:
        lotes.append(lote1)
    for i in range(1, total_lotes):
        try:
            clicou = await page.evaluate("""
                (idx) => {
                    const area = document.getElementById('ProcessViewInfo');
                    const menu = area.querySelector(
                        'table.table-hover.table-defaultsys.table-striped.table-bordered'
                    );
                    if (!menu) return false;
                    const linhas = Array.from(menu.querySelectorAll('tbody tr'))
                        .filter(tr => /^\\d+/.test(tr.innerText.trim()));
                    if (linhas[idx]) { linhas[idx].click(); return true; }
                    return false;
                }
            """, i)
            if not clicou:
                break
            await page.wait_for_timeout(1500)
            lote = await page.evaluate(JS_LER_LOTE)
            if lote:
                lotes.append(lote)
        except Exception as e:
            print(f"      ⚠️  Erro lote {i + 1}: {e}")
    return lotes

# ─────────────────────────────────────────────────────────────────
#  EXTRAÇÃO DE UM PROCESSO
# ─────────────────────────────────────────────────────────────────
async def extrair_processo(context, url):
    info  = {}
    lotes = []
    page  = await context.new_page()
    try:
        await page.goto(url, wait_until='networkidle', timeout=30000)
        await page.wait_for_timeout(1500)
        campos = {
            'Organization':          '#Organization',
            'Number':                '#Number',
            'AdmNumber':             '#AdmNumber',
            'Modality':              '#Modality',
            'Status':                '#Status',
            'Conductor':             '#Conductor',
            'Authority':             '#Authority',
            'ContractKind':          '#ContractKind',
            'PublicationTime':       '#PublicationTime',
            'ProposalReceivingStart':'#ProposalReceivingStart',
            'ProposalAnalysisStart': '#ProposalAnalysisStart',
            'DisputeStart':          '#DisputeStart',
            'ProductOrService':      '#ProductOrService',
        }
        for chave, seletor in campos.items():
            try:
                el = page.locator(seletor).first
                try:
                    val = await el.input_value()
                except:
                    val = await el.inner_text()
                info[chave] = val.strip()
            except:
                info[chave] = ''

        # Valida promotor — rejeita processo de outro município
        org = info.get('Organization', '').upper().strip()
        if org and PROMOTOR.upper() not in org and org not in PROMOTOR.upper():
            print(f"\n      ❌ Promotor divergente: {info.get('Organization','')!r}")
            return {}, []
        btn = page.locator('button', has_text='Lotes')
        await btn.wait_for(state='visible', timeout=5000)
        await btn.click()
        lotes = await extrair_lotes(page)
    except Exception as e:
        print(f"\n      ⚠️  Erro: {e}")
    finally:
        await page.close()
    return info, lotes

# ─────────────────────────────────────────────────────────────────
#  MONTAGEM DO OBJETO JSON DE UM PROCESSO
# ─────────────────────────────────────────────────────────────────
def montar_processo_json(info, lotes, link):
    lotes_json = []
    for lote in lotes:
        itens_json = [
            {
                "numero":          item.get("Nº", ""),
                "especificacao":   item.get("Especificação", ""),
                "unidade":         item.get("Unidade", ""),
                "quantidade":      item.get("Quant.", ""),
                "valor_referencia":item.get("Val. Ref.", ""),
            }
            for item in lote.get("itens", [])
        ]
        lotes_json.append({
            "numero":          lote.get("numero", ""),
            "titulo":          lote.get("titulo", ""),
            "fase":            lote.get("fase", ""),
            "tipo":            lote.get("tipo", ""),
            "quantidade":      lote.get("quantidade", ""),
            "intervalo_minimo":lote.get("intervalo", ""),
            "exclusivo_me":    lote.get("exclusivo", ""),
            "local_entrega":   lote.get("entrega", ""),
            "garantia":        lote.get("garantia", ""),
            "valor_referencia":lote.get("valor_ref", ""),
            "vencedor":        lote.get("vencedor", ""),
            "melhor_oferta":   lote.get("melhor_oferta", ""),
            "total_itens":     len(itens_json),
            "itens":           itens_json,
        })
    total_itens = sum(len(l.get("itens", [])) for l in lotes)
    return {
        "id":             info.get("Number", ""),
        "promotor":       info.get("Organization", ""),
        "numero_adm":     info.get("AdmNumber", ""),
        "modalidade":     info.get("Modality", ""),
        "situacao":       info.get("Status", ""),
        "condutor":       info.get("Conductor", ""),
        "autoridade":     info.get("Authority", ""),
        "tipo_contrato":  info.get("ContractKind", ""),
        "publicacao":     info.get("PublicationTime", ""),
        "inicio_recepcao":info.get("ProposalReceivingStart", ""),
        "fim_recepcao":   info.get("ProposalAnalysisStart", ""),
        "inicio_disputa": info.get("DisputeStart", ""),
        "objeto":         info.get("ProductOrService", ""),
        "link":           link,
        "total_lotes":    len(lotes_json),
        "total_itens":    total_itens,
        "lotes":          lotes_json,
    }

# ─────────────────────────────────────────────────────────────────
#  SALVAMENTO DO JSON
# ─────────────────────────────────────────────────────────────────
def salvar_json(processos_final, ts):
    total_lotes = sum(p.get('total_lotes', 0) for p in processos_final)
    total_itens = sum(p.get('total_itens', 0) for p in processos_final)
    saida = {
        "metadata": {
            "promotor":        PROMOTOR,
            "total_processos": len(processos_final),
            "total_lotes":     total_lotes,
            "total_itens":     total_itens,
            "atualizado_em":   ts,
        },
        "processos": processos_final,
    }
    with open(ARQUIVO_JSON, 'w', encoding='utf-8') as f:
        json.dump(_clean_obj(saida), f, ensure_ascii=False, indent=2)
    print(f"\n💾 '{ARQUIVO_JSON}' salvo com sucesso!")
    print(f"   Processos: {len(processos_final)} | Lotes: {total_lotes} | Itens: {total_itens}")

# ─────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────
async def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ── Determina modo e inicia Xvfb se necessário ────────────────
    headless, descricao = determinar_modo_headless()
    print(f"\n🖥️  Modo browser: {descricao}")

    if not headless and _LINUX and not _TEM_DISPLAY and not _HEADLESS_ARG:
        xvfb_ok = iniciar_xvfb()
        if not xvfb_ok:
            print("   ⚠️  Xvfb não disponível — tentando headless puro.")
            headless = True

    try:
        # 0. Carrega JSON existente
        indice_existente, todos_existentes = carregar_json_existente()

        async with async_playwright() as p:
            # Argumentos extras para parecer browser real ao BLL
            args = [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-blink-features=AutomationControlled',
            ]
            browser = await p.chromium.launch(
                headless=headless,
                args=args,
            )
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent=(
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/122.0.0.0 Safari/537.36'
                ),
            )
            page = await context.new_page()

            # 1. Acessa a página de busca
            print("\n🌐 Acessando BLL Compras...")
            await page.goto(
                'https://bllcompras.com/Process/ProcessSearchPublic?param1=0#',
                wait_until='networkidle', timeout=35000
            )
            await page.wait_for_timeout(1500)

            # 2. Preenche filtros via JS e dispara ProcessSearch()
            #    HasButtonClick='True' é obrigatório para o filtro de Organization funcionar
            print(f"   🔍 Filtrando por: {PROMOTOR}")
            await page.evaluate("""
                (promotor) => {
                    const org = document.querySelector('input[name="Organization"]');
                    if (org) org.value = promotor;
                    const hbc = document.querySelector('#HasButtonClick');
                    if (hbc) hbc.value = 'True';
                    const status = document.querySelector('select[name="fkStatus"]');
                    if (status) status.value = '';
                }
            """, PROMOTOR)
            await page.wait_for_timeout(400)

            # Valida que o campo foi preenchido antes de buscar
            val = await page.input_value('input[name="Organization"]')
            if PROMOTOR.upper() not in val.upper():
                print(f"   ⚠️  Campo não preenchido corretamente: {val!r}")
                await browser.close()
                return

            # Dispara a busca chamando ProcessSearch() diretamente
            await page.evaluate("() => { ProcessSearch(); }")
            try:
                await page.wait_for_load_state('networkidle', timeout=25000)
            except Exception:
                pass
            await page.wait_for_timeout(4000)

            # Fecha modal de erro que possa ter surgido
            await page.evaluate("""
                () => {
                    document.querySelectorAll('.modal.show, .modal.fade.show').forEach(m => {
                        m.classList.remove('show'); m.style.display = 'none';
                    });
                    document.querySelectorAll('.modal-backdrop').forEach(b => b.remove());
                    document.body.classList.remove('modal-open');
                }
            """)
            await page.wait_for_timeout(500)

            # Valida que os resultados são do promotor correto
            n_rows = await page.locator('#tableProcessDataBody tr').count()
            if n_rows == 0:
                print("   ❌ Nenhum resultado retornado — verifique o nome do promotor.")
                await browser.close()
                return

            amostra = await page.evaluate("""
                () => Array.from(document.querySelectorAll('#tableProcessDataBody tr'))
                    .slice(0, 5)
                    .map(tr => Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim()))
                    .filter(c => c.length > 0)
            """)
            promotor_norm = PROMOTOR.upper()
            hit = any(
                promotor_norm in cel.upper()
                for row in amostra for cel in row if cel
            )
            if not hit:
                print(f"   ❌ Resultados não pertencem ao promotor '{PROMOTOR}'!")
                print(f"      Amostra: {amostra[:2]}")
                await browser.close()
                return

            print(f"   ✅ {n_rows} resultado(s) — promotor confirmado.")

            # 3. Scroll para carregar todos
            await scroll_para_carregar_todos(page)

            # 4. Coleta links e situações
            registros_bll = await coletar_links_e_situacoes(page, LIMITE_PROCESSOS)

            # 5. Decide o que processar
            a_processar, a_manter = decidir_o_que_processar(
                registros_bll, indice_existente, todos_existentes
            )

            if not a_processar:
                print("\n✅ Nenhuma alteração detectada. O dados.json já está atualizado!")
                await browser.close()
                return

            # 6. Extrai apenas o necessário
            total = len(a_processar)
            print(f"\n🔍 Extraindo {total} processo(s)...\n")
            novos_extraidos = []
            for i, reg in enumerate(a_processar):
                print(f"   [{i+1:>3}/{total}] {reg['motivo'][:45]:45s} ", end='')
                try:
                    info, lotes = await extrair_processo(context, reg['link'])
                    if not info.get('Number'):
                        print(f"❌ Processo rejeitado (promotor divergente ou página vazia)")
                        continue
                    proc_json = montar_processo_json(info, lotes, reg['link'])
                    novos_extraidos.append(proc_json)
                    ti = sum(len(l.get('itens', [])) for l in lotes)
                    print(f"✅ {info.get('Number','?'):20s} | {len(lotes)} lote(s) | {ti} item(ns)")
                except Exception as e:
                    print(f"❌ Erro: {e}")
                await asyncio.sleep(0.4)

            await browser.close()

        # 7. Monta lista final e salva
        processos_final = list(a_manter) + novos_extraidos
        print(f"\n{'─'*55}")
        salvar_json(processos_final, ts)
        print(f"\n🎉 Atualização concluída!")
        print(f"   🆕 Novos/atualizados: {len(novos_extraidos)}")
        print(f"   ✅ Mantidos:          {len(a_manter)}")
        print(f"   📦 Total no arquivo:  {len(processos_final)}")

    finally:
        encerrar_xvfb()


if __name__ == "__main__":
    asyncio.run(main())
