import asyncio
import json
import os
import re
from collections import defaultdict
from datetime import datetime
from playwright.async_api import async_playwright

# ─────────────────────────────────────────────────────────────────
#  CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────────
PROMOTOR         = 'MUNICIPIO DE TEIXEIRA DE FREITAS'
LIMITE_REGISTROS = 700    # reduza para testar (ex: 15)
HEADLESS         = True
ARQUIVO_JSON     = 'dados_compra_direta.json'

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
#  CARREGAMENTO DO JSON EXISTENTE
# ─────────────────────────────────────────────────────────────────
def carregar_json_existente():
    if not os.path.exists(ARQUIVO_JSON):
        print(f"📂 '{ARQUIVO_JSON}' não encontrado — será criado do zero.")
        return {}, []
    try:
        with open(ARQUIVO_JSON, 'r', encoding='utf-8') as f:
            dados = json.load(f)
        registros = dados.get('registros', [])
        indice = defaultdict(list)
        for r in registros:
            pid = r.get('id', '').strip()
            if pid:
                indice[pid].append(r)
        total = sum(len(v) for v in indice.values())
        print(f"📂 '{ARQUIVO_JSON}' carregado — {total} registro(s) em {len(indice)} id(s) únicos.")
        return indice, registros
    except Exception as e:
        print(f"⚠️  Erro ao ler '{ARQUIVO_JSON}': {e} — será recriado do zero.")
        return {}, []

# ─────────────────────────────────────────────────────────────────
#  SCROLL
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
#  COLETA DE LINKS + STATUS
# ─────────────────────────────────────────────────────────────────
async def coletar_links_e_status(page, limite):
    await page.wait_for_selector('#tableProcessDataBody tr', timeout=15000)
    total_disp = await page.locator('#tableProcessDataBody tr').count()
    total      = min(limite, total_disp)
    print(f"\n📋 {total} registro(s) encontrado(s) na listagem (de {total_disp} disponíveis)")

    dados_js = await page.evaluate(f"""
        () => {{
            const linhas = Array.from(
                document.querySelectorAll('#tableProcessDataBody tr')
            ).slice(0, {total});
            return linhas.map(tr => {{
                const tds  = tr.querySelectorAll('td');
                const link = tds[0] ? tds[0].querySelector('a') : null;
                return {{
                    href:   link ? link.getAttribute('href') : '',
                    numero: tds[2] ? tds[2].innerText.trim() : '',
                    status: tds[3] ? tds[3].innerText.trim() : '',
                }};
            }}).filter(r => r.href);
        }}
    """)

    if not dados_js:
        print("   ⚠️  Nenhum registro lido da tabela!")
        return []

    print("   Amostra (3 primeiros):")
    for r in dados_js[:3]:
        print(f"     id={r['numero']!r:25s}  status={r['status']!r}")

    return [
        {
            'link':   f"https://bllcompras.com{r['href']}",
            'id':     r['numero'],
            'status': r['status'],
        }
        for r in dados_js
    ]

# ─────────────────────────────────────────────────────────────────
#  LÓGICA INCREMENTAL
# ─────────────────────────────────────────────────────────────────
def decidir_o_que_processar(registros_bll, indice_existente, todos_existentes):
    a_processar   = []
    links_remover = set()
    sem_id = novos = atualizados = ignorados = 0

    for reg in registros_bll:
        pid    = reg.get('id', '').strip()
        st_bll = (reg.get('status') or '').strip().upper()

        if not pid:
            a_processar.append({'link': reg['link'], 'motivo': 'SEM ID'})
            sem_id += 1
            continue

        entradas = indice_existente.get(pid, [])
        if not entradas:
            a_processar.append({'link': reg['link'], 'motivo': f'NOVO ({pid})'})
            novos += 1
        else:
            stats_json = {e.get('status', '').strip().upper() for e in entradas}
            if st_bll and st_bll not in stats_json:
                for e in entradas:
                    if e.get('status', '').strip().upper() != st_bll:
                        links_remover.add(e.get('link', ''))
                a_processar.append({
                    'link':   reg['link'],
                    'motivo': f'ATUALIZADO {pid} ({" / ".join(stats_json)} → {st_bll})'
                })
                atualizados += 1
            else:
                ignorados += 1

    a_manter = [p for p in todos_existentes if p.get('link', '') not in links_remover]

    print(f"\n📊 Resumo da comparação:")
    print(f"   🆕 Novos:        {novos}")
    print(f"   🔄 Atualizados:  {atualizados}  ({len(links_remover)} versão(ões) antiga(s) removida(s))")
    print(f"   ✅ Sem mudança:  {ignorados}")
    if sem_id:
        print(f"   ⚠️  Sem ID:       {sem_id}")
    print(f"   🔍 A extrair:    {len(a_processar)}")
    return a_processar, a_manter

# ─────────────────────────────────────────────────────────────────
#  PARSE DE ITENS DO HTML RETORNADO PELO ENDPOINT
#  O HTML dos itens tem uma tabela com as colunas dos itens.
#  Usa BeautifulSoup-free: lê via regex / page.evaluate em div injetada.
# ─────────────────────────────────────────────────────────────────
async def parse_itens_html(page, html_itens):
    """
    Injeta o HTML numa div temporária e lê os campos com os IDs reais do BLL.
    A navegação lateral usa <tr onclick="GetDirectBuyData('[gkz]...', this)">
    O endpoint de cada item extra é /DirectBuy/DirectBuyItemData?param1=TOKEN
    """
    resultado = await page.evaluate("""
        (html) => {
            const tmp = document.createElement('div');
            tmp.style.display = 'none';
            tmp.innerHTML = html;
            document.body.appendChild(tmp);

            const v = (area, ...sels) => {
                for (const s of sels) {
                    const el = area.querySelector(s);
                    if (el) return (el.value !== undefined ? el.value : el.innerText || '').trim();
                }
                return '';
            };

            // Lê o item exibido no HTML (item atual — sempre 1 por resposta)
            const item = {
                numero:     v(tmp, '#directBuyItemData_Number',
                                   'input[name="directBuyItemData.Number"]'),
                quantidade: v(tmp, '#directBuyItemData_Quantity',
                                   'input[name="directBuyItemData.Quantity"]'),
                unidade:    v(tmp, '#directBuyItemData_Unity',
                                   'input[name="directBuyItemData.Unity"]'),
                descricao:  v(tmp, '#directBuyItemData_Description',
                                   'textarea[name="directBuyItemData.Description"]',
                                   'input[name="directBuyItemData.Description"]'),
                fornecedor: v(tmp, '#itemProvider_PersonName',
                                   'input[name="itemProvider.PersonName"]'),
                marca:      v(tmp, '#itemProvider_Brand',
                                   'input[name="itemProvider.Brand"]'),
                modelo:     v(tmp, '#itemProvider_Model',
                                   'input[name="itemProvider.Model"]'),
                valor:      v(tmp, '#itemProvider_Value',
                                   'input[name="itemProvider.Value"]'),
            };

            // Navegação lateral: <tr onclick="GetDirectBuyData('[gkz]...', this)">
            const tokens_nav = [];
            tmp.querySelectorAll('tr[onclick*="GetDirectBuyData"]').forEach(tr => {
                const oc = tr.getAttribute('onclick') || '';
                const m  = oc.match(/GetDirectBuyData\\(('([^']+)'|"([^"]+)")/);
                const tk = m ? (m[2] || m[3] || '') : '';
                if (tk && !tokens_nav.includes(tk)) tokens_nav.push(tk);
            });

            document.body.removeChild(tmp);
            return { item, tokens_nav };
        }
    """, html_itens)
    return resultado


# ─────────────────────────────────────────────────────────────────
#  EXTRAÇÃO DE ITENS — POST para cada item via token
# ─────────────────────────────────────────────────────────────────
async def _get_item_by_token(page, token):
    """
    Chama o endpoint de GetDirectBuyData para buscar um item individual.
    Tenta /DirectBuy/DirectBuyItemViewByItem e /DirectBuy/DirectBuyItemView.
    Retorna o dict do item ou None.
    """
    from urllib.parse import quote
    q = quote(token, safe='')
    endpoints = [
        f"https://bllcompras.com/DirectBuy/DirectBuyItemViewByItem?param1={q}",
        f"https://bllcompras.com/DirectBuy/DirectBuyItemView?param1={q}",
    ]
    for url in endpoints:
        try:
            resp = await page.request.post(
                url,
                headers={"Content-Type": "application/json;charset=utf-8"},
            )
            if resp.status != 200:
                continue
            body = await resp.json()
            html = body.get('html', '')
            if not html:
                continue
            res  = await parse_itens_html(page, html)
            item = res.get('item', {})
            if any(item.get(k, '') for k in ('numero', 'descricao', 'fornecedor')):
                return item
        except Exception:
            continue
    return None


async def _get_item_by_token(page, token):
    """POST para /DirectBuy/DirectBuyItemData (endpoint de GetDirectBuyData)."""
    from urllib.parse import quote
    url = f"https://bllcompras.com/DirectBuy/DirectBuyItemData?param1={quote(token, safe='')}"
    try:
        resp = await page.request.post(url, headers={"Content-Type": "application/json;charset=utf-8"})
        if resp.status != 200:
            return None
        body = await resp.json()
        html = body.get('html', '')
        if not html:
            return None
        res  = await parse_itens_html(page, html)
        item = res.get('item', {})
        return item if any(item.get(k, '') for k in ('numero', 'descricao', 'fornecedor')) else None
    except Exception:
        return None


async def extrair_itens(page, token_itens):
    """
    1. POST /DirectBuy/DirectBuyItemView?param1=TOKEN → HTML com item 1 já visível
       + <tr onclick="GetDirectBuyData(token, this)"> na nav lateral p/ todos os itens
    2. tokens_nav[0] = item 1 (já no HTML) → pular
    3. tokens_nav[1..n] → POST /DirectBuy/DirectBuyItemData para cada um
    """
    itens = []
    if not token_itens:
        return itens

    try:
        from urllib.parse import quote
        url  = f"https://bllcompras.com/DirectBuy/DirectBuyItemView?param1={quote(token_itens, safe='')}"
        resp = await page.request.post(url, headers={"Content-Type": "application/json;charset=utf-8"})
        if resp.status != 200:
            return itens
        body     = await resp.json()
        html_raw = body.get('html', '')
        if not html_raw:
            return itens

        res        = await parse_itens_html(page, html_raw)
        item0      = res.get('item', {})
        tokens_nav = res.get('tokens_nav', [])

        if any(item0.get(k, '') for k in ('numero', 'descricao', 'fornecedor')):
            itens.append(item0)

        for tk in tokens_nav[1:]:
            item_extra = await _get_item_by_token(page, tk)
            if item_extra:
                itens.append(item_extra)

    except Exception as e:
        print(f"\n         ⚠️  Erro itens: {e}")

    return itens

# ─────────────────────────────────────────────────────────────────
#  EXTRAÇÃO DE UMA COMPRA DIRETA
# ─────────────────────────────────────────────────────────────────
async def extrair_compra_direta(context, url):
    info        = {}
    itens       = []
    token_itens = ''
    page        = await context.new_page()
    try:
        await page.goto(url, wait_until='networkidle', timeout=30000)
        await page.wait_for_timeout(1000)

        # Lê campos da aba Informações (já carregada no HTML inicial)
        campos = {
            'OrganizationName': '#OrganizationName',
            'Number':           '#Number',
            'AdmNumber':        '#AdmNumber',
            'ModalityName':     '#ModalityName',
            'StatusName':       '#StatusName',
            'ConductorName':    '#ConductorName',
            'AuthorityName':    '#AuthorityName',
            'YearReference':    '#YearReference',
            'PublicationTime':  '#PublicationTime',
            'ConclusionTime':   '#ConclusionTime',
            'LawArticle':       '#LawArticle',
            'LawIdent':         '#LawIdent',
            'Justificatory':    '#Justificatory',
            'Legislation':      '#Legislation',
            'ProductOrService': '#ProductOrService',
            'Observation':      '#Observation',
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

        # Extrai o token do botão Itens via JS (passado como argumento — sem interpolação)
        token_itens = await page.evaluate("""
            () => {
                const btns = Array.from(document.querySelectorAll('button'));
                for (const btn of btns) {
                    const oc = btn.getAttribute('onclick') || '';
                    const m  = oc.match(/GetItemsInfo\\(('([^']+)'|"([^"]+)")\\)/);
                    if (m) return m[2] || m[3] || '';
                }
                return '';
            }
        """)

        # Extrai itens via POST (sem clicar no botão)
        itens = await extrair_itens(page, token_itens)

    except Exception as e:
        print(f"\n      ⚠️  Erro: {e}")
    finally:
        await page.close()
    return info, itens

# ─────────────────────────────────────────────────────────────────
#  MONTAGEM DO OBJETO JSON
# ─────────────────────────────────────────────────────────────────
def montar_json(info, itens, link):
    itens_json = [
        {
            'numero':     i.get('numero', ''),
            'descricao':  i.get('descricao', ''),
            'quantidade': i.get('quantidade', ''),
            'unidade':    i.get('unidade', ''),
            'fornecedor': i.get('fornecedor', ''),
            'marca':      i.get('marca', ''),
            'modelo':     i.get('modelo', ''),
            'valor':      i.get('valor', ''),
        }
        for i in itens
    ]
    return {
        'id':              info.get('Number', ''),
        'promotor':        info.get('OrganizationName', ''),
        'numero_adm':      info.get('AdmNumber', ''),
        'modalidade':      info.get('ModalityName', ''),
        'status':          info.get('StatusName', ''),
        'coordenador':     info.get('ConductorName', ''),
        'autoridade':      info.get('AuthorityName', ''),
        'ano_referencia':  info.get('YearReference', ''),
        'data_publicacao': info.get('PublicationTime', ''),
        'data_conclusao':  info.get('ConclusionTime', ''),
        'artigo':          info.get('LawArticle', ''),
        'inciso':          info.get('LawIdent', ''),
        'justificativa':   info.get('Justificatory', ''),
        'legislacao':      info.get('Legislation', ''),
        'objeto':          info.get('ProductOrService', ''),
        'observacao':      info.get('Observation', ''),
        'link':            link,
        'total_itens':     len(itens_json),
        'itens':           itens_json,
    }

# ─────────────────────────────────────────────────────────────────
#  SALVAMENTO DO JSON
# ─────────────────────────────────────────────────────────────────
def salvar_json(registros_final, ts):
    total_itens = sum(r.get('total_itens', 0) for r in registros_final)
    saida = {
        'metadata': {
            'promotor':        PROMOTOR,
            'total_registros': len(registros_final),
            'total_itens':     total_itens,
            'atualizado_em':   ts,
        },
        'registros': registros_final,
    }
    with open(ARQUIVO_JSON, 'w', encoding='utf-8') as f:
        json.dump(_clean_obj(saida), f, ensure_ascii=False, indent=2)
    print(f"\n💾 '{ARQUIVO_JSON}' salvo!")
    print(f"   Registros: {len(registros_final)} | Itens: {total_itens}")

# ─────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────
async def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    indice_existente, todos_existentes = carregar_json_existente()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await browser.new_context()
        page    = await context.new_page()

        print("\n🌐 Acessando BLL Compras — Compra Direta...")
        await page.goto(
            'https://bllcompras.com/DirectBuy/DirectBuySearchPublic',
            wait_until='networkidle', timeout=30000
        )
        await page.wait_for_timeout(2000)

        await page.fill('#Organization', PROMOTOR)
        await page.wait_for_timeout(400)

        await page.fill('#DateStart', '')
        await page.wait_for_timeout(300)
        print("   Campo 'PUBLIC. INICIO' limpo.")

        await page.click('#btnDirectBuySearch')
        await page.wait_for_load_state('networkidle', timeout=20000)
        await page.wait_for_timeout(3000)

        await scroll_para_carregar_todos(page)

        registros_bll = await coletar_links_e_status(page, LIMITE_REGISTROS)

        if not registros_bll:
            print("❌ Nenhum registro encontrado na listagem.")
            await browser.close()
            return

        a_processar, a_manter = decidir_o_que_processar(
            registros_bll, indice_existente, todos_existentes
        )

        if not a_processar:
            print("\n✅ Nenhuma alteração detectada. O arquivo já está atualizado!")
            await browser.close()
            return

        total = len(a_processar)
        print(f"\n🔍 Extraindo {total} registro(s)...\n")
        novos_extraidos = []
        for i, reg in enumerate(a_processar):
            print(f"   [{i+1:>3}/{total}] {reg['motivo'][:50]:50s} ", end='', flush=True)
            try:
                info, itens = await extrair_compra_direta(context, reg['link'])
                obj = montar_json(info, itens, reg['link'])
                novos_extraidos.append(obj)
                print(f"✅ {info.get('Number','?'):20s} | {len(itens)} item(ns)")
            except Exception as e:
                print(f"❌ {e}")
            await asyncio.sleep(0.4)

        await browser.close()

    registros_final = list(a_manter) + novos_extraidos

    print(f"\n{'─'*55}")
    salvar_json(registros_final, ts)
    print(f"\n🎉 Concluído!")
    print(f"   🆕 Novos/atualizados: {len(novos_extraidos)}")
    print(f"   ✅ Mantidos:          {len(a_manter)}")
    print(f"   📦 Total no arquivo:  {len(registros_final)}")


if __name__ == "__main__":
    asyncio.run(main())
