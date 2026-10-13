import calendar
import logging
from datetime import datetime, timezone

import psycopg2
import requests
from flask import Flask, Response, jsonify, request

app = Flask(__name__)
logger = logging.getLogger(__name__)

# ==============================================================================
# CONEXÃO COM O POSTGRESQL (NEON.TECH)
# ==============================================================================
# Cole abaixo a sua string de conexão completa do Neon (começa com postgresql://)
DATABASE_URL = "postgresql://neondb_owner:npg_MJQXi47YRVtw@ep-billowing-frog-b6gb0x6j-pooler.c-2.sa-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

_conn_global = None


def obter_conexao_banco():
  global _conn_global

  if _conn_global is not None:
    try:
      cur = _conn_global.cursor()
      cur.execute("SELECT 1")
      cur.close()
      return _conn_global
    except psycopg2.Error:
      logger.exception("Database connection error")
      try:
        _conn_global.close()
      except psycopg2.Error:
        logger.exception("Error closing connection")
      _conn_global = None

  _conn_global = psycopg2.connect(DATABASE_URL)
  _conn_global.autocommit = False
  return _conn_global


COLUNAS_VALIDAS = [
    "temperatura",
    "umidade_ar",
    "chuva_analogico",
    "chuva_estado",
    "gas_analogico",
    "gas_estado",
    "solo_analogico",
    "luminosidade_lux",
    "chama_estado",
    "mq135_analogico",
]


# ==============================================================================
# ROTAS DA API (JSON)
# ==============================================================================


@app.route("/api/leituras/ultima", methods=["GET"])
def obter_ultima_leitura():
  try:
    conn = obter_conexao_banco()
    cursor = conn.cursor()
    cursor.execute("""
            SELECT 
                id,
                TO_CHAR(data_hora, 'YYYY-MM-DD HH24:MI:SS') AS data_hora,
                temperatura, umidade_ar, chuva_analogico, chuva_estado,
                gas_analogico, gas_estado, solo_analogico, luminosidade_lux,
                chama_estado, mq135_analogico
            FROM leituras
            ORDER BY id DESC
            LIMIT 1;
        """)
    row = cursor.fetchone()
    cursor.close()

    if row:
      return (
          jsonify({
              "id": row[0],
              "data_hora": row[1],
              "temperatura": row[2],
              "umidade_ar": row[3],
              "chuva_analogico": row[4],
              "chuva_estado": row[5],
              "gas_analogico": row[6],
              "gas_estado": row[7],
              "solo_analogico": row[8],
              "luminosidade_lux": row[9],
              "chama_estado": row[10],
              "mq135_analogico": row[11],
          }),
          200,
      )
    return jsonify({"erro": "Nenhum dado encontrado"}), 404

  except psycopg2.Error as e:
    print(f"Erro em /api/leituras/ultima: {e}")
    return jsonify({"erro": str(e)}), 500


@app.route("/api/leituras/historico/<sensor>", methods=["GET"])
def obter_historico_sensor(sensor):
  if sensor not in COLUNAS_VALIDAS:
    return jsonify({"erro": "Sensor inválido"}), 400
  try:
    conn = obter_conexao_banco()
    cursor = conn.cursor()
    cursor.execute(f"""
            SELECT 
                id,
                TO_CHAR(data_hora, 'YYYY-MM-DD HH24:MI:SS') AS data_hora,
                {sensor}
            FROM leituras
            ORDER BY id DESC
            LIMIT 50;
        """)
    rows = cursor.fetchall()
    cursor.close()
    return (
        jsonify([{"id": r[0], "data_hora": r[1], "valor": r[2]} for r in rows]),
        200,
    )

  except psycopg2.Error as e:
    print(f"Erro em /api/leituras/historico/{sensor}: {e}")
    return jsonify({"erro": str(e)}), 500


@app.route("/api/leituras/medias/<sensor>", methods=["GET"])
def obter_medias(sensor):
  if sensor not in COLUNAS_VALIDAS:
    return jsonify({"erro": "Sensor inválido"}), 400
  try:
    conn = obter_conexao_banco()
    cursor = conn.cursor()
    cursor.execute(
        f"""
            SELECT
                (SELECT AVG(CAST({sensor} AS FLOAT)) FROM leituras WHERE CAST(data_hora AS DATE) = CURRENT_DATE),
                (SELECT AVG(CAST({sensor} AS FLOAT)) FROM leituras WHERE EXTRACT(MONTH FROM data_hora) = EXTRACT(MONTH FROM CURRENT_DATE) AND EXTRACT(YEAR FROM data_hora) = EXTRACT(YEAR FROM CURRENT_DATE)),
                (SELECT AVG(CAST({sensor} AS FLOAT)) FROM leituras WHERE EXTRACT(YEAR FROM data_hora) = EXTRACT(YEAR FROM CURRENT_DATE));
        """
    )
    row = cursor.fetchone()
    cursor.close()
    return (
        jsonify({
            "dia": round(row[0], 2)
            if row and row[0] is not None
            else None,
            "mes": round(row[1], 2)
            if row and row[1] is not None
            else None,
            "ano": round(row[2], 2)
            if row and row[2] is not None
            else None,
        }),
        200,
    )

  except (psycopg2.Error, TypeError, ValueError) as e:
    print(f"Erro ao calcular médias de {sensor}: {e}")
    return jsonify({"dia": None, "mes": None, "ano": None}), 500


@app.route("/api/leituras", methods=["POST"])
def receber_leitura_esp32():
  try:
    dados = request.get_json(silent=True, force=True)
    if dados is None:
      print("⚠️ POST sem JSON. Content-Type:", request.content_type)
      return jsonify({"erro": "JSON inválido ou vazio"}), 400

    campos = [
        "temperatura",
        "umidade_ar",
        "chuva_analogico",
        "chuva_estado",
        "gas_analogico",
        "gas_estado",
        "solo_analogico",
        "luminosidade_lux",
        "chama_estado",
        "mq135_analogico",
    ]
    valores = [dados.get(c) for c in campos]

    conn = obter_conexao_banco()
    cursor = conn.cursor()
    cursor.execute(
        """
            INSERT INTO leituras (
                data_hora, temperatura, umidade_ar, chuva_analogico, chuva_estado,
                gas_analogico, gas_estado, solo_analogico, luminosidade_lux,
                chama_estado, mq135_analogico
            ) VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """,
        tuple(valores),
    )
    conn.commit()
    cursor.close()
    return jsonify({"status": "sucesso"}), 201

  except (psycopg2.Error, TypeError, ValueError) as e:
    print(f"Erro ao inserir leitura: {e}")
    return jsonify({"erro": str(e)}), 500


# ==============================================================================
# CLIMA - COMPARAÇÃO ENTRE CIDADES (Open-Meteo)
# ==============================================================================
_GEO_URL = "https://geocoding-api.open-meteo.com/v1/search"
_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
_cache_clima = {}


def _geocodificar(nome_cidade):
  r = requests.get(
      _GEO_URL,
      params={"name": nome_cidade, "count": 1, "language": "pt", "format": "json"},
      timeout=8,
  )
  r.raise_for_status()
  dados = r.json()
  if not dados.get("results"):
    raise ValueError(f"Cidade não encontrada: {nome_cidade}")
  item = dados["results"][0]
  return {
      "nome": item.get("name"),
      "pais": item.get("country"),
      "lat": item["latitude"],
      "lon": item["longitude"],
  }


def _clima_cidade(nome_cidade, ano, mes_ini, mes_fim):
  geo = _geocodificar(nome_cidade)

  hoje = datetime.now(tz=timezone.utc).date()
  usar_forecast = ano >= hoje.year
  url = _FORECAST_URL if usar_forecast else _ARCHIVE_URL

  dia_ini = f"{ano}-{mes_ini:02d}-01"
  ultimo_dia = calendar.monthrange(ano, mes_fim)[1]
  dia_fim = f"{ano}-{mes_fim:02d}-{ultimo_dia:02d}"

  if usar_forecast:
    hoje_str = hoje.strftime("%Y-%m-%d")
    if dia_ini > hoje_str:
      url = _ARCHIVE_URL
      dia_ini = f"{ano - 1}-{mes_ini:02d}-01"
      ultimo_dia = calendar.monthrange(ano - 1, mes_fim)[1]
      dia_fim = f"{ano - 1}-{mes_fim:02d}-{ultimo_dia:02d}"
    elif dia_fim > hoje_str:
      dia_fim = hoje_str

  params = {
      "latitude": geo["lat"],
      "longitude": geo["lon"],
      "start_date": dia_ini,
      "end_date": dia_fim,
      "daily": "temperature_2m_mean,temperature_2m_max,temperature_2m_min",
      "timezone": "auto",
  }
  r = requests.get(url, params=params, timeout=20)
  r.raise_for_status()
  d = r.json().get("daily", {})

  temps_medias = [t for t in (d.get("temperature_2m_mean") or []) if t is not None]
  temps_max = [t for t in (d.get("temperature_2m_max") or []) if t is not None]
  temps_min = [t for t in (d.get("temperature_2m_min") or []) if t is not None]

  if not temps_medias:
    raise ValueError(f"Sem dados para {nome_cidade} no período solicitado")

  return {
      "nome": geo["nome"],
      "pais": geo["pais"],
      "media": round(sum(temps_medias) / len(temps_medias), 1),
      "maxima": round(max(temps_max), 1) if temps_max else None,
      "minima": round(min(temps_min), 1) if temps_min else None,
      "dias": len(temps_medias),
  }


@app.route("/api/clima/comparar", methods=["GET"])
def comparar_clima():
  cidade1 = request.args.get("cidade1", "").strip()
  cidade2 = request.args.get("cidade2", "").strip()
  ano = request.args.get("ano", type=int)
  mes_ini = request.args.get("mes_ini", 1, type=int)
  mes_fim = request.args.get("mes_fim", mes_ini, type=int)

  if not cidade1 or not cidade2 or not ano:
    return (
        jsonify({"erro": "Parâmetros obrigatórios: cidade1, cidade2, ano"}),
        400,
    )

  chave_cache = f"{cidade1}|{cidade2}|{ano}|{mes_ini}|{mes_fim}"
  if chave_cache in _cache_clima:
    return jsonify(_cache_clima[chave_cache]), 200

  try:
    resultado = {
        "ano": ano,
        "periodo": f"{mes_ini:02d}/{ano} a {mes_fim:02d}/{ano}",
        "cidades": [
            _clima_cidade(cidade1, ano, mes_ini, mes_fim),
            _clima_cidade(cidade2, ano, mes_ini, mes_fim),
        ],
    }
    _cache_clima[chave_cache] = resultado
    return jsonify(resultado), 200
  except (requests.RequestException, ValueError, TypeError, KeyError) as e:
    print(f"Erro em /api/clima/comparar: {e}")
    return jsonify({"erro": str(e)}), 500


# ==============================================================================
# DASHBOARD WEB (HTML)
# ==============================================================================
PAGINA_HTML = r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<title>Monitoramento IoT - PostgreSQL</title>
<style>
  :root {
    --bg:#11111b; --panel:#1e1e2e; --border:#45475a; --header:#313244;
    --text:#cdd6f4; --accent:#89b4fa; --ok:#a6e3a1; --warn:#f9e2af; --err:#f38ba8;
  }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--text);
         font-family:'Segoe UI',Arial,sans-serif; }
  header { display:flex; justify-content:space-between; align-items:center;
           padding:10px 16px; background:var(--panel); border-bottom:1px solid var(--border); }
  header h1 { margin:0; font-size:15px; color:var(--accent); }
  #status { font-size:12px; color:var(--ok); font-weight:bold; }
  nav { display:flex; flex-wrap:wrap; gap:2px; background:var(--bg);
        padding:6px 8px; border-bottom:1px solid var(--border); }
  nav button { background:var(--panel); color:var(--text); border:1px solid var(--border);
               border-radius:4px; padding:6px 12px; font-size:11px; font-weight:bold;
               cursor:pointer; }
  nav button.active { background:var(--accent); color:#11111b; }
  main { padding:16px; }
  .cards { display:grid; grid-template-columns:repeat(3,1fr); gap:12px; }
  .card { background:var(--panel); border:1px solid var(--border); border-radius:8px;
          padding:12px; min-height:110px; display:flex; flex-direction:column; }
  .card h3 { margin:0 0 8px; font-size:12px; color:var(--accent); }
  .card .val { flex:1; display:flex; align-items:center; justify-content:center;
               font-size:17px; font-weight:bold; text-align:center; }
  table { width:100%; border-collapse:collapse; background:var(--panel);
          border-radius:8px; overflow:hidden; margin-top:10px; }
  th { background:var(--header); color:var(--text); padding:8px; text-align:left;
       font-size:12px; }
  td { padding:8px; border-top:1px solid var(--border); font-size:12px; }
  .medias { display:grid; grid-template-columns:repeat(3,1fr); gap:12px; margin-top:16px; }
  .medias .card { min-height:80px; }
  .medias .card .val { font-size:15px; color:var(--ok); }
  .row { display:flex; flex-wrap:wrap; gap:10px; align-items:center; margin-bottom:14px; }
  .row label { font-size:12px; }
  input, select, button.btn { background:var(--header); color:var(--text);
       border:1px solid var(--border); border-radius:4px; padding:6px 10px;
       font-size:12px; }
  button.btn { background:var(--accent); color:#11111b; font-weight:bold; cursor:pointer; }
  .clima-grid { display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-bottom:16px; }
  .clima-card { background:var(--panel); border:1px solid var(--border);
                border-radius:8px; padding:16px; white-space:pre-line; font-size:14px; }
  .muted { color:#7f849c; font-size:11px; }
</style>
</head>
<body>

<header>
  <h1>🌐 Sistema de Monitoramento IoT — PostgreSQL</h1>
  <span id="status">Carregando…</span>
</header>

<nav id="menu"></nav>

<main id="conteudo"></main>

<script>
const SENSORES = [
  ["temperatura","🌡️ Temperatura"],
  ["umidade_ar","💧 Umidade do Ar"],
  ["chuva_analogico","🌧️ Nível de Chuva"],
  ["chuva_estado","☔ Detector de Chuva"],
  ["gas_analogico","💨 Concentração de Gás"],
  ["gas_estado","⚠️ Alerta de Gás"],
  ["solo_analogico","🌱 Umidade do Solo"],
  ["luminosidade_lux","☀️ Luminosidade (Lux)"],
  ["chama_estado","🔥 Detector de Incêndio"],
  ["mq135_analogico","🍃 Qualidade do Ar"]
];
const API = "";

let abaAtual = "home";

function traduzir(chave, val) {
  if (val === null || val === undefined || val === "") return "Sem dados";
  const n = parseFloat(val);
  if (isNaN(n)) return String(val);
  switch (chave) {
    case "temperatura": return n.toFixed(1)+" °C";
    case "umidade_ar": return n.toFixed(1)+" %";
    case "chuva_analogico":
      if (n < 1500) return "Chuva Forte ("+Math.round(n)+")";
      if (n < 3000) return "Garoinha ("+Math.round(n)+")";
      return "Sem Chuva ("+Math.round(n)+")";
    case "chuva_estado": return n == 0 ? "🌧️ Chovendo Agora" : "☀️ Tempo Seco";
    case "gas_analogico":
      return n > 1500 ? "🚨 VAZAMENTO! ("+Math.round(n)+")" : "🟢 Nível Seguro ("+Math.round(n)+")";
    case "gas_estado": return n == 0 ? "🚨 ALERTA DE GÁS!" : "🟢 Ar Seguro";
    case "solo_analogico":
      if (n < 1500) return "🌱 Solo Muito Úmido ("+Math.round(n)+")";
      if (n < 3000) return "🌱 Solo Ideal ("+Math.round(n)+")";
      return "🌵 Solo Seco! ("+Math.round(n)+")";
    case "luminosidade_lux": return "☀️ "+Math.round(n)+" Lux";
    case "chama_estado": return n == 0 ? "🔥 ALERTA: FOGO DETECTADO!" : "🟢 Sem Fogo";
    case "mq135_analogico":
      return n > 1200 ? "🔴 Qualidade Ruim ("+Math.round(n)+")" : "🟢 Qualidade Boa ("+Math.round(n)+")";
  }
  return String(val);
}

function corrigirData(s) {
  if (!s) return "--";
  const d = new Date(s.replace(" ","T"));
  if (isNaN(d)) return s;
  const pad = x => String(x).padStart(2,"0");
  return pad(d.getDate())+"/"+pad(d.getMonth()+1)+"/"+d.getFullYear()+" "+
         pad(d.getHours())+":"+pad(d.getMinutes())+":"+pad(d.getSeconds());
}

function montarMenu() {
  const menu = document.getElementById("menu");
  menu.innerHTML = "";
  const home = document.createElement("button");
  home.textContent = "🏠 Home";
  home.onclick = () => { abaAtual="home"; montarMenu(); render(); };
  if (abaAtual==="home") home.classList.add("active");
  menu.appendChild(home);

  SENSORES.forEach(([k,t]) => {
    const b = document.createElement("button");
    b.textContent = t;
    if (abaAtual===k) b.classList.add("active");
    b.onclick = () => { abaAtual=k; montarMenu(); render(); };
    menu.appendChild(b);
  });

  const m = document.createElement("button");
  m.textContent = "🌍 Temp. Mundo";
  if (abaAtual==="mundo") m.classList.add("active");
  m.onclick = () => { abaAtual="mundo"; montarMenu(); render(); };
  menu.appendChild(m);
}

async function renderHome() {
  const c = document.getElementById("conteudo");
  c.innerHTML = '<div class="cards" id="cards"></div>';
  const g = document.getElementById("cards");
  SENSORES.forEach(([k,t]) => {
    const div = document.createElement("div");
    div.className="card";
    div.innerHTML = "<h3>"+t+"</h3><div class='val' id='card-"+k+"'>--</div>";
    g.appendChild(div);
  });
  await atualizarHome();
}

async function atualizarHome() {
  try {
    const r = await fetch(API+"/api/leituras/ultima");
    if (!r.ok) throw new Error("HTTP "+r.status);
    const d = await r.json();
    document.getElementById("status").textContent =
      "Última leitura: "+corrigirData(d.data_hora);
    SENSORES.forEach(([k]) => {
      const el = document.getElementById("card-"+k);
      if (el) el.textContent = traduzir(k, d[k]);
    });
  } catch(e) {
    document.getElementById("status").textContent = "Servidor indisponível";
  }
}

async function renderSensor(chave, titulo) {
  const c = document.getElementById("conteudo");
  c.innerHTML =
    '<h2 style="color:#89b4fa;font-size:14px">'+titulo+' — Leitura atual</h2>'+
    '<div class="card" style="min-height:90px"><div class="val" id="atual">--</div></div>'+
    '<h2 style="color:#89b4fa;font-size:14px;margin-top:16px">Histórico (50 últimos)</h2>'+
    '<table><thead><tr><th>ID</th><th>Data e Hora</th><th>Leitura</th></tr></thead>'+
    '<tbody id="hist"></tbody></table>'+
    '<h2 style="color:#89b4fa;font-size:14px;margin-top:16px">Médias</h2>'+
    '<div class="medias">'+
      '<div class="card"><h3>Média Diária</h3><div class="val" id="m-dia">--</div></div>'+
      '<div class="card"><h3>Média Mensal</h3><div class="val" id="m-mes">--</div></div>'+
      '<div class="card"><h3>Média Anual</h3><div class="val" id="m-ano">--</div></div>'+
    '</div>';

  try {
    const [ult, hist, med] = await Promise.all([
      fetch(API+"/api/leituras/ultima").then(r=>r.json()),
      fetch(API+"/api/leituras/historico/"+chave).then(r=>r.json()),
      fetch(API+"/api/leituras/medias/"+chave).then(r=>r.json()),
    ]);
    document.getElementById("atual").textContent = traduzir(chave, ult[chave]);
    const tb = document.getElementById("hist");
    tb.innerHTML = "";
    hist.forEach(item => {
      const tr = document.createElement("tr");
      tr.innerHTML = "<td>"+item.id+"</td><td>"+corrigirData(item.data_hora)+
                     "</td><td>"+traduzir(chave,item.valor)+"</td>";
      tb.appendChild(tr);
    });
    document.getElementById("m-dia").textContent = traduzir(chave, med.dia);
    document.getElementById("m-mes").textContent = traduzir(chave, med.mes);
    document.getElementById("m-ano").textContent = traduzir(chave, med.ano);
  } catch(e) {
    console.error(e);
  }
}

function renderMundo() {
  const c = document.getElementById("conteudo");
  c.innerHTML =
    '<div class="row">'+
      '<label>Cidade 1: <input id="c1" value="Araraquara"></label>'+
      '<label>Cidade 2: <input id="c2" value="Toronto"></label>'+
      '<label>Ano: <input id="ano" type="number" value="2025" style="width:80px"></label>'+
      '<label>Mês ini: <input id="mi" type="number" value="1" min="1" max="12" style="width:60px"></label>'+
      '<label>Mês fim: <input id="mf" type="number" value="3" min="1" max="12" style="width:60px"></label>'+
      '<button class="btn" id="btn">🌡️ Comparar</button>'+
    '</div>'+
    '<div class="clima-grid">'+
      '<div class="clima-card" id="rc1">--</div>'+
      '<div class="clima-card" id="rc2">--</div>'+
    '</div>'+
    '<table><thead><tr><th>Métrica</th><th>Cidade 1</th><th>Cidade 2</th><th>Diferença</th></tr></thead>'+
    '<tbody id="tab"></tbody></table>'+
    '<p class="muted" id="font">Fonte: Open-Meteo</p>';

  document.getElementById("btn").onclick = async () => {
    const c1 = document.getElementById("c1").value.trim();
    const c2 = document.getElementById("c2").value.trim();
    const ano = document.getElementById("ano").value;
    const mi = document.getElementById("mi").value;
    const mf = document.getElementById("mf").value;
    document.getElementById("font").textContent = "Consultando Open-Meteo…";
    try {
      const r = await fetch(API+"/api/clima/comparar?cidade1="+encodeURIComponent(c1)+
        "&cidade2="+encodeURIComponent(c2)+"&ano="+ano+"&mes_ini="+mi+"&mes_fim="+mf);
      const d = await r.json();
      if (!r.ok) { document.getElementById("font").textContent = "Erro: "+(d.erro||r.status); return; }
      const a=d.cidades[0], b=d.cidades[1];
      document.getElementById("rc1").textContent =
        "📍 "+a.nome+" ("+a.pais+")\n🌡️ Média: "+a.media+" °C\n🔺 Máx: "+a.maxima+
        " °C\n🔻 Mín: "+a.minima+" °C\n📅 "+a.dias+" dias";
      document.getElementById("rc2").textContent =
        "📍 "+b.nome+" ("+b.pais+")\n🌡️ Média: "+b.media+" °C\n🔺 Máx: "+b.maxima+
        " °C\n🔻 Mín: "+b.minima+" °C\n📅 "+b.dias+" dias";
      const linhas = [
        ["Média (°C)", a.media, b.media],
        ["Máxima (°C)", a.maxima, b.maxima],
        ["Mínima (°C)", a.minima, b.minima],
      ];
      const tb = document.getElementById("tab");
      tb.innerHTML = "";
      linhas.forEach(([m,va,vb]) => {
        const dif = (va!=null && vb!=null) ? (va-vb).toFixed(1) : "--";
        const sinal = (va!=null && vb!=null && dif>0) ? "+"+dif : dif;
        const tr = document.createElement("tr");
        tr.innerHTML = "<td>"+m+"</td><td>"+(va??"--")+"</td><td>"+(vb??"--")+"</td><td>"+sinal+"</td>";
        tb.appendChild(tr);
      });
      document.getElementById("font").textContent = "Período: "+d.periodo+" | Fonte: Open-Meteo";
    } catch(e) {
      document.getElementById("font").textContent = "Falha: "+e;
    }
  };
}

function render() {
  if (abaAtual==="home") renderHome();
  else if (abaAtual==="mundo") renderMundo();
  else {
    const s = SENSORES.find(x => x[0]===abaAtual);
    if (s) renderSensor(s[0], s[1]);
  }
}

setInterval(() => { if (abaAtual==="home") atualizarHome(); }, 5000);

montarMenu();
render();
</script>
</body>
</html>
"""


@app.route("/", methods=["GET"])
def dashboard_web():
  return Response(PAGINA_HTML, mimetype="text/html")


if __name__ == "__main__":
  print("🚀 Iniciando Servidor Flask IoT em http://localhost:5000 ...")
  app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
