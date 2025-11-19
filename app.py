import io
import csv
import os
import json
from pathlib import Path
from functools import wraps
from flask import Flask, render_template, request, jsonify, send_file, session, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta
import random
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill
from werkzeug.security import generate_password_hash, check_password_hash
import ssl
import tempfile
import os
import stat

# Ajustar permissões dos certificados SSL para PostgreSQL
import os
import stat

def ajustar_permissoes_certificados():
    """Ajusta permissões dos certificados para PostgreSQL"""
    certificados = {
        'private-key.key': stat.S_IRUSR | stat.S_IWUSR,  # 600
        'certificate.pem': stat.S_IRUSR | stat.S_IWUSR,  # 600  
        'ca-certificate.crt': stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH  # 644
    }
    
    for cert_file, perms in certificados.items():
        if os.path.exists(cert_file):
            try:
                os.chmod(cert_file, perms)
                print(f"✅ Permissões ajustadas para: {cert_file}")
            except Exception as e:
                print(f"⚠️ Não foi possível ajustar {cert_file}: {e}")

# Chamar antes de criar o app
ajustar_permissoes_certificados()

# CORREÇÃO DO FUSO HORÁRIO
def agora():
    """Retorna o horário atual de Brasília (UTC-3)"""
    return datetime.utcnow() - timedelta(hours=3)

# Configurações PostgreSQL
class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'sua-chave-secreta-super-segura-aqui-ro-experience-2025'
    
    # String de conexão PostgreSQL com pg8000 - SSL na própria URL
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
    'postgresql+pg8000://squarecloud:5W3Ww67llyHrBmcutvyL5xXO@square-cloud-db-4d0ca60ac1a54ad48adf5608996c6a48.squareweb.app:7091/postgre'
    
    SQLALCHEMY_ENGINE_OPTIONS = {
    'connect_args': {
        # O pg8000 espera os argumentos ssl_ca, ssl_cert e ssl_key para SSL.
        # Isso resolve o erro "unexpected keyword argument 'ssl'".
        'ssl_ca': 'ca-certificate.crt', 
        'ssl_cert': 'certificate.pem',
        'ssl_key': 'private-key.key'
    }
}
    SQLALCHEMY_TRACK_MODIFICATIONS = False

app = Flask(__name__)
app.config.from_object(Config)
db = SQLAlchemy(app)

# Salvar certificados em arquivos temporários
def salvar_certificados():
    """Salva os certificados em arquivos para uso na conexão SSL"""
    cert_content = """-----BEGIN CERTIFICATE-----
MIIDFTCCAf2gAwIBAgIUDkbsd4C4csezkwaszmtST/v9v0UwDQYJKoZIhvcNAQEL
BQAwGjEYMBYGA1UEAwwPKi5zcXVhcmV3ZWIuYXBwMB4XDTI1MTExOTEyMDkzOFoX
DTM1MTExNzEyMDkzOFowGjEYMBYGA1UEAwwPKi5zcXVhcmV3ZWIuYXBwMIIBIjAN
BgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA5LpvtB6KEfsfXP6AttAO3zXojsoe
3oGbOqfM8DxIycPfiZ6X2/XNpevrEpgUCXXKp3u1hsRTLlRL/WczG86C9FU8QLR8
cc88xWSvOTVmgskHphLBWQHr37qKWF5tPrpG5ZiZOk5AkzN+JGjKUBwnk7rCWQRo
IPqRyV2OWzb9+U/tpuwblvbW/wOaBT4rnr2WDn0Htkf8vkv4g0UmoACzNSnEWXJ8
ZKuChO0FQJOeuV9JQNOqV09xHgO7sLGYOFQI/zdN1etVBwQBiYro1VVwKwAP3eS1
dfL6jIn1akRr552rhZ6FAZwDwAfZd/8SU8EZfa99MLsI+jjXefb5NtaAEQIDAQAB
o1MwUTAdBgNVHQ4EFgQU+ZARvsnJZH067Igzon+RcZf/lVkwHwYDVR0jBBgwFoAU
+ZARvsnJZH067Igzon+RcZf/lVkwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0B
AQsFAAOCAQEAWsxHxEpg9WWv8shhxGxNUDw0QRcedbmRNzoL0B8bFfqfWs+kTEDm
Yd1WYAwD9dz5JjUFcZ1UaQ1SMAiCeTVxBIwnhryqdhCGx/JV5PWnYMmTT0Gz7lK4
pX9N2UyYHofSDOkhGZNnfAiDJfep/5TXhlksle5cI0o2NJTH56HRLUOZottbR6St
kWhTPS0KtZIeuaxMJt/2E2R3HwSdqwBfgpF/k5EAbUST19XF5DHO1hrpXzE0LO4L
kvPwAP+1PlH4oomZl5Hn/W4GF4tkuw3S+dLvkJqFCeyXvAReipiTVCbUSoAQUZG4
MxUaA1mEZVDyHCrHddLhA5KTXjtfk+Q0nA==
-----END CERTIFICATE-----"""
    
    key_content = """-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDkum+0HooR+x9c
/oC20A7fNeiOyh7egZs6p8zwPEjJw9+Jnpfb9c2l6+sSmBQJdcqne7WGxFMuVEv9
ZzMbzoL0VTxAtHxxzzzFZK85NWaCyQemEsFZAevfuopYXm0+ukblmJk6TkCTM34k
aMpQHCeTusJZBGgg+pHJXY5bNv35T+2m7BuW9tb/A5oFPiuevZYOfQe2R/y+S/iD
RSagALM1KcRZcnxkq4KE7QVAk565X0lA06pXT3EeA7uwsZg4VAj/N03V61UHBAGJ
iujVVXArAA/d5LV18vqMifVqRGvnnauFnoUBnAPAB9l3/xJTwRl9r30wuwj6ONd5
9vk21oARAgMBAAECggEAFM/nBp1pxEmUmL5aoWfMlqBd3RJEnUvvPzbSBcECgYNm
ZWwOFthmojSVgu9eEV9LtZIi2hSmmfZXJMNdaegQT+GqlhumICfyeGaZMAwtG/Tl
UoXt3Ga5mvWrDW/oIldKSK1qMd05cHt0vKThVck+C6nocbeeLvQFzGwMHj4ifWsN
lLtSD9+Eg/uI50Dsk6yeKA8KpUNh+0i5v1yYIEqa5hBSgip6gGuJGhLeYfcZbaP/
Zy1sZekf3sc15gZKxlE2P1eEi0s5aIjx4nmYRG52sVt26MMAphGgTyklwsDu+J7h
JDUXx0e1aMNtTbzefPxlZmUfryuCG/BDIQ/aXu9yjQKBgQD+/lf5ilb4fVSbkjus
gpoEt870Hfd/h3kXiK3+YkGet4HXBXS3Ppo80niylJFE/G/EyYNotGpN6fZrlZoo
OW2o2YFXjO9HRHheihWHxsy4Uc0zSBV/iabGBbtejoqtoSDV9dl0MaTqhtQH1TNq
YjhF8tWZ3lmJtTdBqhoMhhSHlQKBgQDloY2bF3401ogjBg0TQXq2HGe/wwc1YAF9
ALb380ZrD9fgV6bPGhwnapHsviBR6h9JFj8SERKsfFTN+eCXwSrub2jYCd3VVtzi
a0mwFsx/qUhQpXuF8NYfLaIg2Vso4ML4TzROwSeyjDl+Uu2lhjGm5NHg0bvn2S7q
fMjC4tfHjQKBgDXIccfZbPSmgrT5iAqf1VqPZmBI/c3xRhI3qvNPyCVw8TroFLEd
zCxt/DU2btmNIQnxsvzfqNhcM7SHbmdzjXSiw8wylrmCcJ0IJPhpbv6lilF5r8ch
woJb7SRJjmiG5sUaQM5oNi+dVpV8W4bhxprCWFlG872+8jOwbCQeF7BtAoGAVuJ6
ru0DrsBhgEcA2YFUCeXTh6YmaxapWX/UuxqMUWQMPXFMx13Mu0lCb348wmHAdqn5
MT/Off8JI2phhWnscY0r8VpCWj9kkjMgDYWC2ObXWtRSWYqXqfJdPuqe6GeqvcNS
Ol4xRAh0lIySKKgOz2QS2WriG0Z8zvJqD/s+w90CgYAe2l7uvsiMHnmtPbrN6hni
Rqi5Icu3GubfU85/E45k58sTMnLBpdcdEclaYiFwI8oZJrAvpfDe78bv0vVmJFoR
cFwsUmlpFpi5rqvifLWyqngeLJ+qwKXikzOFey0f2Zb8sPcW85H/4/wY75mpmaFq
TrQKkt+AnNzucIkKjvX/Jg==
-----END PRIVATE KEY-----"""
    
    root_cert_content = """-----BEGIN CERTIFICATE-----
MIIDFTCCAf2gAwIBAgIUDkbsd4C4csezkwaszmtST/v9v0UwDQYJKoZIhvcNAQEL
BQAwGjEYMBYGA1UEAwwPKi5zcXVhcmV3ZWIuYXBwMB4XDTI1MTExOTEyMDkzOFoX
DTM1MTExNzEyMDkzOFowGjEYMBYGA1UEAwwPKi5zcXVhcmV3ZWIuYXBwMIIBIjAN
BgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA5LpvtB6KEfsfXP6AttAO3zXojsoe
3oGbOqfM8DxIycPfiZ6X2/XNpevrEpgUCXXKp3u1hsRTLlRL/WczG86C9FU8QLR8
cc88xWSvOTVmgskHphLBWQHr37qKWF5tPrpG5ZiZOk5AkzN+JGjKUBwnk7rCWQRo
IPqRyV2OWzb9+U/tpuwblvbW/wOaBT4rnr2WDn0Htkf8vkv4g0UmoACzNSnEWXJ8
ZKuChO0FQJOeuV9JQNOqV09xHgO7sLGYOFQI/zdN1etVBwQBiYro1VVwKwAP3eS1
dfL6jIn1akRr552rhZ6FAZwDwAfZd/8SU8EZfa99MLsI+jjXefb5NtaAEQIDAQAB
o1MwUTAdBgNVHQ4EFgQU+ZARvsnJZH067Igzon+RcZf/lVkwHwYDVR0jBBgwFoAU
+ZARvsnJZH067Igzon+RcZf/lVkwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0B
AQsFAAOCAQEAWsxHxEpg9WWv8shhxGxNUDw0QRcedbmRNzoL0B8bFfqfWs+kTEDm
Yd1WYAwD9dz5JjUFcZ1UaQ1SMAiCeTVxBIwnhryqdhCGx/JV5PWnYMmTT0Gz7lK4
pX9N2UyYHofSDOkhGZNnfAiDJfep/5TXhlksle5cI0o2NJTH56HRLUOZottbR6St
kWhTPS0KtZIeuaxMJt/2E2R3HwSdqwBfgpF/k5EAbUST19XF5DHO1hrpXzE0LO4L
kvPwAP+1PlH4oomZl5Hn/W4GF4tkuw3S+dLvkJqFCeyXvAReipiTVCbUSoAQUZG4
MxUaA1mEZVDyHCrHddLhA5KTXjtfk+Q0nA==
-----END CERTIFICATE-----"""
    
    # Salvar os certificados em arquivos
    with open('cert.crt', 'w') as f:
        f.write(cert_content)
    with open('private.key', 'w') as f:
        f.write(key_content)
    with open('cert.pem', 'w') as f:
        f.write(root_cert_content)

# Modelos
class Usuario(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(200), nullable=False)
    nome = db.Column(db.String(100), nullable=False)
    nivel_acesso = db.Column(db.String(20), default='operador')
    ativo = db.Column(db.Boolean, default=True)
    data_criacao = db.Column(db.DateTime, default=agora)
    permissoes = db.Column(db.Text, default='{}')

class LogAuditoria(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    usuario_id = db.Column(db.Integer, db.ForeignKey('usuario.id'))
    acao = db.Column(db.String(100), nullable=False)
    modulo = db.Column(db.String(50), nullable=False)
    dados = db.Column(db.Text)
    ip = db.Column(db.String(45))
    data_hora = db.Column(db.DateTime, default=agora)
    
    usuario = db.relationship('Usuario', backref='logs')

class Cliente(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    cnpj = db.Column(db.String(18), nullable=False)
    razao_social = db.Column(db.String(200), nullable=False)
    responsavel = db.Column(db.String(200), nullable=False, default='Não Informado')
    consultor = db.Column(db.String(200), nullable=False, default='Não Informado')
    checkin_realizado = db.Column(db.Boolean, default=False)
    horario_checkin = db.Column(db.DateTime)
    responsavel_checkin = db.Column(db.String(200))
    direito_imagem = db.Column(db.Boolean, default=False)
    veio_carro = db.Column(db.Boolean, default=False)
    placa_veiculo = db.Column(db.String(10))
    
    vendas = db.relationship('Venda', backref='cliente', lazy=True)

    __table_args__ = (db.UniqueConstraint('cnpj', 'responsavel', name='uq_cnpj_responsavel'),)

class Venda(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    cnpj_compra = db.Column(db.String(18), nullable=False)
    numero_pedido = db.Column(db.String(50), unique=True, nullable=False)
    valor_pedido = db.Column(db.Float, nullable=False)
    data_hora_venda = db.Column(db.DateTime, default=agora)
    cliente_id = db.Column(db.Integer, db.ForeignKey('cliente.id'))
    cnpj_checkin_vinculado = db.Column(db.String(18))
    
    equipamentos = db.relationship('VendaEquipamento', backref='venda', lazy=True)

class Brinde(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tipo_sorteio = db.Column(db.String(10), nullable=False)
    nome = db.Column(db.String(200), nullable=False)
    descricao = db.Column(db.Text)
    valor_aproximado = db.Column(db.Float)
    quantidade_total = db.Column(db.Integer, nullable=False, default=1)
    quantidade_disponivel = db.Column(db.Integer, nullable=False, default=1)
    ativo = db.Column(db.Boolean, default=True)

class Sorteio(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tipo_brinde = db.Column(db.String(50), nullable=False)
    cnpj_vencedor = db.Column(db.String(18), nullable=False)
    razao_social_vencedor = db.Column(db.String(200), nullable=False)
    responsavel_recebimento = db.Column(db.String(200), nullable=False)
    data_sorteio = db.Column(db.DateTime, default=agora)
    valor_acumulado_revenda = db.Column(db.Float, nullable=False)
    brinde_id = db.Column(db.Integer, db.ForeignKey('brinde.id'))
    brinde = db.relationship('Brinde', backref='sorteios')
    entregue = db.Column(db.Boolean, default=False)
    data_entrega = db.Column(db.DateTime)
    responsavel_entrega = db.Column(db.String(200))
    observacao_entrega = db.Column(db.Text)

class FaturamentoSorteio(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    cnpj = db.Column(db.String(18), nullable=False)
    faturamento_acumulado = db.Column(db.Float, nullable=False, default=0.0)
    ultima_atualizacao = db.Column(db.DateTime, default=agora)
    participacoes_utilizadas = db.Column(db.Integer, default=0)

class Estoque(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    fabricante = db.Column(db.String(100), nullable=False)
    modelo = db.Column(db.String(100), nullable=False)
    quantidade_total = db.Column(db.Integer, nullable=False, default=0)
    quantidade_disponivel = db.Column(db.Integer, nullable=False, default=0)
    data_cadastro = db.Column(db.DateTime, default=agora)
    ativo = db.Column(db.Boolean, default=True)
    
    vendas = db.relationship('VendaEquipamento', backref='equipamento', lazy=True)

class VendaEquipamento(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    venda_id = db.Column(db.Integer, db.ForeignKey('venda.id'))
    equipamento_id = db.Column(db.Integer, db.ForeignKey('estoque.id'))
    quantidade = db.Column(db.Integer, nullable=False, default=1)

class PesquisaResposta(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    cnpj_identificado = db.Column(db.String(18))
    razao_social = db.Column(db.String(200))
    organizacao = db.Column(db.Integer, nullable=False)
    palestras = db.Column(db.Integer, nullable=False)
    atendimento = db.Column(db.Integer, nullable=False)
    futuro = db.Column(db.Integer, nullable=False)
    comentarios = db.Column(db.Text)
    data_resposta = db.Column(db.DateTime, default=agora)
    ip = db.Column(db.String(45))
    anonima = db.Column(db.Boolean, default=False)

MODULOS_SISTEMA = {
    'dashboard': {'nome': '📊 Dashboard', 'descricao': 'Página inicial do sistema'},
    'checkin': {'nome': '✅ Check-in', 'descricao': 'Check-in de participantes'},
    'vendas': {'nome': '💰 Vendas', 'descricao': 'Registro de vendas'},
    'estoque': {'nome': '📦 Estoque', 'descricao': 'Gestão de estoque'},
    'sorteio': {'nome': '🎁 Sorteio', 'descricao': 'Realizar sorteios de brindes'},
    'entrega_brindes': {'nome': '✅ Entrega Brindes', 'descricao': 'Confirmar entrega de brindes sorteados'},
    'relatorios': {'nome': '📈 Relatórios', 'descricao': 'Relatórios do sistema'},
    'importacao': {'nome': '📤 Importação', 'descricao': 'Importar dados'},
    'exportacao': {'nome': '📥 Exportação', 'descricao': 'Exportar dados'},
    'brindes': {'nome': '🎯 Brindes', 'descricao': 'Gestão de brindes'},
    'usuarios': {'nome': '👥 Usuários', 'descricao': 'Gestão de usuários'},
    'logs': {'nome': '📊 Logs', 'descricao': 'Logs de auditoria'}
}

# Funções de permissão
def permissao_required(modulo):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if session.get('nivel_acesso') == 'admin':
                return f(*args, **kwargs)
            
            if not tem_permissao(modulo):
                flash(f'❌ Acesso negado. Permissão necessária para: {MODULOS_SISTEMA[modulo]["nome"]}', 'error')
                return redirect(url_for('index'))
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def migrar_banco_dados():
    """Função de migração adaptada para PostgreSQL"""
    try:
        with db.engine.connect() as conn:
            result = conn.execute(db.text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='faturamento_sorteio' AND column_name='participacoes_utilizadas'
            """))
            coluna_existe = result.fetchone() is not None
            
            if not coluna_existe:
                print("🔄 Adicionando campo 'participacoes_utilizadas' à tabela faturamento_sorteio...")
                conn.execute(db.text("ALTER TABLE faturamento_sorteio ADD COLUMN participacoes_utilizadas INTEGER DEFAULT 0"))
                conn.commit()
                print("✅ Campo 'participacoes_utilizadas' adicionado com sucesso!")
            else:
                print("✅ Campo 'participacoes_utilizadas' já existe na tabela")
    except Exception as e:
        print(f"❌ Erro na migração: {e}")

def tem_permissao(modulo):
    """Verifica se usuário tem permissão para o módulo"""
    if session.get('nivel_acesso') == 'admin':
        return True
    
    usuario = Usuario.query.get(session['usuario_id'])
    if usuario and usuario.permissoes:
        try:
            permissoes = json.loads(usuario.permissoes)
            return permissoes.get(modulo, False)
        except json.JSONDecodeError:
            return False
    
    return False

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'usuario_id' not in session:
            flash('🔐 Por favor, faça login para acessar esta página', 'info')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'usuario_id' not in session or session.get('nivel_acesso') != 'admin':
            flash('Acesso negado. Permissão de administrador necessária.', 'error')
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)
    return decorated_function

def criar_usuario_admin():
    """Cria usuário admin padrão se não existir"""
    admin = Usuario.query.filter_by(username='admin').first()
    if not admin:
        admin = Usuario(
            username='admin',
            password_hash=generate_password_hash('admin123'),
            nome='Administrador',
            nivel_acesso='admin'
        )
        db.session.add(admin)
        db.session.commit()
        print("✅ Usuário admin criado: admin / admin123")

def registrar_log(acao, modulo, dados=None):
    """Registra uma ação no log de auditoria"""
    log = LogAuditoria(
        usuario_id=session.get('usuario_id'),
        acao=acao,
        modulo=modulo,
        dados=json.dumps(dados, ensure_ascii=False) if dados else None,
        ip=request.remote_addr
    )
    db.session.add(log)
    db.session.commit()

# Funções auxiliares
def normalizar_cnpj(cnpj):
    if not cnpj:
        return ""
    cnpj_limpo = ''.join(filter(str.isdigit, str(cnpj)))
    if len(cnpj_limpo) == 14:
        return f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:]}"
    return cnpj_limpo

def normalizar_cnpj_pesquisa(cnpj):
    if not cnpj:
        return ""
    
    cnpj_limpo = ''.join(filter(str.isdigit, str(cnpj)))
    
    if len(cnpj_limpo) == 14:
        return f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:]}"
    else:
        return cnpj_limpo

def format_currency(value):
    if value is None or value == 0:
        return "0,00"
    try:
        value_float = float(value)
        return "{:,.2f}".format(value_float).replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        return "0,00"

app.jinja_env.filters['currency'] = format_currency
app.jinja_env.filters['cnpj'] = normalizar_cnpj

def get_faturamento_para_sorteio(cnpj):
    faturamento = FaturamentoSorteio.query.filter_by(cnpj=cnpj).first()
    if faturamento:
        return faturamento.faturamento_acumulado
    return 0.0

def get_revendas_para_sorteio(tipo_brinde):
    revendas_unicas = db.session.query(
        Cliente.cnpj,
        Cliente.razao_social
    ).distinct().all()
    
    revendas_qualificadas = []
    
    for cnpj, razao_social in revendas_unicas:
        faturamento = get_faturamento_para_sorteio(cnpj)
        
        if tipo_brinde == '50k':
            participacoes_totais = int(faturamento // 50000) if faturamento >= 50000 else 0
            
            if participacoes_totais > 0:
                sorteios_realizados = Sorteio.query.filter_by(
                    cnpj_vencedor=cnpj, 
                    tipo_brinde='50k'
                ).count()
                
                participacoes_restantes = participacoes_totais - sorteios_realizados
                
                if participacoes_restantes > 0:
                    revendas_qualificadas.append({
                        'cnpj': cnpj,
                        'razao_social': razao_social,
                        'faturamento_total': faturamento,
                        'participacoes': participacoes_restantes,
                        'sorteios_realizados': sorteios_realizados
                    })
                    
        elif tipo_brinde == '20k' and faturamento >= 20000:
            sorteios_realizados = Sorteio.query.filter_by(
                cnpj_vencedor=cnpj, 
                tipo_brinde='20k'
            ).count()
            
            if sorteios_realizados == 0 and faturamento < 50000:
                revendas_qualificadas.append({
                    'cnpj': cnpj,
                    'razao_social': razao_social,
                    'faturamento_total': faturamento,
                    'participacoes': 1,
                    'sorteios_realizados': 0
                })
    
    return revendas_qualificadas

def atualizar_faturamento_sorteio():
    FaturamentoSorteio.query.delete()
    
    faturamento_por_cnpj = db.session.query(
        Cliente.cnpj,
        db.func.sum(Venda.valor_pedido).label('faturamento_total')
    ).join(Venda).group_by(Cliente.cnpj).all()
    
    for cnpj, faturamento_total in faturamento_por_cnpj:
        sorteios_realizados = Sorteio.query.filter_by(cnpj_vencedor=cnpj, tipo_brinde='50k').count()
        
        faturamento_entry = FaturamentoSorteio(
            cnpj=cnpj,
            faturamento_acumulado=float(faturamento_total) if faturamento_total else 0.0,
            participacoes_utilizadas=sorteios_realizados
        )
        db.session.add(faturamento_entry)
    
    db.session.commit()

def get_estatisticas_avancadas():
    total_clientes = Cliente.query.count()
    total_checkins = Cliente.query.filter_by(checkin_realizado=True).count()
    revendas_unicas = db.session.query(Cliente.cnpj).distinct().count()
    revendas_presentes = db.session.query(Cliente.cnpj)\
        .filter(Cliente.checkin_realizado == True)\
        .distinct()\
        .count()
    
    return {
        'total_clientes': total_clientes,
        'total_checkins': total_checkins,
        'revendas_unicas': revendas_unicas,
        'revendas_presentes': revendas_presentes
    }

def export_to_excel(data, filename, sheet_name="Dados"):
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name
    
    if data:
        headers = list(data[0].keys())
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=header)
            cell.font = Font(bold=True)
            cell.fill = PatternFill(start_color="DDDDDD", end_color="DDDDDD", fill_type="solid")
        
        for row_num, row_data in enumerate(data, 2):
            for col_num, key in enumerate(headers, 1):
                ws.cell(row=row_num, column=col_num, value=row_data.get(key, ''))
    
    for column in ws.columns:
        max_length = 0
        column_letter = column[0].column_letter
        for cell in column:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = (max_length + 2)
        ws.column_dimensions[column_letter].width = adjusted_width
    
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    
    return output

def read_excel_file(file_stream):
    wb = load_workbook(filename=file_stream)
    ws = wb.active
    
    data = []
    headers = []
    
    for row_num, row in enumerate(ws.iter_rows(values_only=True), 1):
        if row_num == 1:
            headers = [str(header).strip().upper() if header else '' for header in row]
        else:
            row_data = {}
            for col_num, value in enumerate(row):
                if col_num < len(headers):
                    row_data[headers[col_num]] = value
            if any(row_data.values()):
                data.append(row_data)
    
    return data

def get_participacoes_50k(cnpj):
    faturamento = get_faturamento_para_sorteio(cnpj)
    if faturamento < 50000:
        return 0
    
    participacoes_totais = int(faturamento // 50000)
    sorteios_realizados = Sorteio.query.filter_by(cnpj_vencedor=cnpj, tipo_brinde='50k').count()
    
    return max(0, participacoes_totais - sorteios_realizados)

# Rotas principais
@app.route('/')
def index():
    if 'usuario_id' not in session:
        return redirect(url_for('login'))
    
    if tem_permissao('dashboard'):
        total_vendas = db.session.query(db.func.sum(Venda.valor_pedido)).scalar() or 0
        estatisticas = get_estatisticas_avancadas()
        consultores_count = db.session.query(Cliente.consultor).distinct().count()
        
        return render_template('index.html',
                            total_vendas=total_vendas,
                            total_checkins=estatisticas['total_checkins'],
                            total_clientes=estatisticas['total_clientes'],
                            revendas_unicas=estatisticas['revendas_unicas'],
                            revendas_presentes=estatisticas['revendas_presentes'],
                            consultores_count=consultores_count,
                            now=agora())
    else:
        return render_template('index.html', now=agora())

@app.route('/dashboard')
@login_required
@permissao_required('dashboard')
def dashboard():
    total_vendas = db.session.query(db.func.sum(Venda.valor_pedido)).scalar() or 0
    estatisticas = get_estatisticas_avancadas()
    consultores_count = db.session.query(Cliente.consultor).distinct().count()
    
    vendas_por_revenda = db.session.query(
        Cliente.razao_social,
        db.func.sum(Venda.valor_pedido).label('total_vendas')
    ).join(Venda).group_by(Cliente.razao_social).order_by(db.desc('total_vendas')).limit(10).all()
    
    vendas_por_hora = db.session.query(
        db.func.extract('hour', Venda.data_hora_venda).label('hora'),
        db.func.count(Venda.id).label('quantidade')
    ).group_by('hora').all()
    
    return render_template('dashboard.html', 
                         total_vendas=total_vendas,
                         total_checkins=estatisticas['total_checkins'],
                         total_clientes=estatisticas['total_clientes'],
                         revendas_unicas=estatisticas['revendas_unicas'],
                         revendas_presentes=estatisticas['revendas_presentes'],
                         consultores_count=consultores_count,
                         vendas_por_revenda=vendas_por_revenda,
                         vendas_por_hora=vendas_por_hora,
                         now=agora())

@app.route('/importar-clientes', methods=['GET', 'POST'])
@login_required
@permissao_required('importacao')
def importar_clientes():
    if request.method == 'POST':
        if 'file' not in request.files:
            return 'Nenhum arquivo selecionado', 400
        
        file = request.files['file']
        if file.filename == '':
            return 'Nenhum arquivo selecionado', 400
        
        if file and file.filename.endswith('.xlsx'):
            try:
                data = read_excel_file(file)
                
                colunas_necessarias = ['CNPJ', 'RAZÃO SOCIAL', 'RESPONSÁVEL', 'CONSULTOR']
                if data and data[0]:
                    colunas_arquivo = [key.upper() for key in data[0].keys()]
                    if not all(coluna in colunas_arquivo for coluna in colunas_necessarias):
                        return f'Arquivo deve conter as colunas: {", ".join(colunas_necessarias)}', 400
                else:
                    return 'Arquivo vazio ou formato inválido', 400
                
                clientes_importados = 0
                clientes_atualizados = 0
                
                for row in data:
                    cnpj_normalizado = normalizar_cnpj(str(row.get('CNPJ', '')))
                    
                    cliente_existente = Cliente.query.filter_by(
                        cnpj=cnpj_normalizado,
                        responsavel=row.get('RESPONSÁVEL', '')
                    ).first()
                    
                    direito_imagem = False
                    if 'DIREITO IMAGEM' in row:
                        direito_imagem_val = str(row.get('DIREITO IMAGEM', '')).upper().strip()
                        direito_imagem = direito_imagem_val in ['SIM', 'YES', 'S', 'Y', '1', 'VERDADEIRO', 'TRUE']
                    
                    if cliente_existente:
                        cliente_existente.razao_social = row.get('RAZÃO SOCIAL', '')
                        cliente_existente.consultor = row.get('CONSULTOR', 'Não Informado')
                        cliente_existente.direito_imagem = direito_imagem
                        clientes_atualizados += 1
                    else:
                        cliente = Cliente(
                            cnpj=cnpj_normalizado,
                            razao_social=row.get('RAZÃO SOCIAL', ''),
                            responsavel=row.get('RESPONSÁVEL', ''),
                            consultor=row.get('CONSULTOR', 'Não Informado'),
                            direito_imagem=direito_imagem
                        )
                        db.session.add(cliente)
                        clientes_importados += 1
                
                db.session.commit()
                
                registrar_log('importacao_clientes', 'importacao', {
                    'clientes_importados': clientes_importados,
                    'clientes_atualizados': clientes_atualizados,
                    'total_registros': len(data)
                })
                
                mensagem = f'Importação concluída! '
                if clientes_importados > 0:
                    mensagem += f'{clientes_importados} novos clientes importados. '
                if clientes_atualizados > 0:
                    mensagem += f'{clientes_atualizados} clientes atualizados.'
                
                return mensagem
                
            except Exception as e:
                registrar_log('erro_importacao_clientes', 'importacao', {
                    'erro': str(e),
                    'arquivo': file.filename
                })
                return f'Erro na importação: {str(e)}', 500
    
    total_vendas = db.session.query(db.func.sum(Venda.valor_pedido)).scalar() or 0
    estatisticas = get_estatisticas_avancadas()
    consultores_count = db.session.query(Cliente.consultor).distinct().count()
    
    return render_template('import_clientes.html',
                         total_vendas=total_vendas,
                         total_checkins=estatisticas['total_checkins'],
                         total_clientes=estatisticas['total_clientes'],
                         revendas_unicas=estatisticas['revendas_unicas'],
                         revendas_presentes=estatisticas['revendas_presentes'],
                         consultores_count=consultores_count)

@app.route('/checkin', methods=['GET', 'POST'])
@login_required
@permissao_required('checkin')
def checkin():
    if request.method == 'POST':
        cnpj = request.form.get('cnpj')
        cnpj_limpo = ''.join(filter(str.isdigit, cnpj))
        
        clientes = Cliente.query.filter(
            db.or_(
                Cliente.cnpj == cnpj_limpo,
                Cliente.cnpj == cnpj
            )
        ).all()
        
        if not clientes:
            return jsonify({'success': False, 'message': 'CNPJ não encontrado na lista'})
        
        if len(clientes) == 1:
            cliente = clientes[0]
            
            if not cliente.direito_imagem:
                return jsonify({
                    'success': False, 
                    'need_direito_imagem': True,
                    'message': 'Cliente não possui termo de direito de imagem assinado',
                    'cliente_id': cliente.id,
                    'razao_social': cliente.razao_social,
                    'responsavel': cliente.responsavel
                })
            
            if not cliente.checkin_realizado:
                veio_carro = request.form.get('veio_carro') == 'true'
                placa_veiculo = request.form.get('placa_veiculo', '').upper().strip() if veio_carro else None
                
                cliente.checkin_realizado = True
                cliente.horario_checkin = agora()
                cliente.responsavel_checkin = cliente.responsavel
                cliente.veio_carro = veio_carro
                cliente.placa_veiculo = placa_veiculo
                
                db.session.commit()
                
                registrar_log('checkin_realizado', 'checkin', {
                    'cliente_id': cliente.id,
                    'cnpj': cliente.cnpj,
                    'razao_social': cliente.razao_social,
                    'responsavel': cliente.responsavel,
                    'veio_carro': veio_carro,
                    'placa_veiculo': placa_veiculo
                })
                
                return jsonify({
                    'success': True, 
                    'message': f'Check-in realizado para {cliente.responsavel}!',
                    'single': True
                })
            else:
                return jsonify({
                    'success': False, 
                    'message': f'Check-in já realizado para {cliente.responsavel}'
                })
        else:
            checkins_realizados = [c for c in clientes if c.checkin_realizado]
            
            return jsonify({
                'success': False,
                'multiple': True,
                'message': 'Encontramos múltiplos responsáveis para este CNPJ. Por favor, selecione:',
                'responsaveis': [{
                    'id': c.id,
                    'nome': c.responsavel,
                    'ja_checkin': c.checkin_realizado,
                    'direito_imagem': c.direito_imagem
                } for c in clientes]
            })
    
    total_clientes = Cliente.query.count()
    total_checkins = Cliente.query.filter_by(checkin_realizado=True).count()
    
    return render_template('checkin.html',
                         total_clientes=total_clientes,
                         total_checkins=total_checkins)
    
@app.route('/confirmar-direito-imagem', methods=['POST'])
@login_required
@permissao_required('checkin')
def confirmar_direito_imagem():
    cliente_id = request.form.get('cliente_id')
    
    cliente = Cliente.query.get(cliente_id)
    if not cliente:
        return jsonify({'success': False, 'message': 'Cliente não encontrado'})
    
    cliente.direito_imagem = True
    db.session.commit()
    
    registrar_log('direito_imagem_confirmado', 'checkin', {
        'cliente_id': cliente.id,
        'cnpj': cliente.cnpj,
        'razao_social': cliente.razao_social,
        'responsavel': cliente.responsavel
    })
    
    return jsonify({
        'success': True, 
        'message': 'Termo de direito de imagem confirmado!'
    })

@app.route('/api/todos-clientes')
@login_required
def api_todos_clientes():
    clientes = Cliente.query.order_by(Cliente.razao_social).all()
    
    result = []
    for cliente in clientes:
        result.append({
            'id': cliente.id,
            'cnpj': cliente.cnpj,
            'razao_social': cliente.razao_social,
            'responsavel': cliente.responsavel,
            'consultor': cliente.consultor,
            'checkin_realizado': cliente.checkin_realizado,
            'horario_checkin': cliente.horario_checkin.isoformat() if cliente.horario_checkin else None,
            'direito_imagem': cliente.direito_imagem,
            'veio_carro': cliente.veio_carro,
            'placa_veiculo': cliente.placa_veiculo
        })
    
    return jsonify(result)

@app.route('/checkin-responsavel', methods=['POST'])
@login_required
def checkin_responsavel():
    cliente_id = request.form.get('cliente_id')
    veio_carro = request.form.get('veio_carro') == 'true'
    placa_veiculo = request.form.get('placa_veiculo', '').upper().strip() if veio_carro else None
    
    cliente = Cliente.query.get(cliente_id)
    if cliente:
        if not cliente.checkin_realizado:
            if not cliente.direito_imagem:
                return jsonify({
                    'success': False, 
                    'message': 'Cliente não possui termo de direito de imagem assinado'
                })
            
            cliente.checkin_realizado = True
            cliente.horario_checkin = agora()
            cliente.responsavel_checkin = cliente.responsavel
            cliente.veio_carro = veio_carro
            cliente.placa_veiculo = placa_veiculo
            db.session.commit()
            
            registrar_log('checkin_responsavel', 'checkin', {
                'cliente_id': cliente.id,
                'cnpj': cliente.cnpj,
                'razao_social': cliente.razao_social,
                'responsavel': cliente.responsavel,
                'veio_carro': veio_carro,
                'placa_veiculo': placa_veiculo
            })
            
            return jsonify({
                'success': True, 
                'message': f'Check-in realizado para {cliente.responsavel}!'
            })
        else:
            return jsonify({
                'success': False, 
                'message': 'Check-in já realizado para este responsável'
            })
    
    return jsonify({'success': False, 'message': 'Responsável não encontrado'})

@app.route('/cadastro-rapido-checkin', methods=['POST'])
@login_required
def cadastro_rapido_checkin():
    data = request.get_json()
    
    cnpj = data.get('cnpj')
    razao_social = data.get('razao_social')
    responsavel = data.get('responsavel')
    consultor = data.get('consultor', 'Não Informado')
    direito_imagem = data.get('direito_imagem', False)
    
    if not cnpj or not razao_social or not responsavel:
        return jsonify({'success': False, 'message': 'CNPJ, Razão Social e Responsável são obrigatórios'})
    
    if not direito_imagem:
        return jsonify({'success': False, 'message': 'É necessário confirmar o termo de direito de imagem'})
    
    cnpj_normalizado = normalizar_cnpj(cnpj)
    
    try:
        cliente_existente = Cliente.query.filter_by(
            cnpj=cnpj_normalizado,
            responsavel=responsavel
        ).first()
        
        if cliente_existente:
            if not cliente_existente.checkin_realizado:
                cliente_existente.checkin_realizado = True
                cliente_existente.horario_checkin = agora()
                cliente_existente.responsavel_checkin = responsavel
                cliente_existente.direito_imagem = True
                db.session.commit()
                
                registrar_log('cadastro_rapido_checkin', 'checkin', {
                    'tipo': 'cliente_existente',
                    'cliente_id': cliente_existente.id,
                    'cnpj': cnpj_normalizado,
                    'razao_social': razao_social,
                    'responsavel': responsavel
                })
                
                return jsonify({
                    'success': True, 
                    'message': f'Check-in realizado para {responsavel} da revenda {razao_social}!'
                })
            else:
                return jsonify({
                    'success': False, 
                    'message': f'Check-in já realizado para {responsavel}'
                })
        else:
            revenda_existente = Cliente.query.filter_by(cnpj=cnpj_normalizado).first()
            
            if revenda_existente:
                novo_cliente = Cliente(
                    cnpj=cnpj_normalizado,
                    razao_social=razao_social,
                    responsavel=responsavel,
                    consultor=consultor,
                    checkin_realizado=True,
                    horario_checkin=agora(),
                    responsavel_checkin=responsavel,
                    direito_imagem=True
                )
                db.session.add(novo_cliente)
                db.session.commit()
                
                registrar_log('cadastro_rapido_checkin', 'checkin', {
                    'tipo': 'novo_responsavel',
                    'cliente_id': novo_cliente.id,
                    'cnpj': cnpj_normalizado,
                    'razao_social': razao_social,
                    'responsavel': responsavel
                })
                
                return jsonify({
                    'success': True, 
                    'message': f'Novo responsável {responsavel} cadastrado e check-in realizado para {razao_social}!'
                })
            else:
                novo_cliente = Cliente(
                    cnpj=cnpj_normalizado,
                    razao_social=razao_social,
                    responsavel=responsavel,
                    consultor=consultor,
                    checkin_realizado=True,
                    horario_checkin=agora(),
                    responsavel_checkin=responsavel,
                    direito_imagem=True
                )
                db.session.add(novo_cliente)
                db.session.commit()
                
                registrar_log('cadastro_rapido_checkin', 'checkin', {
                    'tipo': 'nova_revenda',
                    'cliente_id': novo_cliente.id,
                    'cnpj': cnpj_normalizado,
                    'razao_social': razao_social,
                    'responsavel': responsavel
                })
                
                return jsonify({
                    'success': True, 
                    'message': f'Nova revenda {razao_social} cadastrada e check-in realizado para {responsavel}!'
                })
                
    except Exception as e:
        registrar_log('erro_cadastro_rapido', 'checkin', {
            'cnpj': cnpj,
            'razao_social': razao_social,
            'responsavel': responsavel,
            'erro': str(e)
        })
        return jsonify({'success': False, 'message': f'Erro no cadastro: {str(e)}'})

@app.route('/registrar-venda', methods=['GET', 'POST'])
@login_required
@permissao_required('vendas')
def registrar_venda():
    if request.method == 'POST':
        cnpj_compra = request.form.get('cnpj_compra')
        numero_pedido = request.form.get('numero_pedido')
        valor_pedido = request.form.get('valor_pedido')
        produtos_data = request.form.get('produtos_data')
        
        if not numero_pedido.isdigit():
            return jsonify({'success': False, 'message': 'Número do pedido deve conter apenas números'})
        
        try:
            valor_pedido_float = float(valor_pedido)
            produtos = json.loads(produtos_data) if produtos_data else []
        except (ValueError, json.JSONDecodeError):
            return jsonify({'success': False, 'message': 'Dados inválidos'})
        
        pedido_existente = Venda.query.filter_by(numero_pedido=numero_pedido).first()
        if pedido_existente:
            return jsonify({'success': False, 'message': 'Número de pedido já registrado'})
        
        cliente = Cliente.query.filter_by(cnpj=cnpj_compra, checkin_realizado=True).first()
        
        if cliente:
            venda = Venda(
                cnpj_compra=cnpj_compra,
                numero_pedido=numero_pedido,
                valor_pedido=valor_pedido_float,
                cliente_id=cliente.id
            )
            db.session.add(venda)
            db.session.flush()
            
            produtos_vendidos = []
            for produto in produtos:
                equipamento_id = produto['equipamento_id']
                quantidade = produto['quantidade']
                
                if equipamento_id and quantidade:
                    equipamento = Estoque.query.get(equipamento_id)
                    if equipamento and equipamento.quantidade_disponivel >= quantidade:
                        venda_equipamento = VendaEquipamento(
                            venda_id=venda.id,
                            equipamento_id=equipamento_id,
                            quantidade=quantidade
                        )
                        db.session.add(venda_equipamento)
                        equipamento.quantidade_disponivel -= quantidade
                        
                        produtos_vendidos.append({
                            'equipamento': f"{equipamento.fabricante} - {equipamento.modelo}",
                            'quantidade': quantidade
                        })
                    else:
                        db.session.rollback()
                        return jsonify({'success': False, 'message': f'Estoque insuficiente para {equipamento.fabricante} - {equipamento.modelo}'})
            
            db.session.commit()
            
            registrar_log('venda_registrada', 'vendas', {
                'venda_id': venda.id,
                'numero_pedido': numero_pedido,
                'cnpj_compra': cnpj_compra,
                'valor_pedido': valor_pedido_float,
                'cliente_id': cliente.id,
                'cliente_razao_social': cliente.razao_social,
                'produtos': produtos_vendidos
            })
            
            mensagem = 'Venda registrada com sucesso!'
            if produtos:
                mensagem += ' Produtos debitados do estoque.'
            return jsonify({'success': True, 'message': mensagem})
        else:
            return jsonify({
                'success': False, 
                'need_link': True,
                'message': 'CNPJ não fez check-in. É necessário vincular com um CNPJ que fez check-in.'
            })
    
    return render_template('vendas.html')

@app.route('/vincular-cnpj', methods=['POST'])
@login_required
def vincular_cnpj():
    cnpj_checkin = request.form.get('cnpj_checkin')
    cnpj_compra = request.form.get('cnpj_compra')
    numero_pedido = request.form.get('numero_pedido')
    valor_pedido = request.form.get('valor_pedido')
    produtos_data = request.form.get('produtos_data')
    
    if not numero_pedido.isdigit():
        return jsonify({'success': False, 'message': 'Número do pedido deve conter apenas números'})
    
    try:
        produtos = json.loads(produtos_data) if produtos_data else []
    except json.JSONDecodeError:
        return jsonify({'success': False, 'message': 'Dados de produtos inválidos'})
    
    pedido_existente = Venda.query.filter_by(numero_pedido=numero_pedido).first()
    if pedido_existente:
        return jsonify({'success': False, 'message': 'Número de pedido já registrado'})
    
    cliente = Cliente.query.filter_by(cnpj=cnpj_checkin, checkin_realizado=True).first()
    
    if cliente:
        venda = Venda(
            cnpj_compra=cnpj_compra,
            numero_pedido=numero_pedido,
            valor_pedido=float(valor_pedido),
            cliente_id=cliente.id,
            cnpj_checkin_vinculado=cnpj_checkin
        )
        db.session.add(venda)
        db.session.flush()
        
        produtos_vendidos = []
        for produto in produtos:
            equipamento_id = produto['equipamento_id']
            quantidade = produto['quantidade']
            
            if equipamento_id and quantidade:
                equipamento = Estoque.query.get(equipamento_id)
                if equipamento and equipamento.quantidade_disponivel >= quantidade:
                    venda_equipamento = VendaEquipamento(
                        venda_id=venda.id,
                        equipamento_id=equipamento_id,
                        quantidade=quantidade
                    )
                    db.session.add(venda_equipamento)
                    equipamento.quantidade_disponivel -= quantidade
                    
                    produtos_vendidos.append({
                        'equipamento': f"{equipamento.fabricante} - {equipamento.modelo}",
                        'quantidade': quantidade
                    })
                else:
                    db.session.rollback()
                    return jsonify({'success': False, 'message': f'Estoque insuficiente para {equipamento.fabricante} - {equipamento.modelo}'})
        
        db.session.commit()
        
        registrar_log('venda_vinculada', 'vendas', {
            'venda_id': venda.id,
            'numero_pedido': numero_pedido,
            'cnpj_compra': cnpj_compra,
            'cnpj_checkin': cnpj_checkin,
            'valor_pedido': float(valor_pedido),
            'cliente_id': cliente.id,
            'cliente_razao_social': cliente.razao_social,
            'produtos': produtos_vendidos
        })
        
        return jsonify({'success': True, 'message': 'Venda vinculada e registrada com sucesso!'})
    else:
        return jsonify({'success': False, 'message': 'CNPJ de check-in não encontrado'})
    
@app.route('/api/verificar-pedido/<numero_pedido>')
@login_required
def api_verificar_pedido(numero_pedido):
    pedido_existente = Venda.query.filter_by(numero_pedido=numero_pedido).first()
    return jsonify({'existe': pedido_existente is not None})
    
@app.route('/estoque')
def estoque():
    equipamentos = Estoque.query.filter_by(ativo=True).order_by(Estoque.fabricante, Estoque.modelo).all()
    
    total_equipamentos = len(equipamentos)
    total_disponivel = sum(e.quantidade_disponivel for e in equipamentos)
    total_estoque = sum(e.quantidade_total for e in equipamentos)
    percentual_disponivel = (total_disponivel / total_estoque * 100) if total_estoque > 0 else 0
    
    return render_template('estoque.html',
                         equipamentos=equipamentos,
                         total_equipamentos=total_equipamentos,
                         total_disponivel=total_disponivel,
                         total_estoque=total_estoque,
                         percentual_disponivel=percentual_disponivel)

@app.route('/adicionar-equipamento', methods=['POST'])
@login_required
def adicionar_equipamento():
    fabricante = request.form.get('fabricante')
    modelo = request.form.get('modelo')
    quantidade = request.form.get('quantidade')
    
    if not fabricante or not modelo or not quantidade:
        return jsonify({'success': False, 'message': 'Preencha todos os campos obrigatórios'})
    
    try:
        quantidade_int = int(quantidade)
        
        equipamento_existente = Estoque.query.filter_by(
            fabricante=fabricante.upper(),
            modelo=modelo.upper(),
            ativo=True
        ).first()
        
        if equipamento_existente:
            equipamento_existente.quantidade_total += quantidade_int
            equipamento_existente.quantidade_disponivel += quantidade_int
            
            registrar_log('estoque_atualizado', 'estoque', {
                'equipamento_id': equipamento_existente.id,
                'fabricante': fabricante,
                'modelo': modelo,
                'quantidade_adicionada': quantidade_int,
                'novo_total': equipamento_existente.quantidade_total,
                'novo_disponivel': equipamento_existente.quantidade_disponivel,
                'tipo': 'atualizacao'
            })
        else:
            equipamento = Estoque(
                fabricante=fabricante.upper(),
                modelo=modelo.upper(),
                quantidade_total=quantidade_int,
                quantidade_disponivel=quantidade_int
            )
            db.session.add(equipamento)
            db.session.flush()
            
            registrar_log('equipamento_adicionado', 'estoque', {
                'equipamento_id': equipamento.id,
                'fabricante': fabricante,
                'modelo': modelo,
                'quantidade': quantidade_int,
                'tipo': 'novo'
            })
        
        db.session.commit()
        return jsonify({'success': True, 'message': 'Equipamento adicionado ao estoque com sucesso!'})
        
    except Exception as e:
        registrar_log('erro_adicionar_equipamento', 'estoque', {
            'fabricante': fabricante,
            'modelo': modelo,
            'quantidade': quantidade,
            'erro': str(e)
        })
        return jsonify({'success': False, 'message': f'Erro ao adicionar equipamento: {str(e)}'})

@app.route('/remover-equipamento/<int:equipamento_id>')
@login_required
def remover_equipamento(equipamento_id):
    equipamento = Estoque.query.get(equipamento_id)
    if equipamento:
        equipamento.ativo = False
        db.session.commit()
        
        registrar_log('equipamento_removido', 'estoque', {
            'equipamento_id': equipamento_id,
            'fabricante': equipamento.fabricante,
            'modelo': equipamento.modelo,
            'quantidade_total': equipamento.quantidade_total,
            'quantidade_disponivel': equipamento.quantidade_disponivel
        })
        
        return jsonify({'success': True, 'message': 'Equipamento removido do estoque!'})
    return jsonify({'success': False, 'message': 'Equipamento não encontrado'})

@app.route('/api/estoque-atual')
def api_estoque_atual():
    equipamentos = Estoque.query.filter_by(ativo=True).order_by(Estoque.fabricante, Estoque.modelo).all()
    
    result = []
    for equipamento in equipamentos:
        result.append({
            'id': equipamento.id,
            'fabricante': equipamento.fabricante,
            'modelo': equipamento.modelo,
            'quantidade_total': equipamento.quantidade_total,
            'quantidade_disponivel': equipamento.quantidade_disponivel,
            'status': 'success' if equipamento.quantidade_disponivel > 5 else 
                     'warning' if equipamento.quantidade_disponivel > 0 else 
                     'danger'
        })
    
    return jsonify(result)

@app.route('/estoque-publico')
def estoque_publico():
    return render_template('estoque_publico.html')

@app.route('/api/vendas-hoje')
@login_required
def api_vendas_hoje():
    hoje = agora().date()
    quantidade_vendas_hoje = Venda.query.filter(
        db.cast(Venda.data_hora_venda, db.Date) == hoje
    ).count()
    return jsonify({'total': quantidade_vendas_hoje})

@app.route('/api/brindes-sorteados')
@login_required
def api_brindes_sorteados():
    total_sorteados = Sorteio.query.count()
    return jsonify({'total': total_sorteados})

@app.route('/api/ticket-medio')
@login_required
def api_ticket_medio():
    total_vendas = db.session.query(db.func.sum(Venda.valor_pedido)).scalar() or 0
    count_vendas = Venda.query.count()
    media = total_vendas / count_vendas if count_vendas > 0 else 0
    return jsonify({'media': float(media)})

@app.route('/api/total-pedidos')
@login_required
def api_total_pedidos():
    total_pedidos = Venda.query.count()
    return jsonify({'total': total_pedidos})

@app.route('/sorteio')
@login_required
@permissao_required('sorteio')
def sorteio():
    atualizar_faturamento_sorteio()
    revendas_20k = get_revendas_para_sorteio('20k')
    revendas_50k = get_revendas_para_sorteio('50k')
    brindes_20k = Brinde.query.filter_by(tipo_sorteio='20k', ativo=True).all()
    brindes_50k = Brinde.query.filter_by(tipo_sorteio='50k', ativo=True).all()
    
    total_participacoes_20k = len(revendas_20k)
    total_participacoes_50k = sum(revenda['participacoes'] for revenda in revendas_50k)
    
    brindes_20k_disponiveis = sum(1 for b in brindes_20k if b.quantidade_disponivel > 0)
    brindes_50k_disponiveis = sum(1 for b in brindes_50k if b.quantidade_disponivel > 0)
    
    ultimos_sorteios = Sorteio.query.order_by(Sorteio.data_sorteio.desc()).limit(10).all()
    
    return render_template('sorteio.html', 
                         revendas_20k=revendas_20k,
                         revendas_50k=revendas_50k,
                         brindes_20k=brindes_20k,
                         brindes_50k=brindes_50k,
                         total_revendas_20k=len(revendas_20k),
                         total_revendas_50k=len(revendas_50k),
                         total_participacoes_20k=total_participacoes_20k,
                         total_participacoes_50k=total_participacoes_50k,
                         brindes_20k_disponiveis=brindes_20k_disponiveis,
                         brindes_50k_disponiveis=brindes_50k_disponiveis,
                         ultimos_sorteios=ultimos_sorteios)

@app.route('/realizar-sorteio', methods=['POST'])
@login_required
def realizar_sorteio():
    tipo_brinde = request.form.get('tipo_brinde')
    cnpj_vencedor = request.form.get('cnpj_vencedor')
    responsavel = request.form.get('responsavel')
    
    if not cnpj_vencedor:
        return jsonify({'success': False, 'message': 'Por favor, selecione uma revenda para o sorteio'})
    
    if not responsavel:
        return jsonify({'success': False, 'message': 'Por favor, informe o responsável pelo recebimento'})
    
    revendas_qualificadas = get_revendas_para_sorteio(tipo_brinde)
    revenda_vencedora = next((r for r in revendas_qualificadas if r['cnpj'] == cnpj_vencedor), None)
    
    if not revenda_vencedora:
        return jsonify({'success': False, 'message': 'Revenda não encontrada ou não qualificada para este sorteio'})
    
    brindes_disponiveis = Brinde.query.filter_by(
        tipo_sorteio=tipo_brinde, 
        ativo=True
    ).filter(Brinde.quantidade_disponivel > 0).all()
    
    if not brindes_disponiveis:
        return jsonify({'success': False, 'message': f'Nenhum brinde disponível com estoque para a categoria R$ {tipo_brinde}'})
    
    brinde_sorteado = random.choice(brindes_disponiveis)
    brinde_sorteado.quantidade_disponivel -= 1
    
    sorteio = Sorteio(
        tipo_brinde=tipo_brinde,
        cnpj_vencedor=revenda_vencedora['cnpj'],
        razao_social_vencedor=revenda_vencedora['razao_social'],
        responsavel_recebimento=responsavel,
        valor_acumulado_revenda=revenda_vencedora['faturamento_total'],
        brinde_id=brinde_sorteado.id
    )
    db.session.add(sorteio)
    db.session.commit()
    
    pedidos_qualificadores = Venda.query.join(Cliente)\
                                       .filter(Cliente.cnpj == cnpj_vencedor)\
                                       .order_by(Venda.data_hora_venda.asc())\
                                       .all()
    
    participacoes_restantes = 0
    if tipo_brinde == '50k':
        sorteios_realizados = Sorteio.query.filter_by(
            cnpj_vencedor=cnpj_vencedor, 
            tipo_brinde='50k'
        ).count()
        
        participacoes_totais = revenda_vencedora.get('participacoes', 0)
        participacoes_restantes = max(0, participacoes_totais - sorteios_realizados)
    
    registrar_log('sorteio_realizado', 'sorteio', {
        'sorteio_id': sorteio.id,
        'tipo_brinde': tipo_brinde,
        'cnpj_vencedor': cnpj_vencedor,
        'razao_social_vencedor': revenda_vencedora['razao_social'],
        'responsavel_recebimento': responsavel,
        'valor_acumulado': revenda_vencedora['faturamento_total'],
        'participacoes_antes': revenda_vencedora.get('participacoes', 1),
        'participacoes_restantes': participacoes_restantes,
        'brinde_nome': brinde_sorteado.nome,
        'brinde_valor': brinde_sorteado.valor_aproximado,
        'quantidade_pedidos_qualificadores': len(pedidos_qualificadores)
    })
    
    return jsonify({
        'success': True, 
        'message': f'Sorteio de R$ {tipo_brinde} realizado!' + 
                  (f' (Participações restantes: {participacoes_restantes})' if tipo_brinde == '50k' and participacoes_restantes > 0 else ''),
        'vencedor': {
            'razao_social': revenda_vencedora['razao_social'],
            'cnpj': revenda_vencedora['cnpj'],
            'faturamento': revenda_vencedora['faturamento_total'],
            'participacoes': revenda_vencedora.get('participacoes', 1)
        },
        'brinde': {
            'nome': brinde_sorteado.nome,
            'descricao': brinde_sorteado.descricao,
            'valor_aproximado': brinde_sorteado.valor_aproximado
        },
        'participacoes_restantes': participacoes_restantes,
        'pedidos_qualificadores': [{
            'numero_pedido': p.numero_pedido,
            'valor': p.valor_pedido,
            'data': p.data_hora_venda.strftime('%d/%m/%Y %H:%M')
        } for p in pedidos_qualificadores]
    })

@app.route('/confirmar-entrega')
@login_required
@permissao_required('entrega_brindes')
def confirmar_entrega_page():
    return render_template('confirmar_entrega.html')

@app.route('/api/brindes-sorteados-completo')
@login_required
def api_brindes_sorteados_completo():
    sorteios = Sorteio.query.order_by(Sorteio.data_sorteio.desc()).all()
    
    result = []
    for sorteio in sorteios:
        result.append({
            'id': sorteio.id,
            'tipo_brinde': sorteio.tipo_brinde,
            'cnpj_vencedor': sorteio.cnpj_vencedor,
            'razao_social_vencedor': sorteio.razao_social_vencedor,
            'responsavel_recebimento': sorteio.responsavel_recebimento,
            'data_sorteio': sorteio.data_sorteio.isoformat() if sorteio.data_sorteio else None,
            'valor_acumulado_revenda': sorteio.valor_acumulado_revenda,
            'brinde_id': sorteio.brinde_id,
            'brinde_nome': sorteio.brinde.nome if sorteio.brinde else None,
            'brinde_descricao': sorteio.brinde.descricao if sorteio.brinde else None,
            'brinde_valor': sorteio.brinde.valor_aproximado if sorteio.brinde else None,
            'entregue': sorteio.entregue,
            'data_entrega': sorteio.data_entrega.isoformat() if sorteio.data_entrega else None,
            'responsavel_entrega': sorteio.responsavel_entrega,
            'observacao_entrega': sorteio.observacao_entrega
        })
    
    return jsonify(result)

@app.route('/confirmar-entrega', methods=['POST'])
@login_required
def confirmar_entrega():
    sorteio_id = request.form.get('sorteio_id')
    responsavel_entrega = request.form.get('responsavel_entrega')
    observacao_entrega = request.form.get('observacao_entrega')
    
    if not sorteio_id or not responsavel_entrega:
        return jsonify({'success': False, 'message': 'ID do sorteio e responsável pela entrega são obrigatórios'})
    
    try:
        sorteio = Sorteio.query.get(int(sorteio_id))
        if not sorteio:
            return jsonify({'success': False, 'message': 'Sorteio não encontrado'})
        
        if sorteio.entregue:
            return jsonify({'success': False, 'message': 'Este brinde já foi entregue'})
        
        sorteio.entregue = True
        sorteio.data_entrega = agora()
        sorteio.responsavel_entrega = responsavel_entrega
        sorteio.observacao_entrega = observacao_entrega
        
        db.session.commit()
        
        registrar_log('entrega_brinde_confirmada', 'entrega_brindes', {
            'sorteio_id': sorteio_id,
            'cnpj_vencedor': sorteio.cnpj_vencedor,
            'razao_social_vencedor': sorteio.razao_social_vencedor,
            'brinde_nome': sorteio.brinde.nome if sorteio.brinde else 'N/A',
            'responsavel_entrega': responsavel_entrega,
            'observacao': observacao_entrega
        })
        
        return jsonify({'success': True, 'message': 'Entrega confirmada com sucesso!'})
        
    except Exception as e:
        registrar_log('erro_confirmar_entrega', 'entrega_brindes', {
            'sorteio_id': sorteio_id,
            'erro': str(e)
        })
        return jsonify({'success': False, 'message': f'Erro ao confirmar entrega: {str(e)}'})

@app.route('/brindes')
@login_required
@permissao_required('brindes')
def brindes():
    brindes_20k = Brinde.query.filter_by(tipo_sorteio='20k', ativo=True).all()
    brindes_50k = Brinde.query.filter_by(tipo_sorteio='50k', ativo=True).all()
    return render_template('brindes.html', 
                         brindes_20k=brindes_20k, 
                         brindes_50k=brindes_50k)

@app.route('/adicionar-brinde', methods=['POST'])
@login_required
def adicionar_brinde():
    tipo_sorteio = request.form.get('tipo_sorteio')
    nome = request.form.get('nome')
    descricao = request.form.get('descricao')
    valor_aproximado = request.form.get('valor_aproximado')
    quantidade_total = request.form.get('quantidade_total')
    
    brinde = Brinde(
        tipo_sorteio=tipo_sorteio,
        nome=nome,
        descricao=descricao,
        valor_aproximado=float(valor_aproximado) if valor_aproximado else None,
        quantidade_total=int(quantidade_total),
        quantidade_disponivel=int(quantidade_total)
    )
    db.session.add(brinde)
    db.session.commit()
    
    registrar_log('brinde_adicionado', 'brindes', {
        'brinde_id': brinde.id,
        'tipo_sorteio': tipo_sorteio,
        'nome': nome,
        'valor_aproximado': float(valor_aproximado) if valor_aproximado else None,
        'quantidade_total': int(quantidade_total)
    })
    
    return jsonify({'success': True, 'message': 'Brinde adicionado com sucesso!'})

@app.route('/remover-brinde/<int:brinde_id>')
@login_required
def remover_brinde(brinde_id):
    brinde = Brinde.query.get(brinde_id)
    if brinde:
        brinde.ativo = False
        db.session.commit()
        
        registrar_log('brinde_removido', 'brindes', {
            'brinde_id': brinde_id,
            'nome': brinde.nome,
            'tipo_sorteio': brinde.tipo_sorteio
        })
        
        return jsonify({'success': True, 'message': 'Brinde removido com sucesso!'})
    return jsonify({'success': False, 'message': 'Brinde não encontrado'})

@app.route('/relatorios')
@login_required
@permissao_required('relatorios')
def relatorios():
    sorteios = Sorteio.query.order_by(Sorteio.data_sorteio.desc()).all()
    checkins = Cliente.query.filter_by(checkin_realizado=True)\
                          .order_by(Cliente.horario_checkin.desc())\
                          .all()
    vendas = Venda.query.join(Cliente)\
                       .order_by(Venda.data_hora_venda.desc())\
                       .all()
    
    sorteios_20k = [s for s in sorteios if s.tipo_brinde == '20k']
    sorteios_50k = [s for s in sorteios if s.tipo_brinde == '50k']
    total_faturamento_sorteios = sum(s.valor_acumulado_revenda for s in sorteios)
    
    revendas_unicas_checkin = db.session.query(Cliente.cnpj)\
                                       .filter(Cliente.checkin_realizado == True)\
                                       .distinct()\
                                       .count()
    responsaveis_unicos_checkin = db.session.query(Cliente.responsavel_checkin)\
                                           .filter(Cliente.checkin_realizado == True)\
                                           .distinct()\
                                           .count()
    total_clientes = Cliente.query.count()
    
    total_vendas_valor = sum(v.valor_pedido for v in vendas)
    revendas_compradoras = db.session.query(Venda.cnpj_compra).distinct().count()
    ticket_medio = total_vendas_valor / len(vendas) if vendas else 0
    
    return render_template('relatorios.html',
                         sorteios=sorteios,
                         sorteios_20k=sorteios_20k,
                         sorteios_50k=sorteios_50k,
                         total_faturamento_sorteios=total_faturamento_sorteios,
                         checkins=checkins,
                         revendas_unicas_checkin=revendas_unicas_checkin,
                         responsaveis_unicos_checkin=responsaveis_unicos_checkin,
                         total_clientes=total_clientes,
                         vendas=vendas,
                         total_vendas_valor=total_vendas_valor,
                         revendas_compradoras=revendas_compradoras,
                         ticket_medio=ticket_medio)

@app.route('/exportar-relatorio/<int:sorteio_id>')
@login_required
@permissao_required('exportacao')
def exportar_relatorio(sorteio_id):
    sorteio = Sorteio.query.get_or_404(sorteio_id)
    vendas = Venda.query.join(Cliente).filter(Cliente.cnpj == sorteio.cnpj_vencedor).all()
    pedidos_qualificadores = vendas
    
    data = [{
        'CNPJ': sorteio.cnpj_vencedor,
        'Razão Social': sorteio.razao_social_vencedor,
        'Faturamento Total': sorteio.valor_acumulado_revenda,
        'Brinde Ganho': f'R$ {sorteio.tipo_brinde} - {sorteio.brinde.nome if sorteio.brinde else "N/A"}',
        'Responsável Recebimento': sorteio.responsavel_recebimento,
        'Data Sorteio': sorteio.data_sorteio.strftime('%d/%m/%Y %H:%M'),
        'Números Pedidos': ', '.join([v.numero_pedido for v in pedidos_qualificadores]),
        'Quantidade de Pedidos': len(pedidos_qualificadores),
        'Valor Total Pedidos': sum(v.valor_pedido for v in pedidos_qualificadores)
    }]
    
    output = export_to_excel(data, f'relatorio_sorteio_{sorteio_id}.xlsx', 'Sorteio')
    
    registrar_log('relatorio_exportado', 'exportacao', {
        'sorteio_id': sorteio_id,
        'tipo_relatorio': 'sorteio',
        'cnpj_vencedor': sorteio.cnpj_vencedor
    })
    
    return send_file(output, 
                    download_name=f'relatorio_sorteio_{sorteio_id}.xlsx',
                    as_attachment=True,
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

@app.route('/api/total-vendas')
@login_required
def api_total_vendas():
    total = db.session.query(db.func.sum(Venda.valor_pedido)).scalar() or 0
    return jsonify({'total_vendas': float(total)})

@app.route('/api/ultimos-checkins')
def api_ultimos_checkins():
    ultimos = Cliente.query.filter_by(checkin_realizado=True)\
                          .order_by(Cliente.horario_checkin.desc())\
                          .limit(10)\
                          .all()
    
    result = []
    for cliente in ultimos:
        result.append({
            'cnpj': cliente.cnpj,
            'razao_social': cliente.razao_social,
            'responsavel_checkin': cliente.responsavel_checkin or cliente.responsavel,
            'consultor': cliente.consultor,
            'horario_checkin': cliente.horario_checkin.isoformat() if cliente.horario_checkin else '',
            'veio_carro': cliente.veio_carro,
            'placa_veiculo': cliente.placa_veiculo
        })
    
    return jsonify(result)

@app.route('/api/buscar-veiculo/<placa>')
@login_required
def api_buscar_veiculo(placa):
    if not placa or len(placa.strip()) < 3:
        return jsonify({'success': False, 'message': 'Digite pelo menos 3 caracteres da placa'})
    
    placa_busca = placa.upper().strip()
    
    clientes = Cliente.query.filter(
        Cliente.checkin_realizado == True,
        Cliente.veio_carro == True,
        Cliente.placa_veiculo.ilike(f'%{placa_busca}%')
    ).all()
    
    if not clientes:
        return jsonify({'success': False, 'message': 'Nenhum veículo encontrado com esta placa'})
    
    result = []
    for cliente in clientes:
        result.append({
            'razao_social': cliente.razao_social,
            'responsavel': cliente.responsavel_checkin or cliente.responsavel,
            'cnpj': cliente.cnpj,
            'placa_veiculo': cliente.placa_veiculo,
            'horario_checkin': cliente.horario_checkin.strftime('%d/%m/%Y %H:%M') if cliente.horario_checkin else '',
            'consultor': cliente.consultor
        })
    
    return jsonify({'success': True, 'veiculos': result})

@app.route('/api/ultimos-clientes')
@login_required
def api_ultimos_clientes():
    ultimos = Cliente.query.order_by(Cliente.id.desc()).limit(15).all()
    
    result = []
    for cliente in ultimos:
        result.append({
            'cnpj': cliente.cnpj,
            'razao_social': cliente.razao_social,
            'responsavel': cliente.responsavel,
            'consultor': cliente.consultor,
            'checkin_realizado': cliente.checkin_realizado
        })
    
    return jsonify(result)

@app.route('/exportar-clientes')
@login_required
@permissao_required('exportacao')
def exportar_clientes():
    clientes = Cliente.query.all()
    
    data = []
    for cliente in clientes:
        data.append({
            'CNPJ': cliente.cnpj,
            'RAZÃO SOCIAL': cliente.razao_social,
            'RESPONSÁVEL': cliente.responsavel,
            'CONSULTOR': cliente.consultor,
            'Check-in Realizado': 'Sim' if cliente.checkin_realizado else 'Não',
            'Responsável Check-in': cliente.responsavel_checkin or '',
            'Horário Check-in': cliente.horario_checkin.strftime('%d/%m/%Y %H:%M') if cliente.horario_checkin else ''
        })
    
    output = export_to_excel(data, 'clientes_ro_experience.xlsx', 'Clientes')
    
    registrar_log('exportacao_clientes', 'exportacao', {
        'quantidade_clientes': len(clientes),
        'tipo_exportacao': 'clientes'
    })
    
    return send_file(output, 
                    download_name='clientes_ro_experience.xlsx',
                    as_attachment=True)

@app.route('/exportar-vendas')
@login_required
@permissao_required('exportacao')
def exportar_vendas():
    vendas = Venda.query.join(Cliente).all()
    
    data = []
    for venda in vendas:
        data.append({
            'CNPJ Compra': venda.cnpj_compra,
            'Número Pedido': venda.numero_pedido,
            'Valor Pedido': venda.valor_pedido,
            'Data/Hora': venda.data_hora_venda.strftime('%d/%m/%Y %H:%M'),
            'Razão Social': venda.cliente.razao_social,
            'Responsável': venda.cliente.responsavel,
            'Consultor': venda.cliente.consultor,
            'CNPJ Check-in Vinculado': venda.cnpj_checkin_vinculado or ''
        })
    
    output = export_to_excel(data, 'vendas_ro_experience.xlsx', 'Vendas')
    
    registrar_log('exportacao_vendas', 'exportacao', {
        'quantidade_vendas': len(vendas),
        'tipo_exportacao': 'vendas'
    })
    
    return send_file(output, 
                    download_name='vendas_ro_experience.xlsx',
                    as_attachment=True)

@app.route('/exportar-checkins')
@login_required
@permissao_required('exportacao')
def exportar_checkins():
    checkins = Cliente.query.filter_by(checkin_realizado=True).all()
    
    data = []
    for checkin in checkins:
        data.append({
            'CNPJ': checkin.cnpj,
            'Razão Social': checkin.razao_social,
            'Responsável Check-in': checkin.responsavel_checkin or checkin.responsavel,
            'Consultor': checkin.consultor,
            'Horário Check-in': checkin.horario_checkin.strftime('%d/%m/%Y %H:%M') if checkin.horario_checkin else ''
        })
    
    output = export_to_excel(data, 'checkins_ro_experience.xlsx', 'Check-ins')
    
    registrar_log('exportacao_checkins', 'exportacao', {
        'quantidade_checkins': len(checkins),
        'tipo_exportacao': 'checkins'
    })
    
    return send_file(output, 
                    download_name='checkins_ro_experience.xlsx',
                    as_attachment=True)

@app.route('/download-template')
@login_required
def download_template():
    data = [
        {
            'CNPJ': '12.345.678/0001-90',
            'RAZÃO SOCIAL': 'Empresa ABC Ltda',
            'RESPONSÁVEL': 'João Silva',
            'CONSULTOR': 'Consultor A',
            'DIREITO IMAGEM': 'SIM'
        },
        {
            'CNPJ': '12.345.678/0001-90',
            'RAZÃO SOCIAL': 'Empresa ABC Ltda',
            'RESPONSÁVEL': 'Maria Santos',
            'CONSULTOR': 'Consultor A',
            'DIREITO IMAGEM': 'NÃO'
        },
        {
            'CNPJ': '98.765.432/0001-10',
            'RAZÃO SOCIAL': 'Comércio DEF S/A',
            'RESPONSÁVEL': 'Pedro Oliveira',
            'CONSULTOR': 'Consultor B',
            'DIREITO IMAGEM': 'SIM'
        },
        {
            'CNPJ': '11.222.333/0001-44',
            'RAZÃO SOCIAL': 'Indústria GHI ME',
            'RESPONSÁVEL': 'Carlos Souza',
            'CONSULTOR': 'Consultor C',
            'DIREITO IMAGEM': 'NÃO'
        },
        {
            'CNPJ': '55.666.777/0001-88',
            'RAZÃO SOCIAL': 'Serviços JKL EIRELI',
            'RESPONSÁVEL': 'Ana Paula Santos',
            'CONSULTOR': 'Consultor A',
            'DIREITO IMAGEM': 'SIM'
        }
    ]
    
    output = export_to_excel(data, 'template_clientes_ro_experience.xlsx', 'Template_Clientes')
    
    registrar_log('template_download', 'exportacao', {
        'tipo_template': 'clientes_com_direito_imagem'
    })
    
    return send_file(
        output,
        download_name='template_clientes_ro_experience.xlsx',
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

@app.route('/login', methods=['GET', 'POST'])
def login():
    if 'usuario_id' in session:
        return redirect(url_for('index'))
    
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        usuario = Usuario.query.filter_by(username=username, ativo=True).first()
        
        if usuario and check_password_hash(usuario.password_hash, password):
            session['usuario_id'] = usuario.id
            session['username'] = usuario.username
            session['nome'] = usuario.nome
            session['nivel_acesso'] = usuario.nivel_acesso
            
            registrar_log('login', 'autenticacao', {
                'usuario': usuario.username,
                'nivel_acesso': usuario.nivel_acesso
            })
            
            return redirect(url_for('index'))
        else:
            return render_template('login.html', error='Credenciais inválidas')
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    if 'usuario_id' in session:
        registrar_log('logout', 'autenticacao', {
            'usuario': session.get('username')
        })
        
        session.clear()
    
    return redirect(url_for('login'))

@app.route('/alterar-senha', methods=['GET', 'POST'])
@login_required
def alterar_senha():
    if request.method == 'POST':
        senha_atual = request.form.get('senha_atual')
        nova_senha = request.form.get('nova_senha')
        confirmar_senha = request.form.get('confirmar_senha')
        
        usuario = Usuario.query.get(session['usuario_id'])
        
        if not check_password_hash(usuario.password_hash, senha_atual):
            return jsonify({'success': False, 'message': 'Senha atual incorreta'})
        
        if nova_senha != confirmar_senha:
            return jsonify({'success': False, 'message': 'As senhas não coincidem'})
        
        if len(nova_senha) < 6:
            return jsonify({'success': False, 'message': 'A senha deve ter pelo menos 6 caracteres'})
        
        usuario.password_hash = generate_password_hash(nova_senha)
        db.session.commit()
        
        registrar_log('alteracao_senha', 'autenticacao', {
            'usuario': usuario.username
        })
        
        return jsonify({'success': True, 'message': 'Senha alterada com sucesso!'})
    
    return render_template('alterar_senha.html')

@app.route('/gestao-usuarios')
@login_required
@admin_required
def gestao_usuarios():
    usuarios = Usuario.query.order_by(Usuario.data_criacao.desc()).all()
    return render_template('gestao_usuarios.html', 
                         usuarios=usuarios, 
                         modulos_sistema=MODULOS_SISTEMA)

@app.route('/criar-usuario', methods=['POST'])
@login_required
@admin_required
def criar_usuario():
    username = request.form.get('username')
    nome = request.form.get('nome')
    password = request.form.get('password')
    nivel_acesso = request.form.get('nivel_acesso', 'operador')
    permissoes_selecionadas = request.form.getlist('permissoes')
    
    if Usuario.query.filter_by(username=username).first():
        flash('Usuário já existe', 'error')
        return redirect(url_for('gestao_usuarios'))
    
    if len(password) < 6:
        flash('A senha deve ter pelo menos 6 caracteres', 'error')
        return redirect(url_for('gestao_usuarios'))
    
    permissoes = {}
    if nivel_acesso == 'admin':
        permissoes = {modulo: True for modulo in MODULOS_SISTEMA.keys()}
    else:
        permissoes = {modulo: (modulo in permissoes_selecionadas) for modulo in MODULOS_SISTEMA.keys()}
    
    usuario = Usuario(
        username=username,
        nome=nome,
        password_hash=generate_password_hash(password),
        nivel_acesso=nivel_acesso,
        permissoes=json.dumps(permissoes)
    )
    
    db.session.add(usuario)
    db.session.commit()
    
    registrar_log('criacao_usuario', 'usuarios', {
        'usuario_criado': username,
        'nivel_acesso': nivel_acesso,
        'permissoes': permissoes_selecionadas,
        'criado_por': session.get('username')
    })
    
    flash('Usuário criado com sucesso!', 'success')
    return redirect(url_for('gestao_usuarios'))

@app.route('/editar-usuario', methods=['POST'])
@login_required
@admin_required
def editar_usuario():
    usuario_id = request.form.get('usuario_id')
    nome = request.form.get('nome')
    nivel_acesso = request.form.get('nivel_acesso')
    password = request.form.get('password')
    permissoes_selecionadas = request.form.getlist('permissoes')
    
    usuario = Usuario.query.get(usuario_id)
    if not usuario:
        flash('Usuário não encontrado', 'error')
        return redirect(url_for('gestao_usuarios'))
    
    usuario.nome = nome
    usuario.nivel_acesso = nivel_acesso
    
    if nivel_acesso == 'admin':
        permissoes = {modulo: True for modulo in MODULOS_SISTEMA.keys()}
    else:
        permissoes = {modulo: (modulo in permissoes_selecionadas) for modulo in MODULOS_SISTEMA.keys()}
    
    usuario.permissoes = json.dumps(permissoes)
    
    if password and len(password) >= 6:
        usuario.password_hash = generate_password_hash(password)
    
    db.session.commit()
    
    registrar_log('edicao_usuario', 'usuarios', {
        'usuario_editado': usuario.username,
        'nivel_acesso': nivel_acesso,
        'permissoes': permissoes_selecionadas,
        'editado_por': session.get('username')
    })
    
    flash('Usuário atualizado com sucesso!', 'success')
    return redirect(url_for('gestao_usuarios'))

@app.route('/alternar-status-usuario/<int:usuario_id>', methods=['POST'])
@login_required
@admin_required
def alternar_status_usuario(usuario_id):
    usuario = Usuario.query.get(usuario_id)
    if not usuario:
        return jsonify({'success': False, 'message': 'Usuário não encontrado'})
    
    if usuario.id == session['usuario_id']:
        return jsonify({'success': False, 'message': 'Não é possível inativar seu próprio usuário'})
    
    usuario.ativo = not usuario.ativo
    db.session.commit()
    
    acao = 'ativado' if usuario.ativo else 'inativado'
    
    registrar_log(f'usuario_{acao}', 'usuarios', {
        'usuario': usuario.username,
        'acao': acao,
        'realizado_por': session.get('username')
    })
    
    return jsonify({'success': True, 'message': f'Usuário {acao} com sucesso!'})

@app.route('/api/usuario/<int:usuario_id>')
@login_required
@admin_required
def api_usuario(usuario_id):
    usuario = Usuario.query.get(usuario_id)
    if usuario:
        return jsonify({
            'id': usuario.id,
            'username': usuario.username,
            'nome': usuario.nome,
            'nivel_acesso': usuario.nivel_acesso,
            'ativo': usuario.ativo
        })
    return jsonify({'error': 'Usuário não encontrado'}), 404

@app.route('/excluir-usuario/<int:usuario_id>', methods=['POST'])
@login_required
@admin_required
def excluir_usuario(usuario_id):
    usuario = Usuario.query.get(usuario_id)
    if not usuario:
        return jsonify({'success': False, 'message': 'Usuário não encontrado'})
    
    if usuario.id == session['usuario_id']:
        return jsonify({'success': False, 'message': 'Não é possível excluir seu próprio usuário'})
    
    if usuario.nivel_acesso == 'admin':
        total_admins = Usuario.query.filter_by(nivel_acesso='admin', ativo=True).count()
        if total_admins <= 1:
            return jsonify({'success': False, 'message': 'Não é possível excluir o último administrador do sistema'})
    
    registrar_log('exclusao_usuario', 'usuarios', {
        'usuario_excluido': usuario.username,
        'nome': usuario.nome,
        'nivel_acesso': usuario.nivel_acesso,
        'excluido_por': session.get('username')
    })
    
    db.session.delete(usuario)
    db.session.commit()
    
    return jsonify({'success': True, 'message': 'Usuário excluído com sucesso!'})

@app.route('/api/log-detalhes/<int:log_id>')
@login_required
@admin_required
def api_log_detalhes(log_id):
    log = LogAuditoria.query.get(log_id)
    if log:
        return jsonify({
            'id': log.id,
            'dados': log.dados
        })
    return jsonify({'error': 'Log não encontrado'}), 404

@app.context_processor
def utility_processor():
    def check_permission_template(modulo):
        return tem_permissao(modulo)
    return dict(tem_permissao=check_permission_template)

@app.route('/api/metricas-ultima-hora')
@login_required
def api_metricas_ultima_hora():
    uma_hora_atras = agora() - timedelta(hours=1)
    
    checkins_ultima_hora = Cliente.query.filter(
        Cliente.horario_checkin >= uma_hora_atras
    ).count()
    
    vendas_ultima_hora = Venda.query.filter(
        Venda.data_hora_venda >= uma_hora_atras
    ).count()
    
    sorteios_ultima_hora = Sorteio.query.filter(
        Sorteio.data_sorteio >= uma_hora_atras
    ).count()
    
    return jsonify({
        'checkins': checkins_ultima_hora,
        'vendas': vendas_ultima_hora,
        'sorteios': sorteios_ultima_hora
    })

@app.route('/logs-auditoria')
@login_required
@admin_required
def logs_auditoria():
    logs = LogAuditoria.query.order_by(LogAuditoria.data_hora.desc()).limit(100).all()
    usuarios = Usuario.query.all()
    return render_template('logs_auditoria.html', logs=logs, usuarios=usuarios, now=agora)

@app.route('/exportar-logs')
@login_required
@admin_required
def exportar_logs():
    logs = LogAuditoria.query.order_by(LogAuditoria.data_hora.desc()).all()
    
    data = []
    for log in logs:
        dados_formatados = ""
        if log.dados:
            try:
                dados_dict = json.loads(log.dados)
                dados_formatados = json.dumps(dados_dict, ensure_ascii=False, indent=2)
            except:
                dados_formatados = log.dados
        
        data.append({
            'ID': log.id,
            'Data/Hora': log.data_hora.strftime('%d/%m/%Y %H:%M:%S'),
            'Usuário': log.usuario.username if log.usuario else 'Sistema',
            'Nome Usuário': log.usuario.nome if log.usuario and log.usuario.nome else 'N/A',
            'Módulo': log.modulo.upper(),
            'Ação': log.acao,
            'IP': log.ip,
            'Dados': dados_formatados
        })
    
    output = export_to_excel(data, 'logs_auditoria_ro_experience.xlsx', 'Logs de Auditoria')
    
    registrar_log('exportacao_logs', 'logs', {
        'quantidade_logs': len(logs),
        'tipo_exportacao': 'excel'
    })
    
    return send_file(
        output,
        download_name='logs_auditoria_ro_experience.xlsx',
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

@app.route('/pesquisa', methods=['GET', 'POST'])
def pesquisa_publica():
    if request.method == 'POST':
        try:
            organizacao = int(request.form.get('organizacao'))
            palestras = int(request.form.get('palestras'))
            atendimento = int(request.form.get('atendimento'))
            futuro = int(request.form.get('futuro'))
            comentarios = request.form.get('comentarios', '').strip()
            cnpj = request.form.get('cnpj', '').strip()
            
            print(f"📝 Dados recebidos - CNPJ: '{cnpj}'")
            
            if not all([organizacao, palestras, atendimento, futuro]):
                flash('Por favor, responda todas as questões obrigatórias', 'error')
                return render_template('pesquisa_publica.html', enviado=False)
            
            anonima = True
            cnpj_identificado = None
            razao_social = None
            
            if cnpj:
                print(f"🔍 Validando CNPJ: {cnpj}")
                cnpj_limpo = ''.join(filter(str.isdigit, cnpj))
                
                if len(cnpj_limpo) == 14:
                    cliente = Cliente.query.filter_by(cnpj=cnpj_limpo).first()
                    
                    if not cliente:
                        cliente = Cliente.query.filter_by(cnpj=normalizar_cnpj(cnpj_limpo)).first()
                    
                    if not cliente:
                        todos_clientes = Cliente.query.all()
                        for cli in todos_clientes:
                            cli_cnpj_limpo = ''.join(filter(str.isdigit, cli.cnpj))
                            if cli_cnpj_limpo == cnpj_limpo:
                                cliente = cli
                                break
                    
                    if cliente:
                        print(f"✅ Cliente encontrado: {cliente.razao_social}")
                        if cliente.checkin_realizado:
                            anonima = False
                            cnpj_identificado = cnpj_limpo
                            razao_social = cliente.razao_social
                            print(f"📋 Resposta IDENTIFICADA: {razao_social}")
                        else:
                            print("⚠️ Cliente encontrado mas sem check-in")
                    else:
                        print("❌ Cliente não encontrado")
                else:
                    print("❌ CNPJ inválido (não tem 14 dígitos)")
            
            print(f"🎯 Tipo de resposta: {'ANÔNIMA' if anonima else 'IDENTIFICADA'}")
            
            resposta = PesquisaResposta(
                cnpj_identificado=cnpj_identificado,
                razao_social=razao_social,
                organizacao=organizacao,
                palestras=palestras,
                atendimento=atendimento,
                futuro=futuro,
                comentarios=comentarios if comentarios else None,
                ip=request.remote_addr,
                anonima=anonima
            )
            
            db.session.add(resposta)
            db.session.commit()
            
            registrar_log('pesquisa_respondida', 'pesquisa', {
                'resposta_id': resposta.id,
                'anonima': anonima,
                'cnpj': cnpj_identificado,
                'razao_social': razao_social,
                'organizacao': organizacao,
                'palestras': palestras,
                'atendimento': atendimento,
                'futuro': futuro
            })
            
            return render_template('pesquisa_publica.html', enviado=True)
            
        except Exception as e:
            db.session.rollback()
            print(f"❌ Erro ao salvar pesquisa: {str(e)}")
            flash(f'Erro ao enviar pesquisa: {str(e)}', 'error')
            return render_template('pesquisa_publica.html', enviado=False)
    
    return render_template('pesquisa_publica.html', enviado=False)

@app.route('/api/validar-cnpj-pesquisa/', defaults={'cnpj': ''})
@app.route('/api/validar-cnpj-pesquisa/<path:cnpj>')
def api_validar_cnpj_pesquisa(cnpj):
    try:
        from urllib.parse import unquote
        cnpj = unquote(cnpj)
        
        cnpj_limpo = ''.join(filter(str.isdigit, cnpj))
        
        print(f"🔍 Validando CNPJ: {cnpj} -> {cnpj_limpo}")
        
        if len(cnpj_limpo) != 14:
            return jsonify({'valido': False, 'mensagem': 'CNPJ deve ter 14 dígitos'})
        
        cnpj_formatado = normalizar_cnpj_pesquisa(cnpj_limpo)
        
        cliente = Cliente.query.filter_by(cnpj=cnpj_limpo).first()
        
        if not cliente:
            cliente = Cliente.query.filter_by(cnpj=normalizar_cnpj(cnpj_limpo)).first()
        
        if not cliente:
            cliente = Cliente.query.filter_by(cnpj=cnpj_formatado).first()
        
        if not cliente:
            todos_clientes = Cliente.query.all()
            for cli in todos_clientes:
                cli_cnpj_limpo = ''.join(filter(str.isdigit, cli.cnpj))
                if cli_cnpj_limpo == cnpj_limpo:
                    cliente = cli
                    break
        
        if cliente:
            print(f"📋 Cliente encontrado: {cliente.razao_social}")
            print(f"✅ Check-in realizado: {cliente.checkin_realizado}")
            print(f"📝 CNPJ no banco: {cliente.cnpj}")
            
            if cliente.checkin_realizado:
                return jsonify({
                    'valido': True, 
                    'mensagem': f'CNPJ validado - {cliente.razao_social}'
                })
            else:
                return jsonify({
                    'valido': False, 
                    'mensagem': 'CNPJ encontrado mas não fez check-in no evento'
                })
        else:
            return jsonify({
                'valido': False, 
                'mensagem': 'Esse CNPJ não fez Check-in no evento, use o CNPJ do seu crachá'
            })
            
    except Exception as e:
        print(f"❌ Erro na validação: {str(e)}")
        return jsonify({'valido': False, 'mensagem': f'Erro na validação: {str(e)}'})
    
@app.route('/relatorio-pesquisas')
@login_required
@permissao_required('relatorios')
def relatorio_pesquisas():
    pesquisas = PesquisaResposta.query.order_by(PesquisaResposta.data_resposta.desc()).all()
    
    total_pesquisas = len(pesquisas)
    pesquisas_identificadas = len([p for p in pesquisas if not p.anonima])
    pesquisas_anonimas = len([p for p in pesquisas if p.anonima])
    
    if total_pesquisas > 0:
        media_organizacao = sum(p.organizacao for p in pesquisas) / total_pesquisas
        media_palestras = sum(p.palestras for p in pesquisas) / total_pesquisas
        media_atendimento = sum(p.atendimento for p in pesquisas) / total_pesquisas
        media_futuro = sum(p.futuro for p in pesquisas) / total_pesquisas
    else:
        media_organizacao = media_palestras = media_atendimento = media_futuro = 0
    
    return render_template('relatorio_pesquisas.html',
                         pesquisas=pesquisas,
                         total_pesquisas=total_pesquisas,
                         pesquisas_identificadas=pesquisas_identificadas,
                         pesquisas_anonimas=pesquisas_anonimas,
                         media_organizacao=media_organizacao,
                         media_palestras=media_palestras,
                         media_atendimento=media_atendimento,
                         media_futuro=media_futuro)

@app.route('/exportar-pesquisas')
@login_required
@permissao_required('exportacao')
def exportar_pesquisas():
    pesquisas = PesquisaResposta.query.order_by(PesquisaResposta.data_resposta.desc()).all()
    
    data = []
    for pesquisa in pesquisas:
        data.append({
            'ID': pesquisa.id,
            'Data/Hora': pesquisa.data_resposta.strftime('%d/%m/%Y %H:%M'),
            'Tipo': 'Anônima' if pesquisa.anonima else 'Identificada',
            'CNPJ': pesquisa.cnpj_identificado or 'N/A',
            'Razão Social': pesquisa.razao_social or 'N/A',
            'Organização (1-10)': pesquisa.organizacao,
            'Palestras (1-10)': pesquisa.palestras,
            'Atendimento (1-10)': pesquisa.atendimento,
            'Futuro (1-10)': pesquisa.futuro,
            'Comentários': pesquisa.comentarios or 'Nenhum',
            'IP': pesquisa.ip
        })
    
    output = export_to_excel(data, 'pesquisas_satisfacao.xlsx', 'Pesquisas')
    
    registrar_log('exportacao_pesquisas', 'exportacao', {
        'quantidade_pesquisas': len(pesquisas)
    })
    
    return send_file(output,
                    download_name='pesquisas_satisfacao_ro_experience.xlsx',
                    as_attachment=True,
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

if __name__ == '__main__':
    with app.app_context():
        try:
            db.create_all()
            criar_usuario_admin()
            migrar_banco_dados()
            atualizar_faturamento_sorteio()
            print("✅ Banco de dados PostgreSQL configurado com sucesso!")
        except Exception as e:
            print(f"❌ Erro ao conectar com PostgreSQL: {e}")
            print("🔧 Verifique a string de conexão")
    
    # Configurações para desenvolvimento local
    host = '0.0.0.0'
    
    if os.environ.get('SQUARECLOUD') or os.environ.get('PORT'):
        port = int(os.environ.get('PORT', 80))
        debug = False
        environment = "SquareCloud"
    else:
        port = 33053
        debug = True
        environment = "Desenvolvimento Local"
    
    print(f"🎯 R.O Experience 2025 - Servidor Iniciado!")
    print(f"📍 Host: {host}")
    print(f"🔧 Porta: {port}")
    print(f"🌐 Ambiente: {environment}")
    print(f"🐛 Debug: {debug}")
    print(f"🗄️  Banco: PostgreSQL")
    print("🚀 Aplicação rodando!")
    print("")
    
    app.run(host=host, port=port, debug=debug)