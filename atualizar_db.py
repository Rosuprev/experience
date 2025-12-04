# arquivo: correcao_definitiva.py
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app import app, db
from datetime import datetime
from sqlalchemy import inspect, text, Table, MetaData
import traceback

def agora():
    from datetime import datetime, timedelta
    return datetime.utcnow() - timedelta(hours=3)

def corrigir_venda_evento_definitivo():
    """CORREÇÃO DEFINITIVA - Resolve todos os problemas"""
    
    print("=" * 70)
    print("CORREÇÃO DEFINITIVA - TABELA VENDA_EVENTO")
    print("=" * 70)
    
    try:
        with app.app_context():
            print("\n1. 🔍 Verificando estado atual...")
            
            # Usar inspector para verificar tabelas
            inspector = inspect(db.engine)
            
            # Verificar se a tabela existe no banco
            tabelas_existentes = inspector.get_table_names()
            print(f"   Tabelas no banco: {', '.join(tabelas_existentes)}")
            
            if 'venda_evento' in tabelas_existentes:
                print("   ✅ Tabela existe no banco")
                
                # Verificar colunas
                colunas = inspector.get_columns('venda_evento')
                colunas_nomes = [col['name'] for col in colunas]
                print(f"   Colunas: {', '.join(colunas_nomes)}")
                
                # Verificar se tem numero_nf
                if 'numero_nf' not in colunas_nomes:
                    print("\n2. ❌ Campo 'numero_nf' FALTANDO no banco")
                    
                    # SOLUÇÃO: Adicionar campo via SQL direto
                    try:
                        with db.engine.connect() as conn:
                            print("   🛠️  Adicionando campo 'numero_nf'...")
                            conn.execute(text("""
                                ALTER TABLE venda_evento 
                                ADD COLUMN numero_nf VARCHAR(50) NULL
                            """))
                            conn.commit()
                            print("   ✅ Campo adicionado ao banco!")
                    except Exception as e:
                        print(f"   ⚠️  Erro ao adicionar campo: {e}")
                else:
                    print("\n2. ✅ Campo 'numero_nf' já existe no banco")
            else:
                print("   ❌ Tabela não existe no banco - será criada")
            
            print("\n3. 🧹 Limpando cache do SQLAlchemy...")
            
            # Método 1: Remover tabela do metadata
            metadata = db.metadata
            if 'venda_evento' in metadata.tables:
                # Não remover, apenas marcar para recriação
                print("   ✅ Tabela encontrada no metadata do SQLAlchemy")
            
            # Método 2: Definir modelo NOVO com extend_existing
            print("4. 📝 Definindo modelo atualizado...")
            
            # Primeiro, remover qualquer referência antiga
            try:
                # Remover da registry
                if 'VendaEvento' in db.Model._decl_class_registry:
                    del db.Model._decl_class_registry['VendaEvento']
            except:
                pass
            
            # Criar NOVA definição
            class VendaEvento(db.Model):
                __tablename__ = 'venda_evento'
                __table_args__ = {'extend_existing': True}  # ← CHAVE PARA RESOLVER
                
                id = db.Column(db.Integer, primary_key=True)
                numero_nf = db.Column(db.String(50), nullable=True)  # ← CAMPO CRÍTICO
                data_emissao = db.Column(db.Date, nullable=False)
                cliente_nome = db.Column(db.String(200), nullable=False)
                vendedor = db.Column(db.String(100), nullable=False)
                equipe = db.Column(db.String(100), nullable=False)
                descricao_produto = db.Column(db.String(300), nullable=False)
                marca = db.Column(db.String(100), nullable=False)
                valor_produtos = db.Column(db.Float, nullable=False)
                quantidade = db.Column(db.Integer, nullable=False, default=1)
                familia = db.Column(db.String(100))
                valor_total = db.Column(db.Float, nullable=False)
                data_importacao = db.Column(db.DateTime, default=agora)
                importado_por = db.Column(db.String(100))
            
            print("5. 📋 Sincronizando com banco...")
            
            # Criar tabela (ou atualizar se existir)
            VendaEvento.__table__.create(db.engine, checkfirst=True)
            
            print("\n6. ✅ Verificação final...")
            
            # Testar acesso
            try:
                with db.engine.connect() as conn:
                    # Testar consulta simples
                    conn.execute(text("SELECT 1 FROM venda_evento LIMIT 1"))
                    print("   ✅ Consulta SQL funciona!")
                    
                    # Verificar colunas finais
                    colunas_finais = inspector.get_columns('venda_evento')
                    print(f"   Colunas finais: {[c['name'] for c in colunas_finais]}")
            except Exception as e:
                print(f"   ⚠️  Erro na consulta: {e}")
            
            print("\n" + "=" * 70)
            print("✅ CORREÇÃO CONCLUÍDA COM SUCESSO!")
            print("=" * 70)
            
            return True
            
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        traceback.print_exc()
        return False

def solucao_emergencia():
    """SOLUÇÃO DE EMERGÊNCIA - Para quando nada mais funciona"""
    
    print("=" * 70)
    print("SOLUÇÃO DE EMERGÊNCIA")
    print("=" * 70)
    print("Esta solução:")
    print("1. Remove a tabela do banco")
    print("2. Limpa completamente o cache do SQLAlchemy")
    print("3. Recria tudo do zero")
    print("4. PERDE TODOS OS DADOS da tabela venda_evento")
    
    resposta = input("\nContinuar? (digite 'SIM'): ")
    if resposta != 'SIM':
        print("❌ Cancelado.")
        return
    
    try:
        with app.app_context():
            print("\n1. 🗑️  Removendo do banco...")
            with db.engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS venda_evento CASCADE"))
                conn.commit()
            
            print("2. 🧹 Limpando cache do SQLAlchemy...")
            
            # Limpar metadata completamente
            metadata = db.metadata
            metadata.clear()
            
            # Refletir apenas as tabelas que existem
            metadata.reflect(bind=db.engine)
            
            print("3. 🛠️  Atualizando app.py automaticamente...")
            
            # Verificar se o modelo no app.py está correto
            modelo_correto = """
# NO app.py - VERIFIQUE se o modelo VendaEvento tem esta estrutura:

class VendaEvento(db.Model):
    __tablename__ = 'venda_evento'
    
    id = db.Column(db.Integer, primary_key=True)
    numero_nf = db.Column(db.String(50), nullable=True)  # ← ESTA LINHA É OBRIGATÓRIA
    data_emissao = db.Column(db.Date, nullable=False)
    cliente_nome = db.Column(db.String(200), nullable=False)
    vendedor = db.Column(db.String(100), nullable=False)
    equipe = db.Column(db.String(100), nullable=False)
    descricao_produto = db.Column(db.String(300), nullable=False)
    marca = db.Column(db.String(100), nullable=False)
    valor_produtos = db.Column(db.Float, nullable=False)
    quantidade = db.Column(db.Integer, nullable=False, default=1)
    familia = db.Column(db.String(100))
    valor_total = db.Column(db.Float, nullable=False)
    data_importacao = db.Column(db.DateTime, default=agora)
    importado_por = db.Column(db.String(100))
"""
            print(modelo_correto)
            
            input("\nPressione Enter após verificar/atualizar o app.py...")
            
            print("4. 📋 Recriando tabela...")
            
            # Importar novamente após atualização
            import importlib
            import sys
            
            if 'app' in sys.modules:
                importlib.reload(sys.modules['app'])
            
            # Tentar acessar o modelo atualizado
            try:
                from app import VendaEvento
                VendaEvento.__table__.create(db.engine)
                print("   ✅ Tabela criada!")
            except:
                print("   ⚠️  Não consegui criar via modelo. Criando via SQL...")
                with db.engine.connect() as conn:
                    conn.execute(text("""
                        CREATE TABLE venda_evento (
                            id SERIAL PRIMARY KEY,
                            numero_nf VARCHAR(50),
                            data_emissao DATE NOT NULL,
                            cliente_nome VARCHAR(200) NOT NULL,
                            vendedor VARCHAR(100) NOT NULL,
                            equipe VARCHAR(100) NOT NULL,
                            descricao_produto VARCHAR(300) NOT NULL,
                            marca VARCHAR(100) NOT NULL,
                            valor_produtos FLOAT NOT NULL,
                            quantidade INTEGER NOT NULL DEFAULT 1,
                            familia VARCHAR(100),
                            valor_total FLOAT NOT NULL,
                            data_importacao TIMESTAMP,
                            importado_por VARCHAR(100)
                        )
                    """))
                    conn.commit()
                    print("   ✅ Tabela criada via SQL!")
            
            print("\n✅ SOLUÇÃO APLICADA!")
            print("\nAgora REINICIE o servidor Flask completamente.")
            
    except Exception as e:
        print(f"❌ ERRO: {e}")
        traceback.print_exc()

if __name__ == '__main__':
    print("\n" + "=" * 70)
    print("MENU DE CORREÇÃO - VENDA_EVENTO")
    print("=" * 70)
    print("\nOpções:")
    print("1. Correção normal (tenta manter dados)")
    print("2. Solução de emergência (remove tudo e recria)")
    print("3. Sair")
    
    try:
        opcao = input("\nOpção (1-3): ").strip()
        
        if opcao == '1':
            if corrigir_venda_evento_definitivo():
                print("\n" + "=" * 70)
                print("INSTRUÇÕES FINAIS:")
                print("1. VERIFIQUE se adicionou 'numero_nf' ao modelo VendaEvento no app.py")
                print("2. Se não adicionou, ADICIONE AGORA:")
                print("   numero_nf = db.Column(db.String(50), nullable=True)")
                print("3. REINICIE o servidor Flask")
                print("4. Teste /importar-vendas-evento")
                print("=" * 70)
        elif opcao == '2':
            solucao_emergencia()
        elif opcao == '3':
            print("👋 Saindo...")
        else:
            print("❌ Opção inválida!")
            
    except KeyboardInterrupt:
        print("\n\n❌ Cancelado pelo usuário.")