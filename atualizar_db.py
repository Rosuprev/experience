#!/usr/bin/env python3
"""
ATUALIZADOR GERAL DO BANCO DE DADOS - R.O Experience 2025
Este script será usado para TODAS as atualizações futuras do banco.

Execute: python atualizador_db.py
"""

import sys
import os
from datetime import datetime

# Adiciona o diretório atual ao path para importar o app
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app import app, db
from sqlalchemy import text, inspect

class AtualizadorBanco:
    def __init__(self):
        self.versao_atual = "1.1"  # Versão atual do schema
        self.migrations_executadas = []
    
    def log_migracao(self, mensagem):
        """Registra uma migração executada"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {mensagem}"
        self.migrations_executadas.append(log_entry)
        print(f"  📝 {mensagem}")
    
    def verificar_tabela_versao(self):
        """Verifica/Cria tabela de controle de versão"""
        try:
            result = db.session.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'db_versao'
                );
            """))
            
            if not result.scalar():
                # Cria tabela de controle de versão
                db.session.execute(text("""
                    CREATE TABLE db_versao (
                        id SERIAL PRIMARY KEY,
                        versao VARCHAR(20) NOT NULL,
                        data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        migrations_executadas TEXT
                    );
                """))
                self.log_migracao("Tabela de controle de versão criada")
            
            return True
        except Exception as e:
            print(f"❌ Erro ao verificar tabela versão: {e}")
            return False
    
    def get_versao_atual_db(self):
        """Obtém a versão atual do banco"""
        try:
            result = db.session.execute(text("SELECT versao FROM db_versao ORDER BY id DESC LIMIT 1;"))
            versao = result.scalar()
            return versao if versao else "1.0"  # Versão inicial
        except:
            return "1.0"
    
    def registrar_versao(self):
        """Registra a nova versão no banco"""
        try:
            migrations_text = "\n".join(self.migrations_executadas)
            db.session.execute(text("""
                INSERT INTO db_versao (versao, migrations_executadas) 
                VALUES (:versao, :migrations);
            """), {'versao': self.versao_atual, 'migrations': migrations_text})
            db.session.commit()
            self.log_migracao(f"Versão {self.versao_atual} registrada no banco")
        except Exception as e:
            print(f"❌ Erro ao registrar versão: {e}")
    
    def migracao_1_0_para_1_1(self):
        """Migração da versão 1.0 para 1.1 - Novas colunas da pesquisa"""
        
        print("\n🔄 Executando migração 1.0 → 1.1 - Pesquisa de Satisfação")
        
        # Lista de novas colunas para a pesquisa
        novas_colunas_pesquisa = [
            ('comunicacao', 'INTEGER NOT NULL DEFAULT 0'),
            ('formato_evento', 'INTEGER NOT NULL DEFAULT 0'),
            ('alimentacao', 'INTEGER NOT NULL DEFAULT 0'),
            ('palestra_reforma', 'INTEGER NOT NULL DEFAULT 0'),
            ('palestra_estrategia', 'INTEGER NOT NULL DEFAULT 0'),
            ('interacao_brother', 'INTEGER NOT NULL DEFAULT 0'),
            ('interacao_canon', 'INTEGER NOT NULL DEFAULT 0'),
            ('interacao_epson', 'INTEGER NOT NULL DEFAULT 0'),
            ('interacao_hp', 'INTEGER NOT NULL DEFAULT 0'),
            ('interacao_konica', 'INTEGER NOT NULL DEFAULT 0'),
            ('interacao_kyocera', 'INTEGER NOT NULL DEFAULT 0'),
            ('prazo_entrega', 'INTEGER NOT NULL DEFAULT 0'),
            ('frete', 'INTEGER NOT NULL DEFAULT 0')
        ]
        
        colunas_adicionadas = 0
        
        for coluna, tipo in novas_colunas_pesquisa:
            try:
                # Verifica se a coluna já existe
                result = db.session.execute(text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name='pesquisa_resposta' AND column_name=:coluna
                """), {'coluna': coluna})
                
                if result.fetchone() is None:
                    # Coluna não existe, vamos adicionar
                    db.session.execute(text(f"""
                        ALTER TABLE pesquisa_resposta 
                        ADD COLUMN {coluna} {tipo}
                    """))
                    colunas_adicionadas += 1
                    self.log_migracao(f"Coluna '{coluna}' adicionada à pesquisa_resposta")
                    
            except Exception as e:
                print(f"⚠️ Erro ao adicionar coluna {coluna}: {e}")
        
        if colunas_adicionadas > 0:
            self.log_migracao(f"Total de {colunas_adicionadas} novas colunas adicionadas")
        else:
            self.log_migracao("Todas as colunas já existiam")
        
        return True
    
    def migracao_futura_1_1_para_1_2(self):
        """EXEMPLO: Migração futura da versão 1.1 para 1.2"""
        # Esta é uma migração de exemplo para futuras atualizações
        # Quando precisar adicionar novas funcionalidades, edite aqui
        
        print("\n📋 Migração futura 1.1 → 1.2 (EXEMPLO)")
        
        # Exemplo: Adicionar nova tabela
        # try:
        #     db.session.execute(text("""
        #         CREATE TABLE IF NOT EXISTS nova_tabela (
        #             id SERIAL PRIMARY KEY,
        #             nome VARCHAR(100) NOT NULL,
        #             data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        #         );
        #     """))
        #     self.log_migracao("Nova tabela 'nova_tabela' criada")
        # except Exception as e:
        #     print(f"⚠️ Erro ao criar nova tabela: {e}")
        
        self.log_migracao("Migração futura preparada (comentada)")
        return True
    
    def executar_migracoes(self):
        """Executa todas as migrações necessárias baseado na versão atual"""
        
        versao_db = self.get_versao_atual_db()
        print(f"📊 Versão atual do banco: {versao_db}")
        print(f"🎯 Versão alvo: {self.versao_atual}")
        
        if versao_db == self.versao_atual:
            print("✅ Banco já está na versão mais recente!")
            return True
        
        # Executa migrações em sequência
        if versao_db == "1.0":
            if not self.migracao_1_0_para_1_1():
                return False
            versao_db = "1.1"
        
        if versao_db == "1.1" and self.versao_atual == "1.2":
            if not self.migracao_futura_1_1_para_1_2():
                return False
            versao_db = "1.2"
        
        # Registra a nova versão
        self.registrar_versao()
        
        return True
    
    def verificar_estrutura_tabelas(self):
        """Verifica a estrutura de todas as tabelas (apenas informativo)"""
        
        print("\n🔍 Estrutura das tabelas principais:")
        
        tabelas_principais = [
            'pesquisa_resposta', 'cliente', 'venda', 'sorteio', 
            'brinde', 'usuario', 'log_auditoria'
        ]
        
        inspector = inspect(db.engine)
        
        for tabela in tabelas_principais:
            try:
                colunas = inspector.get_columns(tabela)
                print(f"\n📋 {tabela.upper()} ({len(colunas)} colunas):")
                for coluna in colunas:
                    nullable = "NULL" if coluna['nullable'] else "NOT NULL"
                    print(f"   - {coluna['name']}: {coluna['type']} ({nullable})")
            except Exception as e:
                print(f"   ⚠️ Tabela {tabela} não encontrada ou erro: {e}")
    
    def executar(self):
        """Função principal do atualizador"""
        
        print("=" * 70)
        print("🛠️  ATUALIZADOR GERAL DO BANCO - R.O Experience 2025")
        print("=" * 70)
        
        with app.app_context():
            try:
                # Verifica/Cria tabela de controle de versão
                if not self.verificar_tabela_versao():
                    return False
                
                # Executa migrações necessárias
                if not self.executar_migracoes():
                    return False
                
                # Mostra estrutura das tabelas (informativo)
                self.verificar_estrutura_tabelas()
                
                print("\n" + "=" * 70)
                print("✅ ATUALIZAÇÃO CONCLUÍDA COM SUCESSO!")
                print("=" * 70)
                
                if self.migrations_executadas:
                    print("\n📋 Migrações executadas:")
                    for migracao in self.migrations_executadas:
                        print(f"   {migracao}")
                
                print(f"\n🎯 Banco na versão: {self.versao_atual}")
                print("🚀 Próximos passos:")
                print("   1. Reinicie o servidor Flask")
                print("   2. Teste as novas funcionalidades")
                print("   3. Verifique os logs se necessário")
                
                return True
                
            except Exception as e:
                db.session.rollback()
                print(f"❌ Erro crítico durante a atualização: {e}")
                return False

def main():
    """Executa o atualizador"""
    atualizador = AtualizadorBanco()
    success = atualizador.executar()
    
    if not success:
        print("\n❌ ATUALIZAÇÃO FALHOU!")
        sys.exit(1)

if __name__ == "__main__":
    main()