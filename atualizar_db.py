#!/usr/bin/env python3
"""
Script para atualizar o banco de dados com as novas colunas da pesquisa
Execute: python atualizar_db_pesquisa.py
"""

import sys
import os

# Adiciona o diretório atual ao path para importar o app
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app import app, db
from sqlalchemy import text

def atualizar_tabela_pesquisa():
    """Adiciona as novas colunas à tabela pesquisa_resposta"""
    
    print("🔄 Iniciando atualização do banco de dados...")
    
    with app.app_context():
        try:
            # Verifica se a tabela existe
            result = db.session.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'pesquisa_resposta'
                );
            """))
            tabela_existe = result.scalar()
            
            if not tabela_existe:
                print("❌ Tabela 'pesquisa_resposta' não existe!")
                return False
            
            # Lista de novas colunas para adicionar
            novas_colunas = [
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
            
            for coluna, tipo in novas_colunas:
                # Verifica se a coluna já existe
                result = db.session.execute(text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name='pesquisa_resposta' AND column_name=:coluna
                """), {'coluna': coluna})
                
                if result.fetchone() is None:
                    # Coluna não existe, vamos adicionar
                    print(f"  ➕ Adicionando coluna: {coluna}")
                    
                    db.session.execute(text(f"""
                        ALTER TABLE pesquisa_resposta 
                        ADD COLUMN {coluna} {tipo}
                    """))
                    colunas_adicionadas += 1
                else:
                    print(f"  ✅ Coluna já existe: {coluna}")
            
            if colunas_adicionadas > 0:
                db.session.commit()
                print(f"🎉 {colunas_adicionadas} novas colunas adicionadas com sucesso!")
            else:
                print("✅ Todas as colunas já existem no banco!")
            
            # Verifica a estrutura final da tabela
            print("\n📋 Estrutura atual da tabela 'pesquisa_resposta':")
            result = db.session.execute(text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = 'pesquisa_resposta'
                ORDER BY ordinal_position;
            """))
            
            for coluna, tipo, nullable in result:
                print(f"  - {coluna}: {tipo} ({'NULL' if nullable == 'YES' else 'NOT NULL'})")
            
            return True
            
        except Exception as e:
            db.session.rollback()
            print(f"❌ Erro durante a atualização: {e}")
            return False

def migrar_dados_existentes():
    """Migra dados existentes das colunas antigas para as novas (se necessário)"""
    
    print("\n🔄 Verificando migração de dados existentes...")
    
    with app.app_context():
        try:
            # Verifica se existem registros com as colunas antigas
            result = db.session.execute(text("""
                SELECT COUNT(*) FROM pesquisa_resposta 
                WHERE organizacao IS NOT NULL 
                AND palestras IS NOT NULL 
                AND atendimento IS NOT NULL 
                AND futuro IS NOT NULL
            """))
            registros_antigos = result.scalar()
            
            if registros_antigos > 0:
                print(f"📦 Encontrados {registros_antigos} registros com estrutura antiga")
                
                # Aqui você pode adicionar lógica para migrar dados se necessário
                # Por exemplo, copiar valores de colunas antigas para novas
                
                print("💡 Os registros antigos manterão as colunas originais")
                print("💡 Novos registros usarão a nova estrutura")
            else:
                print("✅ Nenhum registro com estrutura antiga encontrado")
                
            return True
            
        except Exception as e:
            print(f"⚠️ Aviso na migração: {e}")
            return True  # Não é crítico

def main():
    """Função principal"""
    
    print("=" * 60)
    print("🛠️  ATUALIZADOR DE BANCO - PESQUISA DE SATISFAÇÃO")
    print("=" * 60)
    
    # Atualiza a estrutura da tabela
    if atualizar_tabela_pesquisa():
        # Migra dados existentes (se houver)
        migrar_dados_existentes()
        
        print("\n" + "=" * 60)
        print("✅ ATUALIZAÇÃO CONCLUÍDA COM SUCESSO!")
        print("=" * 60)
        print("\n📝 Próximos passos:")
        print("1. ✅ Banco de dados atualizado")
        print("2. 🚀 Reinicie o servidor Flask")
        print("3. 🌐 Teste o formulário de pesquisa")
        print("4. 📊 Verifique o relatório de pesquisas")
    else:
        print("\n❌ Falha na atualização do banco de dados!")
        sys.exit(1)

if __name__ == "__main__":
    main()