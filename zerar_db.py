# zerar_tudo_simples.py
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app import app, db
from app import (
    Usuario, Cliente, Venda, Brinde, Sorteio, FaturamentoSorteio, 
    Estoque, VendaEquipamento, PesquisaResposta, PesquisaMarketing, LogAuditoria
)

def zerar_tudo_exceto_usuarios():
    """Zera todas as tabelas exceto usuários (versão simples)"""
    
    with app.app_context():
        try:
            print("🔄 Zerando banco de dados (mantendo apenas usuários)...")
            
            # Excluir na ordem correta para evitar erros de chave estrangeira
            tabelas = [
                (LogAuditoria, "Logs de auditoria"),
                (VendaEquipamento, "Venda equipamentos"),
                (Sorteio, "Sorteios"),
                (FaturamentoSorteio, "Faturamento sorteio"),
                (PesquisaResposta, "Pesquisas resposta"),
                (PesquisaMarketing, "Pesquisas marketing"),
                (Venda, "Vendas"),
                (Estoque, "Estoque"),
                (Brinde, "Brindes"),
                (Cliente, "Clientes")
            ]
            
            for tabela, nome in tabelas:
                count = tabela.query.count()
                if count > 0:
                    db.session.query(tabela).delete()
                    print(f"✅ {count} {nome} excluídos")
            
            db.session.commit()
            
            usuarios_count = Usuario.query.count()
            print(f"\n✅ Banco zerado com sucesso!")
            print(f"👥 {usuarios_count} usuários mantidos")
            
        except Exception as e:
            db.session.rollback()
            print(f"❌ Erro: {str(e)}")

if __name__ == '__main__':
    zerar_tudo_exceto_usuarios()