#!/usr/bin/env python3
"""
ZERADOR DO BANCO DE DADOS - R.O Experience 2025
CUIDADO: Este script APAGA TODOS os dados do banco!

Execute: python zerador_db.py
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app import app, db

class ZeradorBanco:
    def __init__(self):
        self.tabelas_afetadas = []
    
    def confirmar_destruicao(self):
        """Solicita confirmação do usuário"""
        
        print("🚨🚨🚨 PERIGO 🚨🚨🚨")
        print("ESTE SCRIPT VAI APAGAR TODOS OS DADOS DO BANCO!")
        print("Isso inclui:")
        print("  📊 Todas as pesquisas de satisfação")
        print("  👥 Todos os clientes e check-ins")
        print("  💰 Todas as vendas registradas")
        print("  🎁 Todos os sorteios realizados")
        print("  👤 Todos os usuários (exceto admin)")
        print("  📝 Todos os logs de auditoria")
        print("")
        print("ESTA AÇÃO NÃO PODE SER DESFEITA!")
        print("")
        
        confirmacao1 = input("Digite 'ZERAR-TUDO' para continuar: ")
        if confirmacao1 != "ZERAR-TUDO":
            print("❌ Operação cancelada.")
            return False
        
        confirmacao2 = input("Digite 'CONFIRMAR-DESTRUICAO' para confirmar: ")
        if confirmacao2 != "CONFIRMAR-DESTRUICAO":
            print("❌ Operação cancelada.")
            return False
        
        return True
    
    def zerar_banco(self):
        """Apaga e recria todo o banco de dados"""
        
        print("💥 Iniciando destruição do banco...")
        
        with app.app_context():
            try:
                # Drop todas as tabelas
                db.drop_all()
                print("✅ Todas as tabelas removidas")
                
                # Cria todas as tabelas do zero
                db.create_all()
                print("✅ Novas tabelas criadas")
                
                # Recria estruturas básicas
                from app import criar_usuario_admin, migrar_banco_dados
                criar_usuario_admin()
                migrar_banco_dados()
                
                print("🎉 Banco zerado e recriado com sucesso!")
                return True
                
            except Exception as e:
                print(f"❌ Erro ao zerar banco: {e}")
                return False
    
    def executar(self):
        """Função principal do zerador"""
        
        print("=" * 70)
        print("💥 ZERADOR DO BANCO DE DADOS - R.O Experience 2025")
        print("=" * 70)
        
        if not self.confirmar_destruicao():
            return False
        
        return self.zerar_banco()

def main():
    """Executa o zerador"""
    zerador = ZeradorBanco()
    success = zerador.executar()
    
    if success:
        print("\n✨ Banco zerado com sucesso!")
        print("📝 Próximos passos:")
        print("   1. Execute o atualizador_db.py se necessário")
        print("   2. Reinicie o servidor Flask")
        print("   3. Faça login com admin/admin123")
    else:
        print("\n❌ Operação cancelada ou falhou!")
        sys.exit(1)

if __name__ == "__main__":
    main()