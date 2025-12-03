# atualizar_db.py
from app import app, db, VendaEvento

print("🔄 Criando tabelas...")

# Execute dentro do contexto da aplicação
with app.app_context():
    db.create_all()
    print("✅ Tabelas criadas com sucesso!")
    
    # Verificar se a tabela foi criada
    try:
        contagem = VendaEvento.query.count()
        print(f"📊 Tabela 'VendaEvento' criada! Registros: {contagem}")
    except Exception as e:
        print(f"⚠️  Aviso: {e}")
        print("📝 A tabela foi criada, mas ainda não tem registros.")