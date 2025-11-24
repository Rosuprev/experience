# alter_pesquisa_marketing_postgres.py
import psycopg2
from config import Config  # Importe sua configuração

def alter_pesquisa_marketing_postgres():
    try:
        # Conectar ao PostgreSQL
        conn = psycopg2.connect(
            dbname=Config.DATABASE_NAME,
            user=Config.DATABASE_USER,
            password=Config.DATABASE_PASSWORD,
            host=Config.DATABASE_HOST,
            port=Config.DATABASE_PORT
        )
        cursor = conn.cursor()
        
        print("🔧 Executando ALTER TABLE no PostgreSQL...")
        
        # PostgreSQL suporta DROP NOT NULL diretamente
        alter_queries = [
            "ALTER TABLE pesquisa_marketing ALTER COLUMN dificuldade_participacao DROP NOT NULL",
            "ALTER TABLE pesquisa_marketing ALTER COLUMN tipo_campanha_impacto DROP NOT NULL",
            "ALTER TABLE pesquisa_marketing ALTER COLUMN aumento_volume DROP NOT NULL", 
            "ALTER TABLE pesquisa_marketing ALTER COLUMN competitividade DROP NOT NULL"
        ]
        
        for query in alter_queries:
            cursor.execute(query)
            print(f"✅ Executado: {query}")
        
        conn.commit()
        print("🎉 Todas as alterações foram aplicadas com sucesso!")
        
    except psycopg2.Error as e:
        print(f"❌ Erro PostgreSQL: {e}")
        conn.rollback()
    except Exception as e:
        print(f"❌ Erro geral: {e}")
        conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    print("🚀 Iniciando alteração da tabela pesquisa_marketing (PostgreSQL)...")
    alter_pesquisa_marketing_postgres()