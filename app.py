# app.py
import os
from flask import Flask, render_template, request, session
from llm import init_data, init_groq_client, generate_mongodb_query, execute_mongodb_query

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "secret-key-123")

# Initialiser les données et le client Groq au démarrage
print("=" * 50)
print("🚀 Initialisation de l'application...")
print("=" * 50)

# Initialiser les données
init_data()

# Initialiser le client Groq
init_groq_client()

print("=" * 50)
print("✅ Application prête !")
print("=" * 50)

@app.route('/', methods=['GET', 'POST'])
def index():
    """
    Route principale de l'application.
    """
    # Initialiser la session pour la conversation
    if 'conversation' not in session:
        session['conversation'] = []
    
    # Variables pour le template
    question = ""
    mongo_query = ""
    response_text = ""
    results = []
    
    if request.method == 'POST':
        question = request.form.get('question', '').strip()
        
        if question:
            print(f"\n📝 Nouvelle question: {question}")
            
            # Ajouter la question à l'historique de conversation
            session['conversation'].append({
                'role': 'user', 
                'text': question
            })
            
            # Générer la requête MongoDB
            mongo_query = generate_mongodb_query(question)
            print(f"🔍 Requête générée: {mongo_query[:100]}...")
            
            # Exécuter la requête
            results = execute_mongodb_query(mongo_query)
            print(f"📊 Résultats trouvés: {len(results)} produits")
            
            # Générer la réponse textuelle
            if results:
                response_text = f"✅ J'ai trouvé {len(results)} produits correspondant à votre recherche."
                if len(results) == 20:
                    response_text += " (affichage limité à 20 résultats)"
            else:
                response_text = "❌ Aucun produit trouvé pour votre recherche."
            
            # Ajouter la réponse à l'historique de conversation
            session['conversation'].append({
                'role': 'assistant', 
                'text': response_text
            })
            
            # Limiter la taille de l'historique de conversation
            if len(session['conversation']) > 10:
                session['conversation'] = session['conversation'][-10:]
            
            # Marquer la session comme modifiée
            session.modified = True
    
    # Rendre le template avec les données
    return render_template(
        'index.html',
        question=question,
        conversation=session.get('conversation', []),
        results=results,
        sql_query=mongo_query,
        response=response_text
    )

@app.route('/clear', methods=['GET'])
def clear_conversation():
    """
    Route pour effacer l'historique de conversation.
    """
    session['conversation'] = []
    session.modified = True
    return "✅ Conversation effacée. <a href='/'>Retour à l'accueil</a>"

@app.route('/health', methods=['GET'])
def health_check():
    """
    Route pour vérifier l'état de l'application.
    """
    from llm import df, groq_available
    
    status = {
        'status': 'healthy',
        'data_loaded': not df.empty if df is not None else False,
        'groq_available': groq_available,
        'data_count': len(df) if df is not None else 0
    }
    return status

if __name__ == '__main__':
    # Démarrer l'application Flask
    print("\n🌐 Démarrage du serveur Flask...")
    print("👉 Accédez à l'application sur: http://localhost:5000")
    app.run(debug=True, host='0.0.0.0', port=5000)