import sys
from pathlib import Path

# Ajouter le dossier parent au path
sys.path.append(str(Path(__file__).parent.parent))

from app.llm.gemini_client import GeminiClient

gemini_client = GeminiClient(api_key=None)

response = gemini_client.client.models.generate_content(
            model=gemini_client.model,
            contents=gemini_client,
            
            )
print(response)