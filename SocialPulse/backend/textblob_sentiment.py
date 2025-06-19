from flask import Flask, request, jsonify
from textblob import TextBlob
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

def analyze_text(text):
    if not text:
        return {"sentiment": "neutral", "score": 0.5, "subjectivity": 0.5}
    
    blob = TextBlob(text)
    # Polarity ranges from -1 (negative) to 1 (positive)
    # Convert to 0-1 scale for compatibility with existing system
    normalized_score = (blob.sentiment.polarity + 1) / 2
    
    sentiment = "neutral"
    if normalized_score > 0.6:
        sentiment = "positive"
    elif normalized_score < 0.4:
        sentiment = "negative"
    
    return {
        "sentiment": sentiment,
        "score": normalized_score,
        "subjectivity": blob.sentiment.subjectivity
    }

@app.route('/analyze', methods=['POST'])
def analyze():
    data = request.get_json()
    if not data or 'text' not in data:
        return jsonify({"error": "No text provided"}), 400
    
    result = analyze_text(data['text'])
    return jsonify(result)

if __name__ == '__main__':
    port = int(os.getenv('SENTIMENT_PORT', 5001))
    app.run(host='0.0.0.0', port=port)