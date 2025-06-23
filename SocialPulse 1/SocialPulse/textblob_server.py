from flask import Flask, request, jsonify
from textblob import TextBlob
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

@app.route('/analyze', methods=['POST'])
def analyze_sentiment():
    try:
        data = request.get_json()
        if not data or 'text' not in data:
            return jsonify({'error': 'No text provided'}), 400
        
        text = data['text']
        blob = TextBlob(text)
        
        # Get sentiment polarity (-1 to 1) and subjectivity (0 to 1)
        polarity = blob.sentiment.polarity
        subjectivity = blob.sentiment.subjectivity
        
        # First normalize the polarity to 0-1 scale
        normalized_value = (polarity + 1) / 2
        
        # Determine sentiment label using normalized thresholds
        if normalized_value > 0.6:
            sentiment = 'positive'
        elif normalized_value < 0.4:
            sentiment = 'negative'
        else:
            sentiment = 'neutral'
        
        return jsonify({
            'sentiment': sentiment,
            'score': normalized_value,
            'subjectivity': subjectivity,
            'polarity': polarity
        })
    
    except Exception as e:
        print(f"Error processing request: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print('🎯 Starting TextBlob Sentiment Analysis Service...')
    print('✨ Endpoint: http://localhost:5001/analyze')
    print('📝 Example: curl -X POST http://localhost:5001/analyze -H \'Content-Type: application/json\' -d \'{"text": "This is great!"}\'')
    app.run(host='0.0.0.0', port=5001) 