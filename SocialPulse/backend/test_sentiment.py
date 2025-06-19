from textblob import TextBlob

def test_sentiment(text):
    blob = TextBlob(text)
    polarity = blob.sentiment.polarity
    normalized_score = (polarity + 1) / 2
    
    sentiment = "neutral"
    if polarity > 0.1:
        sentiment = "positive"
    elif polarity < -0.1:
        sentiment = "negative"
        
    print(f"\nAnalyzing: '{text}'")
    print(f"Raw Polarity: {polarity:.2f} (-1 to +1)")
    print(f"Normalized Score: {normalized_score:.2f} (0 to 1)")
    print(f"Sentiment Category: {sentiment}")
    print(f"Subjectivity: {blob.sentiment.subjectivity:.2f} (0=objective, 1=subjective)")

# Test examples
examples = [
    "I absolutely love this beautiful dress!",
    "This product is terrible, very disappointed.",
    "The package arrived on Tuesday.",
    "Not bad, but could be better.",
    "The quality is amazing and the price is great!",
]

print("TextBlob Sentiment Analysis Examples:")
print("====================================")

for example in examples:
    test_sentiment(example) 

# Check the database
print("Checking the database for sentiment data...")
print("====================================")
print("SELECT * FROM sentiment_data LIMIT 10;")