import pandas as pd
import os
import json
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

summary_model = SentenceTransformer('all-MiniLM-L6-v2')
tqdm.pandas()

def load_data(metadata_path="../metadata.json"):
    with open(metadata_path, "r") as f:
        metadata = json.load(f)

    X_features = []
    Y_labels = []
    date_list = []

    for date, info in metadata.items():
        news_files = info["news"] if isinstance(info["news"], list) else [info["news"]]
        combined_df = pd.DataFrame()

        for file in news_files:
            feature_path = os.path.join("../features", file)
            if not os.path.exists(feature_path):
                continue
            df = pd.read_csv(feature_path)
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        if combined_df.empty or info['rate'] is None:
            continue

        summary_embeddings = summary_model.encode(combined_df['summary'].tolist())
        summary_vector = np.mean(summary_embeddings, axis=0)

        sentiment_encoded = pd.get_dummies(combined_df['sentiment']).sum().values

        keyword_counts = combined_df['keywords'].str.split(", ").explode().value_counts()
        keyword_vector = keyword_counts.reindex(
            combined_df['keywords'].str.split(", ").explode().unique(), fill_value=0).values[:10]

        full_vector = np.concatenate([summary_vector, sentiment_encoded, keyword_vector])

        X_features.append(full_vector)
        Y_labels.append(info['rate'])
        date_list.append(date)

    return np.array(X_features), np.array(Y_labels), np.array(date_list)

def train_and_evaluate(mode="random"):
    X, y, dates = load_data()

    if mode == "random":
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    elif mode == "last":
        last_index = np.argmax(dates)
        X_test = X[last_index:last_index+1]
        y_test = y[last_index:last_index+1]
        X_train = np.delete(X, last_index, axis=0)
        y_train = np.delete(y, last_index, axis=0)
    else:
        raise ValueError("Invalid mode. Choose 'random' or 'last'.")

    model = RandomForestRegressor()
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    print(f"✅ 모델 평가 완료 - Mode: {mode}")
    print(f"실제값 (y_test): {y_test[0]:.2f}%")
    print(f"예측값 (y_pred): {y_pred[0]:.2f}%")
    print(f"MAE: {mae:.4f}%")

if __name__ == "__main__":
    train_and_evaluate(mode="random")