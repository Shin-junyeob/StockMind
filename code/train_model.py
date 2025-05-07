# train_model.py (기본 구조)
import pandas as pd
import os
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.preprocessing import LabelEncoder
from sentence_transformers import SentenceTransformer
from tqdm import tqdm
import numpy as np

tqdm.pandas()

# 모델 입력을 위한 임베딩 모델 (summary)
summary_model = SentenceTransformer('all-MiniLM-L6-v2')

# metadata 불러오기
metadata = pd.read_csv("../metadata.csv")

X_features = []  # input 벡터
Y_labels = []    # 변동률 (rate) 예측용

for _, row in metadata.iterrows():
    feature_path = os.path.join('../features/', row['feature_file'])
    if not os.path.exists(feature_path):
        continue
    
    df = pd.read_csv(feature_path)

    # summary 평균 임베딩
    summary_embeddings = summary_model.encode(df['summary'].tolist())
    summary_vector = np.mean(summary_embeddings, axis=0)

    # sentiment: one-hot 인코딩
    sentiment_encoded = pd.get_dummies(df['sentiment']).sum().values  # ex: [3, 1, 2]

    # keyword 수치화 (단어 수 기준)
    keyword_counts = df['keywords'].str.split(", ").explode().value_counts()
    keyword_vector = keyword_counts.reindex(df['keywords'].str.split(", ").explode().unique(), fill_value=0).values[:10]  # 상위 10개 단어 기준

    # 최종 벡터 연결
    full_vector = np.concatenate([summary_vector, sentiment_encoded, keyword_vector])
    X_features.append(full_vector)
    Y_labels.append(row['rate'])  # 예측 타겟: 변동률

X = np.array(X_features)
y = np.array(Y_labels)

# 모델 훈련
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
model = RandomForestRegressor()
# X_train, y_train = X, y # 5일이상 쌓이면 삭제
model.fit(X_train, y_train)
# y_pred = model.predict(X) # 5일이상 쌓이면 삭제
# print(y_pred) # 5일이상 쌓이면 삭제

# 평가
y_pred = model.predict(X_test)
mae = mean_absolute_error(y_test, y_pred)
print(f"✅ 모델 평가 완료 - MAE: {mae:.4f}")
