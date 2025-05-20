import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset, random_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import confusion_matrix, classification_report
import matplotlib.pyplot as plt
import seaborn as sns

files = [
    "../results/flag_pattern_with_result.csv",
    "../results/pennant_pattern_with_result.csv",
    "../results/cup_handle_pattern_with_result.csv",
    "../results/gap_pattern_with_result.csv"
]

df_all = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
X = df_all.drop(columns=["label"]).values
y = df_all["label"].values

scaler = MinMaxScaler()
X_scaled = scaler.fit_transform(X)

X_tensor = torch.tensor(X_scaled, dtype=torch.float32).unsqueeze(2)
y_tensor = torch.tensor(y, dtype=torch.float32).unsqueeze(1)

dataset = TensorDataset(X_tensor, y_tensor)
train_size = int(0.8 * len(dataset))
val_size = len(dataset) - train_size
train_set, val_set = random_split(dataset, [train_size, val_size])
train_loader = DataLoader(train_set, batch_size=32, shuffle=True)
val_loader = DataLoader(val_set, batch_size=32)

class CNNClassifier(nn.Module):
    def __init__(self):
        super(CNNClassifier, self).__init__()
        self.conv1 = nn.Conv1d(1, 64, kernel_size=3)
        self.pool = nn.MaxPool1d(2)
        self.dropout = nn.Dropout(0.3)
        self.fc1 = nn.Linear(64 * 14, 64)
        self.fc2 = nn.Linear(64, 1)
        self.sigmoid = nn.Sigmoid()

    def forward(self, x):
        x = x.permute(0, 2, 1)
        x = self.pool(torch.relu(self.conv1(x)))
        x = self.dropout(x)
        x = x.reshape(x.size(0), -1)
        x = torch.relu(self.fc1(x))
        return self.sigmoid(self.fc2(x))

model = CNNClassifier()
criterion = nn.BCELoss()
optimizer = optim.Adam(model.parameters(), lr=0.001)

for epoch in range(20):
    model.train()
    for inputs, labels in train_loader:
        optimizer.zero_grad()
        outputs = model(inputs)
        loss = criterion(outputs, labels)
        loss.backward()
        optimizer.step()

model.eval()
correct, total = 0, 0
with torch.no_grad():
    for inputs, labels in val_loader:
        outputs = model(inputs)
        predicted = (outputs > 0.5).float()
        total += labels.size(0)
        correct += (predicted == labels).sum().item()

print(f"Validation Accuracy: {correct / total:.4f}")

model.eval()
all_preds, all_labels = [], []
with torch.no_grad():
    for inputs, labels in val_loader:
        outputs = model(inputs)
        preds = (outputs > 0.3).float()
        all_preds.extend(preds.numpy().flatten())
        all_labels.extend(labels.numpy().flatten())
print(classification_report(all_labels, all_preds))

label_series = pd.Series(all_labels)
label_counts = label_series.value_counts()

cm = confusion_matrix(all_labels, all_preds)
labels_sorted = [0, 1]

fig, axes = plt.subplots(1, 2, figsize=(12, 5))

axes[0].bar(label_counts.index.astype(str), label_counts.values, color=['skyblue', 'salmon'])
axes[0].set_title("Label Distribution in Validation Set")
axes[0].set_xlabel("Label")
axes[0].set_ylabel("Count")

sns.heatmap(cm, annot=True, fmt='d', cmap="Blues",
            xticklabels=labels_sorted, yticklabels=labels_sorted, ax=axes[1])
axes[1].set_title("Confusion Matrix")
axes[1].set_xlabel("Predicted Label")
axes[1].set_ylabel("True Label")

plt.tight_layout()
plt.show()