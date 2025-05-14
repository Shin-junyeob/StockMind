import numpy as np
import pandas as pd

def generate_flag_pattern():
    base = np.random.randint(10, 60)
    pole = np.linspace(base, base + np.random.randint(10, 25), 3)
    flag = np.linspace(pole[-1], pole[-1] + np.random.randint(-5, 5), 4) + np.random.normal(0, 1.5, 4)
    breakout = np.linspace(flag[-1], flag[-1] + np.random.randint(5, 15), 3)
    return np.concatenate([pole, flag, breakout])

def generate_pennant_pattern():
    base = np.random.randint(10, 60)
    pole = np.linspace(base, base + np.random.randint(10, 25), 3)
    pennant = pole[-1] + np.sin(np.linspace(0, 2 * np.pi, 4)) * np.linspace(3, 1, 4)
    breakout = np.linspace(pennant[-1], pennant[-1] + np.random.randint(5, 15), 3)
    return np.concatenate([pole, pennant, breakout])

def generate_cup_and_handle_pattern():
    base = np.random.randint(10, 40)
    depth = np.random.randint(5, 15)
    cup = -np.cos(np.linspace(0, np.pi, 7)) * depth + base + depth
    handle = np.linspace(cup[-1], cup[-1] - np.random.randint(2, 6), 3)
    return np.concatenate([cup, handle])

def generate_gap_pattern():
    base = np.random.randint(20, 50)
    pre = np.linspace(base, base + np.random.randint(5, 10), 4)
    gap = np.random.randint(10, 20)
    post = pre[-1] + gap + np.linspace(0, np.random.randint(5, 15), 6)
    return np.concatenate([pre, post])

# 공통 시계열 생성 함수
def generate_series_with_pattern(pattern_func, total_length=36, pattern_start=20, pattern_length=10, result_length=6):
    pre_pattern = np.cumsum(np.random.normal(0, 2, pattern_start)) + np.random.randint(20, 40)
    pattern = pattern_func()
    result = np.cumsum(np.random.normal(0, 2, result_length)) + pattern[-1]
    full_series = np.concatenate([pre_pattern, pattern, result])
    label = int(result[-1] > pattern[-1])
    return full_series[:pattern_start + pattern_length], label

# 공통 데이터 생성 함수
def create_pattern_dataset(pattern_func, filename, n_samples=200):
    data = []
    for _ in range(n_samples):
        x, y = generate_series_with_pattern(pattern_func)
        data.append(list(x) + [y])
    columns = [f"day_{i+1}" for i in range(30)] + ["label"]
    df = pd.DataFrame(data, columns=columns)
    df.to_csv(filename, index=False)
    return df

df_flag = create_pattern_dataset(generate_flag_pattern, "flag_pattern_with_result.csv")
df_pennant = create_pattern_dataset(generate_pennant_pattern, "pennant_pattern_with_result.csv")
df_cup = create_pattern_dataset(generate_cup_and_handle_pattern, "cup_handle_pattern_with_result.csv")
df_gap = create_pattern_dataset(generate_gap_pattern, "gap_pattern_with_result.csv")
