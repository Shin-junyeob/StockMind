import matplotlib.pyplot as plt
import numpy as np

# Updated pattern generators with dynamic y-axis ranges

def generate_flag_pattern_v3():
    base = np.random.randint(0, 60)
    height = np.random.randint(10, 40)
    pole = np.linspace(base, base + height, 3)
    flag = np.linspace(pole[-1], pole[-1] + np.random.randint(-5, 5), 4) + np.random.normal(0, 2, 4)
    breakout = np.linspace(flag[-1], flag[-1] + np.random.randint(5, 25), 3)
    return np.concatenate([pole, flag, breakout])

def generate_pennant_pattern_v3():
    base = np.random.randint(0, 60)
    height = np.random.randint(10, 40)
    pole = np.linspace(base, base + height, 3)
    pennant_base = pole[-1]
    pennant = pennant_base + np.sin(np.linspace(0, 2 * np.pi, 4)) * np.linspace(5, 1, 4)
    breakout = np.linspace(pennant[-1], pennant[-1] + np.random.randint(5, 25), 3)
    return np.concatenate([pole, pennant, breakout])

def generate_cup_handle_pattern_v3():
    bottom = np.random.randint(10, 50)
    depth = np.random.randint(5, 20)
    cup = -np.cos(np.linspace(0, np.pi, 5)) * depth + bottom
    handle = np.linspace(cup[-1], cup[-1] - np.random.randint(3, 10), 2) + np.random.normal(0, 1, 2)
    breakout = np.linspace(handle[-1], handle[-1] + np.random.randint(10, 25), 3)
    return np.concatenate([cup, handle, breakout])

def generate_gap_pattern_v3():
    start = np.random.randint(10, 60)
    part1 = np.linspace(start, start + np.random.randint(5, 20), 4)
    gap = np.random.randint(10, 30)
    part2 = part1[-1] + gap + np.linspace(0, np.random.randint(10, 20), 6)
    return np.concatenate([part1, part2])

# Updated plotting function without fixed y-axis
def plot_multiple_patterns_custom_range(pattern_func, title):
    fig, axes = plt.subplots(5, 2, figsize=(12, 10))
    fig.suptitle(f"{title} - 10 Variations (Dynamic Y-Range)", fontsize=16)
    for i, ax in enumerate(axes.flat):
        pattern = pattern_func()
        pattern = pattern + np.random.normal(0, 1.5, len(pattern))
        ax.plot(np.arange(1, 11), pattern)
        ax.set_title(f"{title} #{i+1}")
        ax.set_xticks(range(1, 11))
        ax.grid(True)
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.show()

# Generate updated graphs with different y-axis ranges
plot_multiple_patterns_custom_range(generate_flag_pattern_v3, "Flag Pattern")
plot_multiple_patterns_custom_range(generate_pennant_pattern_v3, "Pennant Pattern")
plot_multiple_patterns_custom_range(generate_cup_handle_pattern_v3, "Cup and Handle Pattern")
plot_multiple_patterns_custom_range(generate_gap_pattern_v3, "Gap Pattern")
