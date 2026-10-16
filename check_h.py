import re

file_path = r"C:\Users\Ahmed\Desktop\atb_bi_project\chapitre1.tex"

with open(file_path, "r", encoding="utf-8") as f:
    lines = f.readlines()

for idx, line in enumerate(lines):
    if "[H]" in line:
        print(f"Line {idx+1}: {line.strip()}")
