import re

file_path = r"C:\Users\Ahmed\Desktop\atb_bi_project\chapitre1.tex"

with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

# Replace any table[H] or figure[H] with [!ht]
content = re.sub(r'\\begin\{(table|figure)\}\[H\]', r'\\begin{\1}[!ht]', content)

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)

print("[H] replaced with [!ht] in C:\\Users\\Ahmed\\Desktop\\atb_bi_project\\chapitre1.tex successfully.")
