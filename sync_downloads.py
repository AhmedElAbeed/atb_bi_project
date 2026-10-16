import os
import shutil

# Clean source files
src_tex = r"C:\Users\Ahmed\Desktop\atb_bi_project\chapitre1.tex"
dest_tex = r"C:\Users\Ahmed\Downloads\chapitre1.tex"

# Copy the clean LaTeX file to Downloads
shutil.copy(src_tex, dest_tex)
print(f"Copied {src_tex} to {dest_tex}")

# Create directories in Downloads
downloads_figures = r"C:\Users\Ahmed\Downloads\figures"
downloads_logo = r"C:\Users\Ahmed\Downloads\logo"

os.makedirs(downloads_figures, exist_ok=True)
os.makedirs(downloads_logo, exist_ok=True)
print("Created figures and logo folders in C:\\Users\\Ahmed\\Downloads")

# Placeholders content
png_bytes = b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15c4\x00\x00\x00\nIDATx\x9cc`\x00\x00\x00\x02\x00\x01H\xaf\xa4q\x00\x00\x00\x00IEND\xaeB`\x82'
jpg_bytes = b'\xff\xd8\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t\t\x08\n\x0c\x14\r\x0c\x0b\x0b\x0c\x19\x12\x13\x0f\x14\x1d\x1a\x1f\x1e\x1d\x1a\x1c\x1c $.\x27" "#\x1c\x1c(7),01444\x1f\'9=82<.342\xff\xc0\x00\x0b\x08\x00\x01\x00\x01\x01\x01\x11\x00\xff\xc4\x00\x1f\x00\x00\x01\x05\x01\x01\x01\x01\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\xff\xda\x00\x08\x01\x01\x00\x00?\x00\x37\x00\xa0\xff\xd9'

# Subfolders png files
figures_png = ["download.png", "excel_logo.png", "powerbi_logo.png", "crispml.png", "scrum.png", "gimsi.png"]
logo_jpg = ["atb_logo.jpg", "atb_building.jpg"]

for fn in figures_png:
    path = os.path.join(downloads_figures, fn)
    with open(path, "wb") as f:
        f.write(png_bytes)
    print(f"Created placeholder: {path}")

# If we have real jpg files in atb_bi_project/logo, let's copy them, otherwise write minimalist
for fn in logo_jpg:
    path = os.path.join(downloads_logo, fn)
    src_jpg = os.path.join(r"C:\Users\Ahmed\Desktop\atb_bi_project\logo", fn)
    if os.path.exists(src_jpg):
        shutil.copy(src_jpg, path)
        print(f"Copied {src_jpg} to {path}")
    else:
        with open(path, "wb") as f:
            f.write(jpg_bytes)
        print(f"Created placeholder: {path}")

print("Sync and creation completed successfully.")
