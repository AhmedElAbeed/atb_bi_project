import os
import shutil

# Root directories where directories should be created
roots = [
    r"C:\Users\Ahmed\Desktop",
    r"C:\Users\Ahmed\Desktop\atb_bi_project"
]

# Minimal valid 1x1 PNG bytes
png_bytes = b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15c4\x00\x00\x00\nIDATx\x9cc`\x00\x00\x00\x02\x00\x01H\xaf\xa4q\x00\x00\x00\x00IEND\xaeB`\x82'

# Minimal valid 1x1 JPEG bytes
jpg_bytes = b'\xff\xd8\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t\t\x08\n\x0c\x14\r\x0c\x0b\x0b\x0c\x19\x12\x13\x0f\x14\x1d\x1a\x1f\x1e\x1d\x1a\x1c\x1c $.\x27" "#\x1c\x1c(7),01444\x1f\'9=82<.342\xff\xc0\x00\x0b\x08\x00\x01\x00\x01\x01\x01\x11\x00\xff\xc4\x00\x1f\x00\x00\x01\x05\x01\x01\x01\x01\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\xff\xda\x00\x08\x01\x01\x00\x00?\x00\x37\x00\xa0\xff\xd9'

# Subfolders to create
subfolders = ["figures", "logo"]

# Standard files to create
figures_png = ["download.png", "excel_logo.png", "powerbi_logo.png", "crispml.png", "scrum.png", "gimsi.png"]
logo_jpg = ["atb_logo.jpg", "atb_building.jpg"]

for root in roots:
    print(f"Checking root: {root}")
    # Creare folders
    for sub in subfolders:
        dir_path = os.path.join(root, sub)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            print(f"Created directory: {dir_path}")
        
    # Write PNGs
    for fn in figures_png:
        file_path = os.path.join(root, "figures", fn)
        if not os.path.exists(file_path):
            with open(file_path, "wb") as f:
                f.write(png_bytes)
            print(f"Created file: {file_path}")
            
    # Write JPGs
    for fn in logo_jpg:
        file_path = os.path.join(root, "logo", fn)
        # Check if we can copy it from atb_bi_project/logo if we are at Desktop/logo
        src_path = os.path.join(r"C:\Users\Ahmed\Desktop\atb_bi_project\logo", fn)
        if root == r"C:\Users\Ahmed\Desktop" and os.path.exists(src_path):
            shutil.copy(src_path, file_path)
            print(f"Copied {src_path} to {file_path}")
        elif not os.path.exists(file_path):
            with open(file_path, "wb") as f:
                f.write(jpg_bytes)
            print(f"Created file: {file_path}")

print("Placeholder creation completed.")
