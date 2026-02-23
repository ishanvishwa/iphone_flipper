import os, ast
for root, _, files in os.walk("."):
    for file in files:
        if file.endswith(".py"):
            path = os.path.join(root, file)
            with open(path) as f:
                content = f.read()
            try:
                tree = ast.parse(content)
            except SyntaxError as e:
                print(f"SyntaxError in {path}: {e}")
