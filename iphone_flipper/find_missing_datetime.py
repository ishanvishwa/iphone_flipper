import os, ast
for root, _, files in os.walk("."):
    for file in files:
        if file.endswith(".py"):
            path = os.path.join(root, file)
            with open(path) as f:
                content = f.read()
            try:
                tree = ast.parse(content)
            except SyntaxError:
                continue
            has_datetime_import = False
            has_datetime_usage = False
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name == "datetime": has_datetime_import = True
                elif isinstance(node, ast.ImportFrom):
                    if node.module == "datetime": has_datetime_import = True
                    for alias in node.names:
                        if alias.name == "datetime": has_datetime_import = True
                elif isinstance(node, ast.Name):
                    if node.id == "datetime": has_datetime_usage = True
            if has_datetime_usage and not has_datetime_import:
                print("Missing import in:", path)
