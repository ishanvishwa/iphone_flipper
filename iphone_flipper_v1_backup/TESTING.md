# Testing

Run unit tests from repository root:

```bash
python -m unittest discover -s server/tests -p "test_*.py"
```

If using the local virtual environment:

```bash
./.venv/bin/python -m unittest discover -s server/tests -p "test_*.py"
```
