# Development Notebooks

This folder contains ad-hoc development and exploration notebooks.

## Usage

- Use these notebooks for experimentation and prototyping
- Do not use for production workflows
- Move stable code to `src/` modules
- Name notebooks descriptively with date prefix (e.g., `2026-10-09_member_analysis.py`)

## Best Practices

1. **Import from src modules**: Use `sys.path.append("/Workspace/src")` to access production code
2. **Document assumptions**: Add markdown cells explaining your analysis
3. **Clean up**: Archive or delete notebooks when no longer needed
4. **Version control**: Keep notebooks in git for collaboration
