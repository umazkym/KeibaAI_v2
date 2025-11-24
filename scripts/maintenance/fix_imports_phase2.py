import re
from pathlib import Path

def fix_file(file_path):
    path = Path(file_path)
    if not path.exists():
        print(f"File not found: {path}")
        return

    content = path.read_text(encoding='utf-8')
    original_content = content

    # 1. Fix sys.path.append
    # Old: project_root = Path(__file__).resolve().parent.parent
    # New: project_root = Path(__file__).resolve().parent.parent.parent
    
    # Pattern for project_root definition
    # We want to ensure it goes up 3 levels (scripts/pipelines/script.py -> scripts/pipelines -> scripts -> root)
    # Original was in keibaai/src/script.py -> keibaai/src -> keibaai (2 levels to get to keibaai dir, but we want root)
    
    # Let's standardize: project_root = ...parent.parent.parent
    # And sys.path.append(str(project_root))
    
    if "project_root = Path(__file__).resolve().parent.parent" in content:
        content = content.replace(
            "project_root = Path(__file__).resolve().parent.parent",
            "project_root = Path(__file__).resolve().parent.parent.parent"
        )
        print(f"Fixed project_root in {path.name}")

    # 2. Fix imports
    # from src import ... -> from keibaai.src import ...
    # from src.modules... -> from keibaai.src.modules...
    
    content = re.sub(r'^from src import', 'from keibaai.src import', content, flags=re.MULTILINE)
    content = re.sub(r'^from src\.', 'from keibaai.src.', content, flags=re.MULTILINE)
    content = re.sub(r'^import src\.', 'import keibaai.src.', content, flags=re.MULTILINE)

    # 3. Fix config loading path
    # Old: config_path = Path(__file__).resolve().parent.parent / "configs"
    # New: config_path = project_root / "keibaai" / "configs"
    
    if 'config_path = Path(__file__).resolve().parent.parent / "configs"' in content:
        content = content.replace(
            'config_path = Path(__file__).resolve().parent.parent / "configs"',
            'config_path = project_root / "keibaai" / "configs"'
        )
        print(f"Fixed config_path in {path.name}")
        
    # Also check for data_root calculation
    # Old: data_root = Path(__file__).resolve().parent.parent / default_cfg['data_path']
    # New: data_root = project_root / "keibaai" / default_cfg['data_path']
    
    if "data_root = Path(__file__).resolve().parent.parent / default_cfg['data_path']" in content:
         content = content.replace(
            "data_root = Path(__file__).resolve().parent.parent / default_cfg['data_path']",
            "data_root = project_root / \"keibaai\" / default_cfg['data_path']"
        )
         print(f"Fixed data_root in {path.name}")

    if content != original_content:
        path.write_text(content, encoding='utf-8')
        print(f"Updated {path}")
    else:
        print(f"No changes needed for {path}")

files_to_fix = [
    "scripts/debug/debug_run_parsing_pipeline_local_1000.py",
    "scripts/pipelines/run_parsing_pipeline_local.py",
    "scripts/pipelines/run_scraping_pipeline_local.py",
    "scripts/pipelines/run_scraping_pipeline_with_args.py"
]

for f in files_to_fix:
    fix_file(f)
