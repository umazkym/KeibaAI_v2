
import os

target_file = r'C:\Users\zk-ht\Keiba\Keiba_AI_v2\scripts\reports\create_interactive_charts.py'

# Raw JS with single braces
raw_debug_script = r'''
<div id="debug-console" style="position:fixed;top:0;left:0;width:100%;background:rgba(200,0,0,0.9);color:white;z-index:9999;padding:10px;display:none;font-size:12px;max-height:100px;overflow:auto;"></div>
<script>
    window.onerror = function(msg, url, line, col, error) {
        const d = document.getElementById('debug-console');
        d.style.display = 'block';
        d.innerHTML += `<div>Error: ${msg} at line ${line}:${col}</div>`;
        return false;
    };
    console.error = (function(old) {
        return function(text) {
            old(text);
            const d = document.getElementById('debug-console');
            d.style.display = 'block';
            d.innerHTML += `<div>Console Error: ${text}</div>`;
        };
    }(console.error));
</script>
'''

# Escape for f-string
escaped_debug_script = raw_debug_script.replace('{', '{{').replace('}', '}}')

with open(target_file, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Cleanup previous bad injection (which had single braces)
# The previous script injected: content.replace("<body>", "<body>" + debug_script)
# We try to remove it. Since we don't know exactly what the file looks like now (indentation?), 
# searching for the raw string might be hard if it was formatted.
# But likely it was inserted literally.
if '<div id="debug-console"' in content:
    print("Found existing debug console injection. Removing it...")
    # We will try to identify the block. It starts with <div id="debug-console" and ends with </script>
    start_marker = '<div id="debug-console"'
    end_marker = '</script>'
    start_idx = content.find(start_marker)
    # The end marker needs to be the one *after* start_idx
    end_idx = content.find(end_marker, start_idx)
    
    if start_idx != -1 and end_idx != -1:
        # Remove the block + the newline before it maybe
        content = content[:start_idx] + content[end_idx+len(end_marker):]
        print("Removed bad injection.")

# 2. Inject the escaped version
# We inject it *after* <body>
if "<body>" in content:
    # Check if we already have it (escaped version)
    if 'id="debug-console"' not in content:
        content = content.replace("<body>", "<body>" + escaped_debug_script)
        print("Injected escaped debug console.")
    else:
        print("Debug console already present (or partially present).")
else:
    print("Could not find <body> tag.")

with open(target_file, 'w', encoding='utf-8') as f:
    f.write(content)
