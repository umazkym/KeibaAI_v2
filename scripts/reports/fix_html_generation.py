
import os

target_file = r'C:\Users\zk-ht\Keiba\Keiba_AI_v2\scripts\reports\create_interactive_charts.py'

with open(target_file, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Wrap the assignment in parentheses
# Find `html = f'''` 
# Note: exact indentation is important. It is indented by 4 spaces.
if "    html = f'''<!DOCTYPE html>" in content:
    content = content.replace("    html = f'''<!DOCTYPE html>", "    html = (f'''<!DOCTYPE html>")
else:
    print("Could not find start of html assignment")
    exit(1)

# 2. Replace the data injection
# Old: `    const DATA = {data_json};`
# New: `    const DATA = ''' + data_json + f''';`
if "    const DATA = {data_json};" in content:
    content = content.replace("    const DATA = {data_json};", "    const DATA = ''' + data_json + f''';")
else:
    print("Could not find data injection line")
    exit(1)

# 3. Add closing parenthesis
# Find the end of the string. CONSTANT: `'''` followed by `    return html`
# We need to be careful not to replace other `'''`.
# The file ends with:
# </body>
# </html>
# '''
#     return html

if "'''\n    return html" in content:
    content = content.replace("'''\n    return html", "''')\n    return html")
elif "'''\r\n    return html" in content: # Handle CRLF if present
    content = content.replace("'''\r\n    return html", "''')\r\n    return html")
else:
    # Try searching for just the last triple quote before return html
    # Use rfind?
    idx = content.rfind("'''")
    ret_idx = content.rfind("return html")
    if idx != -1 and ret_idx != -1 and idx < ret_idx:
        # Check if it looks right
        print(f"Found ''' at {idx}, return at {ret_idx}")
        # Insert )
        content = content[:idx+3] + ")" + content[idx+3:]
    else:
        print("Could not find end of html assignment")
        exit(1)

with open(target_file, 'w', encoding='utf-8') as f:
    f.write(content)

print("Successfully applied fix.")
