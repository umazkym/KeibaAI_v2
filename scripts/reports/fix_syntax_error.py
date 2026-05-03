
import os

target_file = r'C:\Users\zk-ht\Keiba\Keiba_AI_v2\scripts\reports\create_interactive_charts.py'

with open(target_file, 'r', encoding='utf-8') as f:
    content = f.read()

# The error pattern:
# return renderFilters() + `
#         return `

# We want to remove `        return `

# Since whitespace might vary, we can try a replace on the two lines together.
# In the previous view, it was:
# 1020:         return renderFilters() + `
# 1021:         return `

# Let's try to match that.
bad_pattern = "return renderFilters() + `\n        return `"
good_pattern = "return renderFilters() + `"

if bad_pattern in content:
    content = content.replace(bad_pattern, good_pattern)
    print("Fixed syntax error (method 1)")
else:
    # Try more loose matching if indentation is different
    # "return `" is what we want to delete.
    # Look for "return renderFilters() + `" followed by whitespace and "return `"
    import re
    # matches: return renderFilters() + ` [newline + spaces] return `
    content = re.sub(r'(return renderFilters\(\) \+ `)\s+return `', r'\1', content)
    print("Fixed syntax error (method 2 - regex)")

with open(target_file, 'w', encoding='utf-8') as f:
    f.write(content)
