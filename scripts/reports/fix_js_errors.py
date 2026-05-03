
import os
import re

target_file = r'C:\Users\zk-ht\Keiba\Keiba_AI_v2\scripts\reports\create_interactive_charts.py'

with open(target_file, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Rename existing renderChart to drawCharts (if not already done)
# We need to be careful if we run this script multiple times.
# If "function drawCharts() {{" exists, we might have already run it.
# Check if "function renderChart()" exists AND "function drawCharts()" does NOT exist.
if "function renderChart() {{" in content and "function drawCharts() {{" not in content:
    content = content.replace("function renderChart() {{", "function drawCharts() {{")
    print("Renamed renderChart to drawCharts")
elif "function drawCharts() {{" in content:
    print("drawCharts already exists. Skipping rename.")
else:
    print("Could not find function renderChart()")

# 2. Insert new renderChart function (that returns HTML)
# We insert it before drawCharts.
# We must use double braces {{ }} because the target file uses f-string for HTML generation.
# In Python f-string: {{ }} -> { }
# So if we want JS template literal `${h}`, we need `${{h}}` in the Python source file.
# To get `${{h}}` in the Python source file via this script, we can just write `${{h}}` in our raw string.

# But wait! If we already ran this script, we might have inserted the BAD version.
# We should try to remove the bad version or overwrite it.
# The bad version has `style="height:${h}; ${m}"`.
# The good version has `style="height:${{h}}; ${{m}}"`.

new_render_chart = r'''
    function renderChart() {{
        const isMobile = window.innerWidth <= 600;
        const h = isMobile ? '220px' : '280px';
        const m = isMobile ? 'margin-bottom:20px' : 'margin-bottom:10px';
        return `
            <div id="c1" class="card" style="height:${{h}}; ${{m}}"></div>
            <div id="c2" class="card" style="height:${{h}}; ${{m}}"></div>
            <div id="c3" class="card" style="height:${{h}}; ${{m}}"></div>
            <div id="c4" class="card" style="height:${{h}}; ${{m}}"></div>
            <div class="card" style="padding:10px; font-size:11px; color:#666;">
                ※ チャートが表示されない場合は再読み込みしてください
            </div>
        `;
    }}

'''

# Check for existing bad injection
if 'style="height:${h}' in content:
    print("Found bad injection. Replacing...")
    # Attempt to replace the strict block we added
    # We can try to Regex replace the function block
    content = re.sub(r'function renderChart\(\) \{\{.*?return `.*?`;\s*\}\}\s*', '', content, flags=re.DOTALL)
    # Now insert the new one before drawCharts
    content = content.replace("function drawCharts() {{", new_render_chart + "function drawCharts() {{")
    
elif "function renderChart() {{" not in content:
     # If we renamed it to drawCharts but didn't insert renderChart correctly (or it was broken),
     # we just insert strictly before drawCharts
     content = content.replace("function drawCharts() {{", new_render_chart + "function drawCharts() {{")
     print("Inserted new renderChart function")
else:
    # renderChart exists. Is it the old one or new one?
    # If it has "Plotly.newPlot", it's the old one (logic failed).
    pass

# 3. Insert global toggle functions
# Start matching stricter to avoid double insertion
toggle_funcs_signature = 'window.togExpand = (k) =>'
if toggle_funcs_signature not in content:
    toggle_funcs = r'''
    window.togExpand = (k) => {{
        const key = state.race + '-' + k;
        state.expanded[key] = !state.expanded[key];
        render();
    }};
    window.togMatchup = (k) => {{
        const key = state.race + '-m-' + k;
        state.expanded[key] = !state.expanded[key];
        render();
    }};

'''
    if "function init() {{" in content:
        content = content.replace("function init() {{", toggle_funcs + "function init() {{")
        print("Inserted global toggle functions")
else:
    print("Toggle functions already present")

with open(target_file, 'w', encoding='utf-8') as f:
    f.write(content)
