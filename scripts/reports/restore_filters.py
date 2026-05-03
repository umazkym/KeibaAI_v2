
import os
import re

target_file = r'C:\Users\zk-ht\Keiba\Keiba_AI_v2\scripts\reports\create_interactive_charts.py'

with open(target_file, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Define renderFilters function
render_filters_code = r'''
    function renderFilters() {{
        const rows = DATA.races[state.race]?.rows || [];
        const allVenues = [...new Set(rows.map(r => r['場所']).filter(v => v))];
        const allDists = [...new Set(rows.map(r => r['距離']).filter(v => v))];
        const allCourses = [...new Set(rows.map(r => r['ｺｰｽ'] || r['コース']).filter(v => v))];

        const renderChips = (items, currentFilters, type) => {{
            return items.map(item => {{
                const isActive = !currentFilters.includes(item);
                const style = isActive ? 'background:var(--primary);color:#fff;border-color:var(--primary);' : 'background:#fff;color:#333;';
                return `<div class="filter-chip" style="${{style}}" onclick="toggleFilter('${{type}}', '${{item}}')">${{item}}</div>`;
            }}).join('');
        }};

        return `
            <div class="filter-sticky">
                <div class="flex-sb" onclick="setState({{showFilters: !state.showFilters}})" style="cursor:pointer; padding:4px;">
                    <div class="font-bold text-sm">🔍 絞り込み条件 (${{state.showFilters ? '閉じる' : '開く'}})</div>
                    <div>${{state.showFilters ? '▲' : '▼'}}</div>
                </div>
                ${{state.showFilters ? `
                <div class="filter-detail-content open">
                     <div class="filter-wrap-row">
                        <div class="filter-label-fixed">場所</div>
                        ${{renderChips(allVenues, state.venueFilters, 'venue')}}
                    </div>
                    <div class="filter-wrap-row">
                        <div class="filter-label-fixed">距離</div>
                        ${{renderChips(allDists, state.distFilters, 'dist')}}
                    </div>
                    <div class="filter-wrap-row">
                        <div class="filter-label-fixed">コース</div>
                        ${{renderChips(allCourses, state.courseFilters, 'course')}}
                    </div>
                </div>
                ` : ''}}
            </div>
        `;
    }}

'''

# 2. Insert renderFilters before renderChart
if "function renderChart() {{" in content:
    content = content.replace("function renderChart() {{", render_filters_code + "function renderChart() {{")
else:
    print("Could not find renderChart")

# 3. Update renderChart to call renderFilters
# We look for the return string in renderChart
# The current renderChart returns: `... <div id="c1" ...`
# we want: `return renderFilters() + `...`

# Regex to find the return statement in renderChart
pattern = r'(function renderChart\(\) \{\{.*?return `)(.*?`;)'
match = re.search(pattern, content, re.DOTALL)
if match:
    # We replace `return ` with `return renderFilters() + `
    # But we need to be careful with the capture groups.
    # match.group(0) is the whole block.
    # We want to replace "return `" with "return renderFilters() + `"
    # effectively inserting renderFilters() + before the template literal.
    
    # Let's simple string replace if unique enough?
    # `return ` is too common.
    # `const m = isMobile ? 'margin-bottom:20px' : 'margin-bottom:10px';` is unique-ish.
    
    marker = "const m = isMobile ? 'margin-bottom:20px' : 'margin-bottom:10px';"
    if marker in content:
        # Check if we already added it
        if "return renderFilters() +" not in content:
            content = content.replace(marker, marker + "\n        return renderFilters() + `")
            # We also need to remove the original `return ` that followed?
            # Wait, my previous script put:
            # const m = ...;
            # return `
            # ...
            
            # So if I replace `const m = ...;` with `const m = ...;\n return renderFilters() + `
            # Then the subsequent `return ` will be a syntax error?
            # No, the previous code was:
            # const m = ...;
            # return `...`;
            
            # If I allow the original `return` to stay, it becomes:
            # const m = ...;
            # return renderFilters() + `
            # return `...`;  <-- This return is NOT there.
            
            # Ah, I need to REMOVE the original `return `
            pass
        else:
             print("renderFilters() call already exists")
    else:
        print("Could not find marker in renderChart")

    # Better approach with Regex:
    # Find `return ` inside renderChart block and replace it.
    # We can assume renderChart is the one we defined.
    # Search for: `const m = ...;` followed by `return`
    content = re.sub(
        r"(const m = isMobile \? 'margin-bottom:20px' : 'margin-bottom:10px';\s*)return `",
        r"\1return renderFilters() + `",
        content
    )


# 4. Add toggleFilter global function
toggle_code = r'''
    window.toggleFilter = (type, val) => {{
        let list = [];
        if(type==='venue') list = state.venueFilters || [];
        if(type==='dist') list = state.distFilters || [];
        if(type==='course') list = state.courseFilters || [];

        const idx = list.indexOf(val);
        if(idx === -1) list.push(val);
        else list.splice(idx, 1);

        if(type==='venue') setState({{venueFilters: list}});
        if(type==='dist') setState({{distFilters: list}});
        if(type==='course') setState({{courseFilters: list}});
    }};
'''

if "window.togMatchup =" in content:
    if "window.toggleFilter =" not in content:
        content = content.replace("window.togMatchup =", toggle_code + "window.togMatchup =")
    else:
        print("toggleFilter already exists")
else:
    print("Could not find window.togMatchup")

with open(target_file, 'w', encoding='utf-8') as f:
    f.write(content)
