
import os
import re

target_file = r'C:\Users\zk-ht\Keiba\Keiba_AI_v2\scripts\reports\create_interactive_charts.py'

with open(target_file, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Fix Footer Overlap (Increase padding)
# padding-bottom: calc(70px + var(--safe-b));
if "padding-bottom: calc(70px + var(--safe-b));" in content:
    content = content.replace("padding-bottom: calc(70px + var(--safe-b));", "padding-bottom: calc(100px + var(--safe-b));")
    print("Increased footer padding")
else:
    print("Could not find footer padding style")

# 2. Restore Chart Labels (Remove isMobile checks for labels)
# xaxis:{zeroline:false, showticklabels: !isMobile} -> xaxis:{zeroline:false, showticklabels: true}
content = content.replace("showticklabels: !isMobile", "showticklabels: true")
content = content.replace("title: isMobile ? '' :", "title:")
print("Restored chart labels")

# 3. Enhance Filter (Replace renderFilters) -- Redefining the function entirely
new_render_filters = r'''
    function renderFilters() {{
        const rows = DATA.races[state.race]?.rows || [];
        const info = DATA.race_info[state.race] || {{}};
        const allVenues = [...new Set(rows.map(r => r['場所']).filter(v => v))];
        const allDists = [...new Set(rows.map(r => r['距離']).filter(v => v))];
        const allCourses = [...new Set(rows.map(r => r['ｺｰｽ'] || r['コース']).filter(v => v))];

        const JRA = ['札幌', '函館', '福島', '新潟', '東京', '中山', '中京', '京都', '阪神', '小倉'];

        const renderChips = (items, currentFilters, type) => {{
            return items.map(item => {{
                // excluded list
                const isExcluded = currentFilters.includes(item);
                const isActive = !isExcluded; 
                // Active(visible) = Primary color. Excluded(hidden) = Grey/White.
                const style = isActive 
                    ? 'background:var(--primary);color:#fff;border-color:var(--primary);' 
                    : 'background:#fff;color:#ccc;border:1px solid #ddd;';
                
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
                     <div class="filter-wrap-row" style="border-bottom:1px dashed #eee; padding-bottom:8px; margin-bottom:8px;">
                        <div class="filter-label-fixed">一括</div>
                        <div class="filter-chip" onclick="applyQuickFilter('JRA')" style="border:1px solid #666;background:#eee;">中央のみ</div>
                        <div class="filter-chip" onclick="applyQuickFilter('NAR')" style="border:1px solid #666;background:#eee;">地方のみ</div>
                        <div class="filter-chip" onclick="applyQuickFilter('same_venue')" style="border:1px solid #666;background:#eee;">同場所</div>
                        <div class="filter-chip" onclick="applyQuickFilter('same_dist')" style="border:1px solid #666;background:#eee;">同距離</div>
                        <div class="filter-chip" onclick="applyQuickFilter('reset')" style="border:1px solid #666;background:#fff;">全表示</div>
                    </div>
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

# Replace existing renderFilters function
# Regex to replace functional block: function renderFilters() {{ ... }}
# Logic: Find start, find matching brace logic is hard in regex.
# Assumption: renderFilters() is followed by `function renderChart() {{` (based on restore_filters.py)
# Actually in `fix_syntax_error.py` we saw `renderFilters` comes before `renderChart`.

# Let's try to match from `function renderFilters() {{` up to `function renderChart() {{`
# and replace it.
pattern = r'function renderFilters\(\) \{\{.*?function renderChart\(\) \{\{'
replacement = new_render_filters + "\n\n    function renderChart() {{"

# We need re.DOTALL
match = re.search(pattern, content, re.DOTALL)
if match:
    content = content.replace(match.group(0), replacement)
    print("Replaced renderFilters function")
else:
    print("Could not find renderFilters function block")

# 4. Inject applyQuickFilter
quick_filter_code = r'''
    window.applyQuickFilter = (type) => {{
        const rows = DATA.races[state.race]?.rows || [];
        const info = DATA.race_info[state.race] || {{}};
        const allVenues = [...new Set(rows.map(r => r['場所']).filter(v => v))];
        const allDists = [...new Set(rows.map(r => r['距離']).filter(v => v))];
        const JRA = ['札幌', '函館', '福島', '新潟', '東京', '中山', '中京', '京都', '阪神', '小倉'];
        
        // Logic: Exclude venues that DO NOT match criteria
        let newVenues = [];
        let newDists = [];

        if(type === 'reset') {{
            setState({{venueFilters: [], distFilters: [], courseFilters: []}});
            return;
        }}

        if(type === 'JRA') {{
            // Hide non-JRA
            newVenues = allVenues.filter(v => !JRA.includes(v));
        }} else if(type === 'NAR') {{
             // Hide JRA
             newVenues = allVenues.filter(v => JRA.includes(v));
        }} else if(type === 'same_venue') {{
             // Hide others
             newVenues = allVenues.filter(v => v !== info.place);
        }} else if(type === 'same_dist') {{
             // Hide others
             newDists = allDists.filter(d => d !== info.dist);
        }}

        const s = {{}};
        if(type!=='same_dist') s.venueFilters = newVenues;
        if(type==='same_dist') s.distFilters = newDists;
        
        setState(s);
    }};
'''

# Inject before window.toggleFilter
if "window.toggleFilter =" in content:
    content = content.replace("window.toggleFilter =", quick_filter_code + "window.toggleFilter =")
    print("Injected applyQuickFilter")
else:
    print("Could not find window.toggleFilter")

with open(target_file, 'w', encoding='utf-8') as f:
    f.write(content)
