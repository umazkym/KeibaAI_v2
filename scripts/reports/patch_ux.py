
import os

target_file = r'C:\Users\zk-ht\Keiba\Keiba_AI_v2\scripts\reports\create_interactive_charts.py'

# Raw JS content (using single braces typical for JS)
# We will double them before injecting into the f-string file
new_render_chart_raw = r'''    // --- CHART ---
    function renderChart() {
        const d = DATA.chart_data[state.race];
        const blocked = state.filters[state.race] || [];
        const active = d.horses.filter(h => !blocked.includes(h.umaban));
        if(!active.length) return;
        
        const rows = DATA.races[state.race]?.rows || [];
        const rowMap = {}; // date+umaban -> row
        rows.forEach(r => {
            const key = r['日付'] + '-' + r['番'];
            rowMap[key] = r;
        });

        const pts = [];
        active.forEach(h => h.records.forEach(r => {
            // Date Filter (範囲指定)
            if(state.filterDateFrom && r.date < state.filterDateFrom) return;
            if(state.filterDateTo && r.date > state.filterDateTo) return;
            
            // Lookup row for venue/dist
            const rowKey = r.date + '-' + h.umaban;
            const row = rowMap[rowKey];
            
            // Venue Filter (exclude if in list)
            if(state.venueFilters.length > 0 && row && state.venueFilters.includes(row['場所'])) return;
            
            // Distance Filter (exclude if in list)
            if(state.distFilters.length > 0 && row && state.distFilters.includes(row['距離'])) return;
            
            // Course Filter (exclude if in list)
            if(state.courseFilters.length > 0 && state.courseFilters.includes(r.course)) return;
            
            // 場所情報を取得
            const venue = row ? row['場所'] || '' : '';
            const dist = row ? row['距離'] || '' : '';
            const rank = row ? row['着順'] || '' : '';
            const margin = row ? row['着差'] || '' : '';
            pts.push({...r, u:h.umaban, w:h.waku, course:r.course||'', venue: venue, dist: dist, rank: rank, margin: margin});
        }));
        
        // Dynamic chart height based on screen width
        const isMobile = window.innerWidth <= 600;
        const chartHeight = isMobile ? '220px' : '280px';
        const chartMargin = isMobile ? 'margin-bottom:20px;' : 'margin-bottom:10px;';
        
        // Mobile-optimized layout
        const layout = { 
            margin: isMobile ? {t:10,b:25,l:25,r:10} : {t:10,b:30,l:30,r:10}, 
            xaxis:{zeroline:false, showticklabels: !isMobile}, 
            yaxis:{zeroline:false, showticklabels: !isMobile}, 
            showlegend:false,
            autosize: true
        };
        const cfg = {displayModeBar:false, responsive:true};
        
        // 芝=明るい緑, ダート=オレンジ (色覚に配慮した高彩度配色)
        const courseColor = (c) => c === '芝' ? '#00C853' : (c === 'ダート' || c === 'ダ') ? '#FF6D00' : '#888';
        const borderColors = pts.map(p => courseColor(p.course));
        
        // オッズ情報を取得するヘルパー
        const getOddsText = (u) => {
            const o = DATA.odds?.[state.race]?.[u];
            if(o && o.odds) return `${o.odds}倍 ${o.ninki}人気`;
            return '';
        };
        
        // 場所名を短縮（最初の1-2文字）
        const shortVenue = (v) => {
            if(!v) return '';
            const map = {'中山':'山', '東京':'東', '阪神':'阪', '京都':'京', '中京':'中', '札幌':'札', '函館':'函', '新潟':'潟', '福島':'福', '小倉':'倉'};
            return map[v] || v.charAt(0);
        };
        
        // 共通のマーカー設定
        const mainTrace = (xData, yData) => ({
            x: xData, y: yData, mode:'markers+text', type:'scatter',
            marker: {size: isMobile ? 12 : 14, color:pts.map(p=>WAKU[p.w]), line:{width:2,color:borderColors}},
            text: pts.map(p=>p.u), textposition:'middle center', textfont:{size: isMobile ? 8 : 9, color:pts.map(p=>TXT[p.w])},
            hovertext: pts.map(p=>`${p.u}番 ${getOddsText(p.u)}<br>${p.date} ${p.venue} ${p.course} ${p.dist}m<br>${p.rank}着 / ${p.margin}`)
        });
        
        // 場所ラベル用トレース
        const venueTrace = (xData, yData) => ({
            x: xData, y: yData, mode:'text', type:'scatter',
            text: pts.map(p=>shortVenue(p.venue)), 
            textposition:'bottom right', 
            textfont:{size:isMobile ? 6 : 7, color:'#666'},
            hoverinfo:'skip'
        });
        
        // Set container heights directly
        ['c1', 'c2', 'c3', 'c4'].forEach(id => {
            const el = document.getElementById(id);
            if(el) {
                 el.style.height = chartHeight;
                 if(isMobile) el.style.marginBottom = '20px';
            }
        });
        
        Plotly.newPlot('c1', [
            mainTrace(pts.map(p=>p.l3f_idx), pts.map(p=>p.time_idx)),
            venueTrace(pts.map(p=>p.l3f_idx), pts.map(p=>p.time_idx))
        ], {...layout, xaxis:{...layout.xaxis, title: isMobile ? '' : '上がり指数'}, yaxis:{...layout.yaxis, title: isMobile ? '' : 'T指数'}}, cfg);
        
        Plotly.newPlot('c2', [
            mainTrace(pts.map(p=>p.c1_ratio), pts.map(p=>p.time_idx)),
            venueTrace(pts.map(p=>p.c1_ratio), pts.map(p=>p.time_idx))
        ], {...layout, xaxis:{...layout.xaxis, title: isMobile ? '' : '位置取り'}, yaxis:{...layout.yaxis, title: isMobile ? '' : 'T指数'}}, cfg);

        Plotly.newPlot('c3', [
            mainTrace(pts.map(p=>p.c1_ratio), pts.map(p=>p.l3f_idx)),
            venueTrace(pts.map(p=>p.c1_ratio), pts.map(p=>p.l3f_idx))
        ], {...layout, xaxis:{...layout.xaxis, title: isMobile ? '' : '位置取り'}, yaxis:{...layout.yaxis, title: isMobile ? '' : '上がり指数'}, autorange:'reversed'}, cfg);

        Plotly.newPlot('c4', [
            mainTrace(pts.map(p=>p.rpci), pts.map(p=>p.l3f_idx)),
            venueTrace(pts.map(p=>p.rpci), pts.map(p=>p.l3f_idx))
        ], {...layout, xaxis:{...layout.xaxis, title: isMobile ? '' : 'RPCI'}, yaxis:{...layout.yaxis, title: isMobile ? '' : '上がり指数'}}, cfg);

    }'''

new_render_data_raw = r'''    // --- DATA ---
    function renderData() {
        const rows = DATA.races[state.race]?.rows || [];
        const headers = COLS.map(c => 
            `<div class="sort-chip ${state.sortCol===c?'active':''}" onclick="setState({sortCol:'${c}', sortAsc:${state.sortCol===c?!state.sortAsc:true} })">
                ${c} ${state.sortCol===c ? (state.sortAsc?'▲':'▼') : ''}
             </div>`
        ).join('');

        let content = '';
        if(state.sortCol) {
            const sorted = [...rows].sort((a,b) => {
                let va = a[state.sortCol]||'', vb = b[state.sortCol]||'';
                let na = parseFloat(va), nb = parseFloat(vb);
                if(!isNaN(na) && !isNaN(nb)) return state.sortAsc ? na-nb : nb-na;
                return state.sortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
            });
            content = sorted.map(row => `
                <div class="card" style="padding:8px 12px; margin-bottom:0; border-bottom:1px solid #eee; border-radius:0; box-shadow:none;">
                    <div class="flex-ac gap-2" style="margin-bottom:4px; padding-bottom:4px;">
                        <div class="h-num" style="background:${WAKU[getWaku(row['番'])] || '#ccc'}; color:${TXT[getWaku(row['番'])] || '#fff'}">${row['番']}</div>
                        <div class="text-xs font-bold" style="flex:1">${row['馬名']}</div>
                        <div class="text-xs text-light">${row['日付']}</div>
                        <div class="font-bold text-xs" style="color:${state.sortCol}">${row[state.sortCol]}</div>
                    </div>
                    <div class="grid-dense">
                        ${COLS.map(c => `<div class="gd-cell"><span class="gd-lbl">${c}</span><div class="gd-val">${row[c]||'-'}</div></div>`).join('')}
                    </div>
                </div>
            `).join('');
            content = `<div class="card" onclick="setState({sortCol:null})" style="text-align:center;padding:12px;color:var(--primary);font-weight:bold;cursor:pointer;margin-bottom:0;border-bottom:1px solid #eee;">↺ グループ表示に戻す</div>` + content;

        } else {
            const groups = {};
            rows.forEach(r => {
                const k = r['番']||'0';
                if(!groups[k]) groups[k] = [];
                groups[k].push(r);
            });
            content = Object.keys(groups).sort((a,b)=>parseInt(a)-parseInt(b)).map(k => {
                const list = groups[k];
                const meta = list[0];
                const w = getWaku(k);
                const open = state.expanded[state.race+'-'+k];
                const arrow = open ? '▲' : '▼';
                const body = open ? `
                    <div style="margin-top:8px">
                        ${list.map(row => `
                            <div style="margin-bottom:8px; border-bottom:1px dashed #eee; padding-bottom:4px;">
                                <div class="grid-dense">
                                    ${COLS.map(c => `<div class="gd-cell"><span class="gd-lbl">${c}</span><div class="gd-val">${row[c]||'-'}</div></div>`).join('')}
                                </div>
                            </div>
                        `).join('')}
                    </div>` : '';
                return `
                <div class="card" style="padding:10px 12px; cursor:pointer; margin-bottom:0; border-bottom:1px solid #eee; border-radius:0; box-shadow:none;" onclick="togExpand('${k}')">
                    <div class="flex-ac">
                        <div class="h-num" style="background:${WAKU[w]}; color:${TXT[w]}">${k}</div>
                        <div class="flex-c" style="flex:1">
                            <div class="font-bold text-sm">${meta['馬名']}</div>
                            <div class="text-xs text-light">${meta['騎手']} (${list.length}走)</div>
                        </div>
                        <div class="text-light">${arrow}</div>
                    </div>
                    ${body}
                </div>`;
            }).join('');
        }
        return `<div class="card"><div class="font-bold text-xs mb-2">▼ 並び替え（タップでランキング表示）</div><div class="sort-area">${headers}</div></div>${content}`;
    }'''

# Double braces for f-string compatibility
new_render_chart = new_render_chart_raw.replace('{', '{{').replace('}', '}}')
new_render_data = new_render_data_raw.replace('{', '{{').replace('}', '}}')

with open(target_file, 'r', encoding='utf-8') as f:
    content = f.read()

# Replace renderChart
start_marker_chart = "    // --- CHART ---"
end_marker_chart = "    // --- DATA ---"
start_idx = content.find(start_marker_chart)
end_idx = content.find(end_marker_chart)

if start_idx != -1 and end_idx != -1:
    content = content[:start_idx] + new_render_chart + "\n\n" + content[end_idx:]
    print("Replaced renderChart")
else:
    print("Could not find renderChart block")

# Replace renderData
start_marker_data = "    // --- DATA ---"
end_marker_data = "    // --- MATCHUP ---"
start_idx = content.find(start_marker_data)
end_idx = content.find(end_marker_data)

if start_idx != -1 and end_idx != -1:
    content = content[:start_idx] + new_render_data + "\n\n" + content[end_idx:]
    print("Replaced renderData")
else:
    print("Could not find renderData block")

with open(target_file, 'w', encoding='utf-8') as f:
    f.write(content)
