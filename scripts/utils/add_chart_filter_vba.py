#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ExcelファイルにVBAマクロを追加し、.xlsm形式で保存するスクリプト

【機能】
Analysis_シートの「表示」列（✓）を切り替えると、
対応する馬のチャート上のシリーズが非表示/表示される

【使い方】
python add_chart_filter_vba.py race_analysis_20251214_中山.xlsx
"""

import os
import sys

def add_vba_to_excel(xlsx_path: str) -> str:
    """
    ExcelファイルにVBAマクロを追加し、.xlsm形式で保存
    
    Args:
        xlsx_path: 入力の.xlsxファイルパス
    
    Returns:
        出力の.xlsmファイルパス
    """
    try:
        import win32com.client as win32
    except ImportError:
        print("win32comがインストールされていません。")
        print("pip install pywin32 を実行してください。")
        return None
    
    # 絶対パスに変換
    xlsx_path = os.path.abspath(xlsx_path)
    xlsm_path = xlsx_path.replace('.xlsx', '.xlsm')
    
    print(f"入力ファイル: {xlsx_path}")
    print(f"出力ファイル: {xlsm_path}")
    
    # Excelを起動（Dispatchを直接使用してキャッシュ問題を回避）
    excel = win32.Dispatch('Excel.Application')
    excel.Visible = False
    excel.DisplayAlerts = False
    
    try:
        # ワークブックを開く
        wb = excel.Workbooks.Open(xlsx_path)
        
        # VBAプロジェクトにモジュールを追加
        vba_module = wb.VBProject.VBComponents.Add(1)  # 1 = vbext_ct_StdModule
        vba_module.Name = "ChartFilter"
        
        # VBAコードを追加
        vba_code = '''
' チャートの馬別表示切替機能
' Analysis_シートの「表示」列（AG列）の✓を消すと対応する馬がチャートから非表示になる

Sub UpdateChartVisibility()
    Dim ws As Worksheet
    Dim chartObj As ChartObject
    Dim series As series
    Dim i As Integer
    Dim visCell As Range
    Dim horseName As String
    Dim isVisible As Boolean
    
    ' Analysis_で始まるシートを処理
    For Each ws In ThisWorkbook.Worksheets
        If Left(ws.Name, 9) = "Analysis_" Then
            ' 各馬の表示状態を確認
            For i = 2 To 20  ' 最大18頭 + ヘッダー
                Set visCell = ws.Cells(i, 33)  ' AG列
                horseName = ws.Cells(i, 35).Value  ' AI列 = 馬名
                
                If horseName <> "" Then
                    isVisible = (visCell.Value = "✓")
                    
                    ' 全てのチャートを処理
                    For Each chartObj In ws.ChartObjects
                        For Each series In chartObj.Chart.SeriesCollection
                            If InStr(series.Name, horseName) > 0 Then
                                ' シリーズの表示/非表示を切り替え
                                On Error Resume Next
                                If isVisible Then
                                    series.Format.Line.Visible = msoTrue
                                    series.MarkerStyle = xlMarkerStyleCircle
                                Else
                                    series.Format.Line.Visible = msoFalse
                                    series.MarkerStyle = xlMarkerStyleNone
                                End If
                                On Error GoTo 0
                            End If
                        Next series
                    Next chartObj
                End If
            Next i
        End If
    Next ws
End Sub

' ワークシート変更時に自動実行
Private Sub Workbook_SheetChange(ByVal Sh As Object, ByVal Target As Range)
    ' AG列（33列目）が変更された場合のみ実行
    If Target.Column = 33 Then
        If Left(Sh.Name, 9) = "Analysis_" Then
            Call UpdateChartVisibility
        End If
    End If
End Sub
'''
        
        vba_module.CodeModule.AddFromString(vba_code)
        
        # ThisWorkbookにイベントコードを追加
        thisworkbook_code = '''
Private Sub Workbook_SheetChange(ByVal Sh As Object, ByVal Target As Range)
    ' AG列（33列目）が変更された場合のみ実行
    If Target.Column = 33 Then
        If Left(Sh.Name, 9) = "Analysis_" Then
            Call UpdateChartVisibility
        End If
    End If
End Sub
'''
        
        # ThisWorkbookモジュールを取得してコードを追加
        for comp in wb.VBProject.VBComponents:
            if comp.Name == "ThisWorkbook":
                comp.CodeModule.AddFromString(thisworkbook_code)
                break
        
        # .xlsm形式で保存
        wb.SaveAs(xlsm_path, FileFormat=52)  # 52 = xlOpenXMLWorkbookMacroEnabled
        
        print(f"VBAマクロを追加して保存しました: {xlsm_path}")
        print("\n【使い方】")
        print("1. Excelでファイルを開く")
        print("2. マクロを有効化する")
        print("3. Analysis_シートのAG列「表示」の「✓」を消すと馬がチャートから消えます")
        
        return xlsm_path
        
    except Exception as e:
        print(f"エラー: {e}")
        return None
        
    finally:
        wb.Close(SaveChanges=False)
        excel.Quit()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("使い方: python add_chart_filter_vba.py <excelファイル.xlsx>")
        sys.exit(1)
    
    xlsx_file = sys.argv[1]
    if not os.path.exists(xlsx_file):
        print(f"ファイルが見つかりません: {xlsx_file}")
        sys.exit(1)
    
    result = add_vba_to_excel(xlsx_file)
    if result:
        print(f"\n成功: {result}")
    else:
        print("\n失敗しました")
        sys.exit(1)
