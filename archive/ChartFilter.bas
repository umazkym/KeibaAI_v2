Attribute VB_Name = "ChartFilter"
' ============================================
' Chart Horse Visibility Toggle
' ============================================
' Toggle visibility of horses on charts by editing
' the Show column (X=24) in Analysis_ sheets.
' Delete the check mark to hide, add it back to show.
'
' After editing, run: Alt+F8 -> UpdateChartVisibility
' ============================================

' Waku colors (same as Python code)
Private Function GetWakuColor(waku As Integer) As Long
    Select Case waku
        Case 1: GetWakuColor = RGB(255, 255, 255)
        Case 2: GetWakuColor = RGB(0, 0, 0)
        Case 3: GetWakuColor = RGB(255, 0, 0)
        Case 4: GetWakuColor = RGB(0, 0, 255)
        Case 5: GetWakuColor = RGB(255, 255, 0)
        Case 6: GetWakuColor = RGB(0, 128, 0)
        Case 7: GetWakuColor = RGB(255, 165, 0)
        Case 8: GetWakuColor = RGB(255, 192, 203)
        Case Else: GetWakuColor = RGB(128, 128, 128)
    End Select
End Function

Private Function GetWakuFromUmaban(umaban As Integer) As Integer
    If umaban <= 2 Then
        GetWakuFromUmaban = 1
    ElseIf umaban <= 4 Then
        GetWakuFromUmaban = 2
    ElseIf umaban <= 6 Then
        GetWakuFromUmaban = 3
    ElseIf umaban <= 8 Then
        GetWakuFromUmaban = 4
    ElseIf umaban <= 10 Then
        GetWakuFromUmaban = 5
    ElseIf umaban <= 12 Then
        GetWakuFromUmaban = 6
    ElseIf umaban <= 14 Then
        GetWakuFromUmaban = 7
    Else
        GetWakuFromUmaban = 8
    End If
End Function

Sub UpdateChartVisibility()
    Dim ws As Worksheet
    Dim chartObj As ChartObject
    Dim ser As Series
    Dim i As Integer
    Dim visCell As Range
    Dim horseName As String
    Dim horseNum As String
    Dim seriesPattern As String
    Dim isVisible As Boolean
    Dim seriesName As String
    Dim matchCount As Integer
    Dim wakuNum As Integer
    Dim markerColor As Long
    
    ' Column positions: X=24 (Show), Y=25 (Umaban), Z=26 (Name)
    Const COL_SHOW As Integer = 24
    Const COL_UMABAN As Integer = 25
    Const COL_NAME As Integer = 26
    Const START_ROW As Integer = 3
    
    matchCount = 0
    
    For Each ws In ThisWorkbook.Worksheets
        If Left(ws.Name, 9) = "Analysis_" Then
            For i = START_ROW To START_ROW + 20
                Set visCell = ws.Cells(i, COL_SHOW)
                horseNum = CStr(ws.Cells(i, COL_UMABAN).Value)
                horseName = CStr(ws.Cells(i, COL_NAME).Value)
                
                If Len(horseName) > 0 And horseName <> "" Then
                    seriesPattern = "(" & horseNum & ") " & horseName
                    isVisible = (Len(visCell.Value) > 0 And visCell.Value <> "")
                    
                    If IsNumeric(horseNum) Then
                        wakuNum = GetWakuFromUmaban(CInt(horseNum))
                    Else
                        wakuNum = 0
                    End If
                    markerColor = GetWakuColor(wakuNum)
                    
                    For Each chartObj In ws.ChartObjects
                        On Error Resume Next
                        For Each ser In chartObj.Chart.SeriesCollection
                            seriesName = ser.Name
                            
                            If seriesName = seriesPattern Then
                                matchCount = matchCount + 1
                                If isVisible Then
                                    ser.MarkerStyle = xlMarkerStyleCircle
                                    ser.MarkerSize = 10
                                    ser.MarkerBackgroundColor = markerColor
                                    ser.MarkerForegroundColor = RGB(0, 0, 0)
                                Else
                                    ser.MarkerStyle = xlMarkerStyleNone
                                End If
                            End If
                        Next ser
                        On Error GoTo 0
                    Next chartObj
                End If
            Next i
        End If
    Next ws
    
    Application.StatusBar = "Updated " & matchCount & " chart series"
    MsgBox matchCount & " series updated.", vbInformation, "Complete"
End Sub

Sub ShowAllHorses()
    Dim ws As Worksheet
    Dim i As Integer
    
    Const COL_SHOW As Integer = 24
    Const COL_NAME As Integer = 26
    Const START_ROW As Integer = 3
    
    For Each ws In ThisWorkbook.Worksheets
        If Left(ws.Name, 9) = "Analysis_" Then
            For i = START_ROW To START_ROW + 20
                If Len(ws.Cells(i, COL_NAME).Value) > 0 Then
                    ws.Cells(i, COL_SHOW).Value = ChrW(10003)
                End If
            Next i
        End If
    Next ws
    
    Call UpdateChartVisibility
End Sub
