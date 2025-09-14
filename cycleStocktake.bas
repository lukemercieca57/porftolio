'' This VBA code is part of an Excel sheet used for cycle stock counts.
'' Data from the inventory system is imported using a Power Query connection to Azure SQL.
'' The code selects items to be counted, generates the stock count list and submits the results to the SQL database.
'' The Excel sheet was made to allow for stock counts to be done with a barcode scanner.

Sub UpdatePick()
    Dim tableTarget As ListObject, tableSource As ListObject, checkSource As ListColumn
    Dim keySource As ListColumn, keyTarget As ListColumn, nameTarget As ListColumn
    Dim cell As Range
    Dim i As Long, j As Long, lastRow As Long

    Set tableSource = ThisWorkbook.Worksheets("configData").ListObjects("famlist")
    Set tableTarget = ThisWorkbook.Worksheets("pick").ListObjects("pick")

    Set checkSource = tableSource.ListColumns("check")
    Set keySource = tableSource.ListColumns("Key")
    Set keyTarget = tableTarget.ListColumns("Key")
    Set nameTarget = tableTarget.ListColumns("FamilyCode")
    
    ThisWorkbook.Worksheets("pick").Unprotect Password:=""

    For Each cell In checkSource.DataBodyRange
        If cell.Value = "add" Then
            lastRow = tableTarget.ListRows.Count + 1
            keyTarget.DataBodyRange.Cells(lastRow).Value = keySource.DataBodyRange.Cells(cell.Row - tableSource.HeaderRowRange.Row).Value
        End If
    Next cell

    For i = tableTarget.ListRows.Count To 1 Step -1
        If IsEmpty(nameTarget.DataBodyRange.Cells(i).Value) Or nameTarget.DataBodyRange.Cells(i).Value = "" Then
            tableTarget.ListRows(i).Delete
        End If
    Next i

    With tableTarget.Sort
        .SortFields.Clear
        .SortFields.Add Key:=tableTarget.ListColumns("OnHand").Range, _
                        SortOn:=xlSortOnValues, _
                        Order:=xlDescending, _
                        DataOption:=xlSortNormal
        .Header = xlYes
        .Apply
    End With

    ThisWorkbook.Worksheets("pick").Protect Password:=""

    Dim ws As WorkSheet
    Dim table As ListObject
    Dim onhand As Range, cls As Range, pick As Range, code As Range, picked As Range, sorted As Range
    Dim tCount As Long, maxsort As Long
    Dim items As Collection
    Dim item As Variant
    Dim classes As Variant, c As Variant, classes2 As Variant

    Set ws = ThisWorkbook.Worksheets("pick")
    Set table = ws.ListObjects("pick")
    Set items = New Collection
    lastRow = table.ListRows.Count

    Set onhand = table.ListColumns("OnHand").DataBodyRange
    Set cls = table.ListColumns("Class").DataBodyRange
    Set pick = table.ListColumns("ToCount").DataBodyRange
    Set code = table.ListColumns("FamilyCode").DataBodyRange
    Set picked = table.ListColumns("Picked").DataBodyRange
    Set sorted = table.ListColumns("NextIndex").DataBodyRange

    classes = Array("A", "B", "C", "D")
    classes2 = Array("B", "C", "D", "A")
    tCount = ws.Cells(1, 3).Value

    For i = 1 To lastRow
        If picked(i) = "yes" Then picked(i) = "" 
    Next i

    If tCount > 0 Then
        For Each c In classes
            For i = 1 To lastRow
                If cls(i) = c And pick(i) = "yes" And onhand(i) <= tCount Then
                    items.Add code(i)
                    tCount = tCount - onhand(i)
                End If
                If tCount <= 0 Then Exit For
            Next i
            If tCount <= 0 Then Exit For
        Next c

        For Each item In items
            For i = 1 To lastRow
                If code(i) = item Then
                    picked(i) = "yes"
                    Exit For
                End If
            Next i
        Next item
    End If
    
    If tCount > 0 Then
        Set items = New Collection
        maxsort = ws.Cells(3, 3).Value
        For j = 1 To maxsort
            For Each c In classes2
                For i = 1 To lastRow
                    If cls(i) = c And sorted(i) = j And onhand(i) <= tCount Then
                        items.Add code(i)
                        tCount = tCount - onhand(i)
                    End If
                    If tCount <= 0 Then Exit For
                Next i
                If tCount <= 0 Then Exit For
            Next c
            If tCount <= 0 Then Exit For
        Next j

        For Each item In items
            For i = 1 To lastRow
                If code(i) = item Then
                    picked(i) = "yes"
                    Exit For
                End If
            Next i
        Next item
    End If

    MsgBox "Items Picked!", vbInformation
End Sub

Sub UpdateStockList()
    Dim tableTarget As ListObject
    Dim tableSource As ListObject
    Dim tableSource2 As ListObject
    Dim tableSource3 As ListObject
    Dim checkSource As ListColumn
    Dim checkSource2 As ListColumn
    Dim checkSource3 As ListColumn
    Dim keySource As ListColumn
    Dim keySource2 As ListColumn
    Dim keySource3 As ListColumn
    Dim keyTarget As ListColumn
    Dim checkTarget As ListColumn
    Dim cell As Range
    Dim lastRow As Long
    Dim i As Long

    Set tableSource = ThisWorkbook.Worksheets("stockData").ListObjects("stockData")
    Set tableSource2 = ThisWorkbook.Worksheets("Barcodes").ListObjects("barcodes")
    Set tableSource3 = ThisWorkbook.Worksheets("Manual").ListObjects("manual")
    Set tableTarget = ThisWorkbook.Worksheets("Stocktake").ListObjects("stockTake")

    Set checkSource = tableSource.ListColumns("check")
    Set checkSource2 = tableSource2.ListColumns("check")
    Set checkSource3 = tableSource3.ListColumns("check")
    Set keySource = tableSource.ListColumns("key")
    Set keySource2 = tableSource2.ListColumns("key")
    Set keySource3 = tableSource3.ListColumns("key")
    Set keyTarget = tableTarget.ListColumns("key")
    Set checkTarget = tableTarget.ListColumns("check")
    
    ThisWorkbook.Worksheets("Stocktake").Unprotect Password:=""

    For Each cell In checkSource.DataBodyRange
        If cell.Value = "add" Then
            lastRow = tableTarget.ListRows.Count + 1
            keyTarget.DatabodyRange.Cells(lastRow).Value = keySource.DatabodyRange.Cells(cell.Row - tableSource.HeaderRowRange.Row).Value
        End If
    Next cell

    For Each cell In checkSource2.DataBodyRange
        If cell.Value = "add" Then
            lastRow = tableTarget.ListRows.Count + 1
            keyTarget.DatabodyRange.Cells(lastRow).Value = keySource2.DatabodyRange.Cells(cell.Row - tableSource2.HeaderRowRange.Row).Value
        End If
    Next cell

    For Each cell In checkSource3.DataBodyRange
        If cell.Value = "add" Then
            lastRow = tableTarget.ListRows.Count + 1
            keyTarget.DatabodyRange.Cells(lastRow).Value = keySource3.DatabodyRange.Cells(cell.Row - tableSource3.HeaderRowRange.Row).Value
        End If
    Next cell

    For i = tableTarget.ListRows.Count To 1 Step -1
        If IsEmpty(checkTarget.DatabodyRange.Cells(i).Value) or checkTarget.DatabodyRange.Cells(i).Value = "" Then
            tableTarget.ListRows(i).Delete
        End If
    Next i

    With tableTarget.Sort
        .SortFields.Clear
        .SortFields.Add Key:=tableTarget.ListColumns("SKU").Range, _
                        SortOn:=xlSortOnValues, _
                        Order:=xlAscending, _
                        DataOption:=xlSortNormal
        .Header = xlYes
        .Apply
    End With

    ThisWorkbook.Worksheets("Stocktake").Protect Password:=""

    MsgBox "List Updated!", vbInformation
End Sub

Sub SubmitStocktake()
    Dim conn As Object
    Dim connectionString As String
    Dim ws As Worksheet
    Dim table As ListObject
    Dim i As Long
    Dim stdate As Date, expirydate As Variant
    Dim location As String, sku As String, name As String, lot As String, ptype As String, week As String, countedby As String, action As String, trf_location As String, key As String, status As String, comment As String
    Dim scanned As Long, manual As Long, count As Long
    Dim query As String

    connectionString = "Driver={ODBC Driver 17 for SQL Server};Server=;" & _
                       "Database=;" & _
                       "Uid=;" & _
                       "Pwd=;" & _
                       "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

    Set conn = CreateObject("ADODB.Connection")
    conn.Open connectionString

    Set ws = ThisWorkbook.Sheets("Stocktake")
    Set table = ws.ListObjects("Stocktake")

    If ws.Cells(7,6).Value = "Submitted" Then
        MsgBox "Stocktake has already been submitted!", vbExclamation
        Exit Sub
    ElseIf ws.Cells(2,7).Value = "Update" Then
        MsgBox "Press Update before submitting!", vbExclamation
        Exit Sub
    ElseIf ws.Cells(2,9).Value = "Error" Then
        MsgBox "Missing or Duplicate Data!", vbExclamation
        Exit Sub
    ElseIf ws.Cells(8,9).Value <> 0 Then
        MsgBox "Actions Required!", vbExclamation
        Exit Sub
    End If

    stdate = ws.Cells(4,6).Value
    week = ws.Cells(5,6).Value
    countedby = ws.Cells(6,6).Value

    For i = 1 To table.ListRows.Count
        key = table.ListColumns("key").DataBodyRange(i).Value
        sku = table.ListColumns("SKU").DataBodyRange(i).Value
        name = table.ListColumns("Name").DataBodyRange(i).Value
        lot = table.ListColumns("Lot").DataBodyRange(i).Value
        onhand = table.ListColumns("OnHand").DataBodyRange(i).Value
        unitcost = table.ListColumns("UnitCost").DataBodyRange(i).Value
        scanned = table.ListColumns("Scanned").DataBodyRange(i).Value
        count = table.ListColumns("Count").DataBodyRange(i).Value
        variance = table.ListColumns("Variance").DataBodyRange(i).Value
        varvalue = table.ListColumns("Var $").DataBodyRange(i).Value
        action = table.ListColumns("Action").DataBodyRange(i).Value
        trf_location = table.ListColumns("Location").DataBodyRange(i).Value

        If Trim(table.ListColumns("ExpiryDate").DataBodyRange(i).Value & "") = "" Then
            expirydate = "NULL"
        Else
            expirydate = "'" & format(table.ListColumns("ExpiryDate").DataBodyRange(i).Value, "yyyy-mm-dd") & "'"
        End If

        If IsEmpty(table.ListColumns("Manual").DataBodyRange(i).Value) Then
            manual = 0
        Else
            manual = table.ListColumns("Manual").DataBodyRange(i).Value
        End If

        If IsEmpty(table.ListColumns("Action").DataBodyRange(i).Value) or Trim(table.ListColumns("Action").DataBodyRange(i).Value & "") = "" Then
            status = ""
        Else
            status = "Pending"
        End If

        query = "INSERT INTO AOF_FGstocktakeHistory (ID, Date, Week, Location, CountedBy, SKU, Name, Lot, ExpiryDate, OnHand, UnitCost, Scanned, Manual, Count, Variance, VarValue, Action, Trf_Location, Status, Comment) VALUES('" & _
            key & "', " & "CONVERT(date, '" & Format(stdate, "yyyy-mm-dd") & "', 23), '" & week & "', 'Finished Goods', '" & countedby & "', '" & _ 
            sku & "', '" & name & "', '" & lot & "', " & expirydate & ", " & onhand & " , " & unitcost & " , " & scanned & ", " & manual & ", " & count & ", " & variance & ", " & varvalue & ", '" & action & "', '" & trf_location & "', '" &  status & "', '" & comment & "')"
        
        conn.Execute query
    Next i

    conn.Close
    Set conn = Nothing

    ThisWorkbook.Queries("LastCounted").Refresh
    MsgBox "Stocktake submitted!", vbInformation
End Sub