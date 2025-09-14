' VBA code used to process Stocktakes for external locations.
' The Excel sheet compares stocktake data to consigned stock data from Azure SQL database, and calculates adjustments and stock to be supplied and returned.
' The VBA code then posts the adjustments and stock transfers to the Inventory Management System for the dispatch team to action.

'Process StockTake
Sub CompleteST()
    Dim conn As Object
    Dim connectionString As String
    Dim ws As Worksheet
    Dim location As String, stdate As Date
    Dim query As String

    connectionString = "Driver={ODBC Driver 17 for SQL Server};Server=;" & _
                       "Database=;" & _
                       "Uid=;" & _
                       "Pwd=;" & _
                       "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

    Set ws = ThisWorkbook.Sheets("Stocktake_calc")
    location = ws.Cells(2, 2).Value
    stdate = ws.Cells(3, 2).Value

    query = "UPDATE AOF_StockTakeHistory SET Status = 'Processed' WHERE Location = '" & location & "' AND Date = '" & Format(stdate, "yyyy-mm-dd") & "';"

    If ws.Cells(3, 3).Value = "Processed" Then
        MsgBox "Stocktake has already been processed!", vbExclamation
        Exit Sub
    End If

    Set conn = CreateObject("ADODB.Connection")
    conn.Open connectionString
    conn.Execute query
    conn.Close
    Set conn = Nothing

    ThisWorkbook.Queries("STdate").Refresh
    MsgBox "Stocktake Completed!", vbInformation
End Sub

' Adjustments
Sub Adjustment()
    Dim ws As Worksheet
    Dim table As ListObject
    Dim lastRow As Long
    Dim i As Long
    Dim post As String
    Dim Lines As String
    Dim firstEntry As Boolean
    Dim location As String, expiryDate As String

    Set ws = ThisWorkbook.Sheets("Adjustments")
    Set table = ws.ListObjects("StockAdj")
    lastRow = table.ListRows.Count
    location = ws.Cells(3, 3).Value
    Lines = ""
    firstEntry = True
    
    For i = 1 to lastRow
        If Trim(table.ListColumns("Exclude").DataBodyRange(i).Value) <> "yes" Then
            If Not firstEntry Then
                Lines = Lines & ","
            End If

            If Trim(table.ListColumns("ExpiryDate").DataBodyRange(i).Value) <> "" Then
                expiryDate = Format(table.ListColumns("ExpiryDate").DataBodyRange(i).Value, "yyyy-mm-dd") & "T00:00:00"
            Else
                expiryDate = ""
            End If

            Lines = Lines & "{" & _
                """ProductID"":""" & table.ListColumns("ID").DataBodyRange(i).Value & """," & _
                """Quantity"":" & table.ListColumns("NewOnHand").DataBodyRange(i).Value & "," & _
                """UnitCost"":" & table.ListColumns("UnitCost").DataBodyRange(i).Value & "," & _
                """Location"":""" & location & """," & _
                """BatchSN"":""" & table.ListColumns("adjLot").DataBodyRange(i).Value & """," & _
                """ExpiryDate"":""" & expiryDate & """" & _
                "}"

            firstEntry = False
        End If
    Next i

    post = "{" & _
        """Status"":""DRAFT""," & _
        """EffectiveDate"":""" & Format(Now(), "yyyy-mm-dd") & "T00:00:00""," & _
        """Reference"":""STT - " & ws.Cells(3, 3).Value & """," & _     
        """Lines"":[" & Lines & "]" & _
        "}"

    Debug.Print post
    If Not PostToDear("stockadjustment", post) then Exit Sub

    MsgBox "Stock Adjustment posted to Dear.", vbInformation
End Sub

' Create Transfer
Sub Supply()
    Dim ws As Worksheet
    Dim postST As String

    Set ws = ThisWorkbook.Sheets("Transfers")

    postST = "{" & _
        """FromLocation"":""Finished Goods""," & _
        """ToLocation"":""" & ws.Cells(1, 2).Value & """," & _
        """Status"":""DRAFT""," & _
        """CompletionDate"":""" & Format(Now(), "yyyy-mm-dd") & "T00:00:00""," & _
        """RequiredByDate"":""" & Format(Now(), "yyyy-mm-dd") & "T00:00:00""," & _
        """Reference"":""STN - " & ws.Cells(1, 2).Value & """," & _
        """SkipOrder"":false," & _        
        """Lines"":[]" & _
        "}"

    Debug.Print postST
    If Not PostToDear("stockTransfer", postST) then Exit Sub
    ThisWorkbook.Queries("StockTransferList").Refresh
End Sub

' Create Return
Sub Returns()
    Dim ws As Worksheet
    Dim postST As String

    Set ws = ThisWorkbook.Sheets("Transfers")

    postST = "{" & _
        """FromLocation"":""" & ws.Cells(1, 2).Value & """," & _
        """ToLocation"":""Returned Goods""," & _
        """Status"":""DRAFT""," & _
        """CompletionDate"":""" & Format(Now(), "yyyy-mm-dd") & "T00:00:00""," & _
        """RequiredByDate"":""" & Format(Now(), "yyyy-mm-dd") & "T00:00:00""," & _
        """Reference"":""STR - " & ws.Cells(1, 2).Value & """," & _
        """SkipOrder"":false," & _        
        """Lines"":[]" & _
        "}"

    Debug.Print postST
    If Not PostToDear("stockTransfer", postST) then Exit Sub
    ThisWorkbook.Queries("StockTransferList").Refresh
End Sub

' Post Transfer
Sub PostSupply()
    Dim ws As Worksheet
    Dim table As ListObject
    Dim lastRow As Long
    Dim i As Long
    Dim stData As String
    Dim stLines As String
    Dim firstEntry1 As Boolean
    Dim expiryDate As String

    Set ws = ThisWorkbook.Sheets("Transfers")
    Set table = ws.ListObjects("StockTransfer")
    lastRow = table.ListRows.Count

    stLines = ""
    firstEntry1 = true
   
    For i = 1 to lastRow
        If Trim(table.ListColumns("Exclude").DataBodyRange(i).Value) <> "yes" Then
            If Not firstEntry1 Then
                stLines = stLines & ","
            End If

            stLines = stLines & "{" & _
                """SKU"":""" & table.ListColumns("SKU").DataBodyRange(i).Value & """," & _
                """TransferQuantity"":" & table.ListColumns("Qty").DataBodyRange(i).Value & _
                "}"

            firstEntry1 = False
        End If
    Next i

    stData = "{" & _
        """TaskID"":""" & ws.Cells(7, 1).Value & """," & _
        """Status"":""AUTHORISED""," & _
        """Lines"":[" & stLines & "]" & _
        "}"

    Debug.Print stData
    If Not PostToDear("stockTransfer/order", stData) Then Exit Sub

    MsgBox "Stock Transfer posted to Dear.", vbInformation
End Sub

' Post Return
Sub PostReturn()
    Dim ws As Worksheet
    Dim table As ListObject
    Dim lastRow As Long
    Dim i As Long
    Dim stData As String
    Dim stLines As String
    Dim firstEntry1 As Boolean
    Dim expiryDate As String

    Set ws = ThisWorkbook.Sheets("Transfers")
    Set table = ws.ListObjects("StockReturn")
    lastRow = table.ListRows.Count

    stLines = ""
    firstEntry1 = true
   
    For i = 1 to lastRow
        If Trim(table.ListColumns("Exclude").DataBodyRange(i).Value) <> "yes" Then
            If Not firstEntry1 Then
                stLines = stLines & ","
            End If

            stLines = stLines & "{" & _
                """SKU"":""" & table.ListColumns("SKU").DataBodyRange(i).Value & """," & _
                """TransferQuantity"":" & table.ListColumns("Qty").DataBodyRange(i).Value & _
                "}"

            firstEntry1 = False
        End If
    Next i

    stData = "{" & _
        """TaskID"":""" & ws.Cells(7, 6).Value & """," & _
        """Status"":""AUTHORISED""," & _
        """Lines"":[" & stLines & "]" & _
        "}"

    Debug.Print stData
    If Not PostToDear("stockTransfer/order", stData) Then Exit Sub

    MsgBox "Stock Return posted to Dear.", vbInformation
End Sub
