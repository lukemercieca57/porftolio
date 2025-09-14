'' VBA code to post Sale Orders, Fulfilments and Stock Transfer to the Inventory Management System from Excel
'' Cut Sale Order processing time roughly from 15 to 5 minutes, and also reduced the work to be done by the dispatch team.

Function PostToDear(endpoint As String, postData As String) As Boolean
    Dim http As Object
    Dim accountID As String
    Dim apiKey As String
    Dim url As String
    Dim response As String

    accountID = ""
    apiKey = ""
    url = "https://inventory.dearsystems.com/ExternalApi/v2/"

    Set http = CreateObject("MSXML2.XMLHTTP")

    With http
        .Open "POST", url & endpoint, false
        .SetRequestHeader "Content-Type", "application/json"
        .SetRequestHeader "api-auth-accountid", accountID
        .SetRequestHeader "api-auth-applicationkey", apiKey
        .Send postData

        response = .responseText
    End With

    If http.Status <> 200 Then
        MsgBox "Error: " & http.status & " - " & response, vbCritical
        PostToDear = False
        Exit Function
    End If

    PostToDear = True
End Function

Function PutToDear(endpoint As String, postData As String) As Boolean
    Dim http As Object
    Dim accountID As String
    Dim apiKey As String
    Dim url As String
    Dim response As String

    accountID = ""
    apiKey = ""
    url = "https://inventory.dearsystems.com/ExternalApi/v2/"

    Set http = CreateObject("MSXML2.XMLHTTP")

    With http
        .Open "PUT", url & endpoint, false
        .SetRequestHeader "Content-Type", "application/json"
        .SetRequestHeader "api-auth-accountid", accountID
        .SetRequestHeader "api-auth-applicationkey", apiKey
        .Send postData

        response = .responseText
    End With

    If http.Status <> 200 Then
        MsgBox "Error: " & http.status & " - " & response, vbCritical
        PutToDear = False
        Exit Function
    End If

    PutToDear = True
End Function

' Create SO
Sub CreateSO()
    Dim ws As Worksheet
    Dim addresses As String
    Dim postData As String

    Set ws = ThisWorkbook.Sheets("SO_Process")

    addresses = """BillingAddress"": {" & _
        """Line1"":""" & ws.Cells(28,2).Value &  """," & _
        """Line2"":""" & ws.Cells(29,2).Value &  """," & _
        """City"":""" & ws.Cells(30,2).Value &  """," & _
        """State"":""" & ws.Cells(31,2).Value &  """," & _
        """Postcode"":""" & ws.Cells(32,2).Value &  """," & _
        """Country"":""" & ws.Cells(33,2).Value &  """}," & _
        """ShippingAddress"": {" & _
        """Line1"":""" & ws.Cells(36,2).Value &  """," & _
        """Line2"":""" & ws.Cells(37,2).Value &  """," & _
        """City"":""" & ws.Cells(38,2).Value &  """," & _
        """State"":""" & ws.Cells(39,2).Value &  """," & _
        """Postcode"":""" & ws.Cells(40,2).Value &  """," & _
        """Country"":""" & ws.Cells(41,2).Value &  """}" & _
        "}"

    postData = "{" & _
        """Customer"":""" & ws.Cells(2,2).Value & """," & _
        """SkipQuote"":true," & _
        """ShipBy"":""" & Format(ws.Cells(3,2).Value, "yyyy-mm-dd") & "T00:00:00""," & _
        """Location"":""" & ws.Cells(4,2).Value & """," & _
        """CustomerReference"":""" & ws.Cells(8,2).Value & """," & _
        """SalesRepresentative"":""""," & _
        """SaleType"":""Advanced""," & _
        """AdditionalAttributes"": {" & _
        """AdditionalAttribute1"":""" & UCase(ws.Cells(10,2).Value) &  """," & _
        """AdditionalAttribute2"":""" & ws.Cells(11,2).Value &  """," & _
        """AdditionalAttribute3"":""" & ws.Cells(12,2).Value &  """," & _
        """AdditionalAttribute5"":""" & ws.Cells(15,2).Value &  """," & _
        """AdditionalAttribute6"":""" & ws.Cells(14,2).Value &  """," & _
        """AdditionalAttribute7"":""" & ws.Cells(16,2).Value &  """," & _
        """AdditionalAttribute8"":""" & UCase(ws.Cells(13,2).Value) &  """}," & addresses

    If ws.Cells(19,2).Value = "" Then
        Debug.Print postData
        If Not PostToDear("sale", postData) Then Exit Sub
        ThisWorkbook.Queries("SaleList").Refresh
        ThisWorkbook.Queries("ProductAvailability").Refresh
    Else
        MsgBox "SO Exists.", vbInformation
        Exit Sub
    End If
End Sub

' Amend SO
Sub AmendSO()
    Dim ws As Worksheet
    Dim addresses As String
    Dim postData As String

    Set ws = ThisWorkbook.Sheets("SO_Process")

    postData = "{" & _
        """ID"":""" & ws.Cells(18,2).Value & """," & _
        """Customer"":""" & ws.Cells(2,2).Value & """," & _
        """ShipBy"":""" & Format(ws.Cells(3,2).Value, "yyyy-mm-dd") & "T00:00:00""," & _
        """Location"":""" & ws.Cells(4,2).Value & """," & _
        """AdditionalAttributes"": {" & _
        """AdditionalAttribute1"":""" & UCase(ws.Cells(10,2).Value) &  """," & _
        """AdditionalAttribute2"":""" & ws.Cells(11,2).Value &  """," & _
        """AdditionalAttribute3"":""" & ws.Cells(12,2).Value &  """," & _
        """AdditionalAttribute5"":""" & ws.Cells(15,2).Value &  """," & _
        """AdditionalAttribute6"":""" & ws.Cells(14,2).Value &  """," & _
        """AdditionalAttribute7"":""" & ws.Cells(16,2).Value &  """," & _
        """AdditionalAttribute8"":""" & UCase(ws.Cells(13,2).Value) &  """}" & _
        "}"

    Debug.Print postData
    If Not PutToDear("sale", postData) Then Exit Sub
    ThisWorkbook.Queries("ProductAvailability").Refresh

End Sub

' Complete SO
Sub CompleteSO()
    Dim ws As Worksheet
    Dim postFile As String
    Dim table As ListObject
    Dim lastRow As Long
    Dim i As Long
    Dim firstEntry As Boolean
    Dim postLines As String
    Dim Lines As String
    
    Set ws = ThisWorkbook.Sheets("SO_Process")
    Set table = ws.ListObjects("SO_Lines")
    lastRow = table.ListRows.Count
    Lines = ""
    firstEntry = True
    
    ' Post Lines    
    For i = 1 to lastRow
        If Trim(table.ListColumns("SKU").DataBodyRange(i).Value) <> "" Then
            If Not firstEntry Then
                Lines = Lines & ","
            End If

            Lines = Lines & "{" & _
                """ProductID"":""" & table.ListColumns("ProductID").DataBodyRange(i).Value & """," & _
                """SKU"":""" & table.ListColumns("SKU").DataBodyRange(i).Value & """," & _
                """Name"":""" & table.ListColumns("Name").DataBodyRange(i).Value & """," & _
                """Quantity"":" & table.ListColumns("Qty").DataBodyRange(i).Value & """," & _
                """Comment"":""" & table.ListColumns("Comment").DataBodyRange(i).Value & "," & _
                """Price"":" & table.ListColumns("Price").DataBodyRange(i).Value & "," & _
                """Discount"":" & table.ListColumns("Discount").DataBodyRange(i).Value & "," & _
                """Tax"":" & table.ListColumns("Tax").DataBodyRange(i).Value & "," & _
                """Total"":" & table.ListColumns("Total").DataBodyRange(i).Value & "," & _
                """TaxRule"":""" & ws.Cells(23, 2).Value & """" & _
                "}"

            firstEntry = False
        End If
    Next i  

    postLines = "{" & _
        """SaleID"":""" & ws.Cells(18, 2).Value & """," & _
        """CombineAdditionalCharges"":false," & _
        """Memo"":""""," & _
        """Status"":""AUTHORISED""," & _
        """Lines"":[" & Lines & "]" & _
        "}"

    Debug.Print postLines
    If Not PostToDear("sale/order", postLines) Then Exit Sub

    MsgBox "SO posted to Dear.", vbInformation
End Sub

' Create Fulfilment & Transfer
Sub CreateFF()
    Dim ws As Worksheet
    Dim postFF As String
    Dim postST As String

    Set ws = ThisWorkbook.Sheets("SO_Process")

    postFF = "{""SaleID"":""" & ws.Cells(18,2).Value & """}"
    If Not PostToDear("sale/fulfilment", postFF) Then Exit Sub

    postST = "{" & _
        """FromLocation"":""Finished Goods""," & _
        """ToLocation"":""" & ws.Cells(4, 2).Value & """," & _
        """Status"":""DRAFT""," & _
        """CompletionDate"":""" & Format(Now(), "yyyy-mm-dd") & "T00:00:00""," & _
        """RequiredByDate"":""" & Format(ws.Cells(3,2).Value, "yyyy-mm-dd") & "T00:00:00""," & _
        """Reference"":""" & ws.Cells(19, 2).Value & """," & _
        """SkipOrder"":false," & _        
        """Lines"":[]" & _
        "}"

    Debug.Print postST
    If Not PostToDear("stockTransfer", postST) then Exit Sub
    ThisWorkbook.RefreshAll
End Sub

' Post Fulfilment
Sub CompleteFF()
    Dim ws As Worksheet
    Dim table As ListObject
    Dim lastRow As Long
    Dim i As Long
    Dim pickData As String
    Dim packData As String
    Dim shipData As String
    Dim stData As String
    Dim pickLines As String
    Dim packLines As String
    Dim stLines As String
    Dim firstEntry1 As Boolean
    Dim firstEntry2 As Boolean
    Dim firstEntry3 As Boolean
    Dim expiryDate As String

    Set ws = ThisWorkbook.Sheets("SO_Process")
    Set table = ws.ListObjects("SO_Lines")
    lastRow = table.ListRows.Count

    pickLines = ""
    packLines = ""
    stLines = ""
    firstEntry1 = true
    firstEntry2 = true
    firstEntry3 = true

    'PICK
    For i = 1 to lastRow
        If Trim(table.ListColumns("SKU").DataBodyRange(i).Value) <> "" Then
            If Not firstEntry1 Then
                pickLines = pickLines & ","
            End If

            If Trim(table.ListColumns("ExpiryDate").DataBodyRange(i).Value) <> "" Then
                expiryDate = Format(table.ListColumns("ExpiryDate").DataBodyRange(i).Value, "yyyy-mm-dd") & "T00:00:00"
            Else
                expiryDate = ""
            End If

            pickLines = pickLines & "{" & _
                """SKU"":""" & table.ListColumns("SKU").DataBodyRange(i).Value & """," & _                
                """Location"":""" & ws.Cells(4, 2).Value & """," & _
                """Quantity"":" & table.ListColumns("Qty").DataBodyRange(i).Value & "," & _
                """BatchSN"":""" & table.ListColumns("Lot").DataBodyRange(i).Value & """," & _
                """ExpiryDate"":""" & expiryDate & """" & _
                "}"

            firstEntry1 = False
        End If
    Next i

    pickData = "{" & _
        """TaskID"":""" & ws.Cells(24, 2).Value & """," & _
        """Status"":""AUTHORISED""," & _
        """Lines"":[" & pickLines & "]" & _
        "}"

    Debug.Print pickData

    If Not PostToDear("sale/fulfilment/pick", pickData) Then Exit Sub

    'PACK
    For i = 1 to lastRow
        If Trim(table.ListColumns("SKU").DataBodyRange(i).Value) <> "" Then
            If Not firstEntry2 Then
                packLines = packLines & ","
            End If

            If Trim(table.ListColumns("ExpiryDate").DataBodyRange(i).Value) <> "" Then
                expiryDate = Format(table.ListColumns("ExpiryDate").DataBodyRange(i).Value, "yyyy-mm-dd") & "T00:00:00"
            Else
                expiryDate = ""
            End If

            packLines = packLines & "{" & _
                """SKU"":""" & table.ListColumns("SKU").DataBodyRange(i).Value & """," & _
                """Location"":""" & ws.Cells(4, 2).Value & """," & _
                """Quantity"":" & table.ListColumns("Qty").DataBodyRange(i).Value & "," & _
                """BatchSN"":""" & table.ListColumns("Lot").DataBodyRange(i).Value & """," & _
                """ExpiryDate"":""" & expiryDate & """," & _
                """Box"":""1""" & _
                "}"

            firstEntry2 = False
        End If
    Next i

    packData = "{" & _
        """TaskID"":""" & ws.Cells(24, 2).Value & """," & _
        """Status"":""AUTHORISED""," & _
        """Lines"":[" & packLines & "]" & _
        "}"

    Debug.Print packData

    If Not PostToDear("sale/fulfilment/pack", packData) Then Exit Sub

    'SHIP
    shipData = "{" & _
        """TaskID"":""" & ws.Cells(24, 2).Value & """," & _
        """Status"":""AUTHORISED""," & _
        """Lines"":[{" & _
        """ShipmentDate"":""" & Format(Now(), "yyyy-mm-dd") & "T00:00:00""," & _
        """Carrier"":""Domestic""," & _
        """Box"":""1""," & _
        """TrackingNumber"":""" & ws.Cells(20, 2).Value & """}]" & _
        "}"

    Debug.Print shipData

    If Not PostToDear("sale/fulfilment/ship", shipData) Then Exit Sub

    'Stock Transfer
    For i = 1 to lastRow
        If Trim(table.ListColumns("SKU").DataBodyRange(i).Value) <> "" And Trim(table.ListColumns("ST").DataBodyRange(i).Value) <> "1" Then
            If Not firstEntry3 Then
                stLines = stLines & ","
            End If

            stLines = stLines & "{" & _
                """ProductID"":""" & table.ListColumns("ProductID").DataBodyRange(i).Value & """," & _
                """TransferQuantity"":" & table.ListColumns("Qty").DataBodyRange(i).Value & _
                "}"

            firstEntry3 = False
        End If
    Next i

    stData = "{" & _
        """TaskID"":""" & ws.Cells(21, 2).Value & """," & _
        """Status"":""AUTHORISED""," & _
        """Lines"":[" & stLines & "]" & _
        "}"

    Debug.Print stData
    If Not PostToDear("stockTransfer/order", stData) Then Exit Sub

    MsgBox "Fulfilment posted to Dear.", vbInformation
End Sub

' Post Fulfilment - stock transfer only
Sub StockTrf()
    Dim ws As Worksheet
    Dim table As ListObject
    Dim lastRow As Long
    Dim i As Long
    Dim stData As String
    Dim stLines As String
    Dim firstEntry1 As Boolean
    Dim expiryDate As String

    Set ws = ThisWorkbook.Sheets("SO_Process")
    Set table = ws.ListObjects("SO_Lines")
    lastRow = table.ListRows.Count

    stLines = ""
    firstEntry1 = true
   
    'Stock Transfer
    For i = 1 to lastRow
        If Trim(table.ListColumns("SKU").DataBodyRange(i).Value) <> "" And Trim(table.ListColumns("ST").DataBodyRange(i).Value) <> "1" Then
            If Not firstEntry1 Then
                stLines = stLines & ","
            End If

            stLines = stLines & "{" & _
                """ProductID"":""" & table.ListColumns("ProductID").DataBodyRange(i).Value & """," & _
                """TransferQuantity"":" & table.ListColumns("Qty").DataBodyRange(i).Value & _
                "}"

            firstEntry1 = False
        End If
    Next i

    stData = "{" & _
        """TaskID"":""" & ws.Cells(21, 2).Value & """," & _
        """Status"":""AUTHORISED""," & _
        """Lines"":[" & stLines & "]" & _
        "}"

    Debug.Print stData
    If Not PostToDear("stockTransfer/order", stData) Then Exit Sub

    MsgBox "Stock Transfer posted to Dear.", vbInformation
End Sub