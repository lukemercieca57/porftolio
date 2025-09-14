// M (Power Query) code usable in Power BI dataflows, Power BI desktop and Excel. Unable to provide live data due to Dear API and Power Query limitations.
// Retrieves record id data from Cin7 Core (Dear Inventory) API, then retrieves the records individually and saves them to a single table.
// Fetches data from the last 2 months only. It was paired with an identical query to fetch earlier data, and both ran on seperate refresh schedules.
// This was a workaround for Dear API not allowing bulk data retrieval on certain endpoints, and Power BI dataflows incremental refresh not being available to Pro licenses. 

let
    apiUrl = "https://inventory.dearsystems.com/ExternalApi/v2/",
    endpoint = "sale",
    endpointID = "SaleList",
    headers = [
    #"Content-Type" = "application/json",
    #"api-auth-accountid" = "",
    #"api-auth-applicationkey" = ""
    ],

    GetPages = (delay) =>
    let
        call = Function.InvokeAfter(() => Web.Contents(apiUrl & endpointID, [Query = [Page = "1", Limit = "1"], Headers = headers, ManualStatusHandling = {503}]), #duration(0, 0, 0, delay)),
        status = try Value.Metadata(call)[Response.Status] otherwise 200,
        content = try Value.Metadata(call)[Content.Type] otherwise "unknown",
        result = if status = 503 or content <> "application/json" then @GetPages(10) else Json.Document(call)
    in
        result,

    RecordID = GetPages(0.5),
    lastpage = Number.RoundUp(RecordID[Total]/1000),
    pages = {1..lastpage},
    StartDate = Date.AddMonths(DateTime.Date(DateTime.AddZone(DateTime.LocalNow(),10,30)), -2),

    GetID = (page, delay) =>
    let
        call = Function.InvokeAfter(() => Web.Contents(apiUrl & endpointID, [Query = [Page = Text.From(page), Limit = "1000"], Headers = headers, ManualStatusHandling = {503}]), #duration(0, 0, 0, delay)),
        status = try Value.Metadata(call)[Response.Status] otherwise 200,
        content = try Value.Metadata(call)[Content.Type] otherwise "unknown",
        result = if status = 503 or content <> "application/json" then @GetID(page, 10) else Json.Document(call)
    in
        result,

    ids = List.Transform(List.Select(List.Combine(List.Transform(pages, each GetID(_, 0.5)[SaleList])), each Date.FromText(Text.Start(_[OrderDate], 10)) >= StartDate), each _[SaleID]),
    
    Get = (id, delay) =>
    let
        call = Function.InvokeAfter(() => Web.Contents(apiUrl & endpoint, [Query = [ID = Text.From(id), CombineAdditionalCharges = "true"], Headers = headers, ManualStatusHandling = {400, 503}]), #duration(0, 0, 0, delay)),
        status = try Value.Metadata(call)[Response.Status] otherwise 200,
        result = if status = 400 then null else if status = 503 then @Get(id, 10) else Json.Document(call)
    in
        result,

    Record = (field) => if field <> null then [
    SalesID = field[ID],
    Order = field[Order],
    AdditionalAttributes = field[AdditionalAttributes]
    ] else null,

    Source = Table.FromRecords(List.Select(List.Transform(ids, each Record(Get(_, 0.5))), each _ <> null))
in
    Source