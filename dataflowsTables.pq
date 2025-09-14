// M (Power Query) code usable in Power BI dataflows, Power BI desktop and Excel. Provides live data in Excel.
// Retrieves data from Cin7 Core (Dear Inventory) API, with pagination and rate limit handling. 

let
    apiUrl = "https://inventory.dearsystems.com/ExternalApi/v2/",
    endpoint = "product",
    headers = [
    #"Content-Type" = "application/json",
    #"api-auth-accountid" = "",
    #"api-auth-applicationkey" = ""
    ],

    GetPages = (delay) =>
    let
        call = Function.InvokeAfter(() => Web.Contents(apiUrl & endpoint, [Query = [Page = "1", Limit = "1"], Headers = headers, ManualStatusHandling = {503}]), #duration(0, 0, 0, delay)),
        status = try Value.Metadata(call)[Response.Status] otherwise 200,
        content = try Value.Metadata(call)[Content.Type] otherwise "unknown",
        result = if status = 503 or content <> "application/json" then @GetPages(10) else Json.Document(call)
    in
        result,
    
    Record = GetPages(0.5),
    lastpage = Number.RoundUp(Record[Total]/1000),
    pages = {1..lastpage},
    
    Get = (page, delay) =>
    let
        call = Function.InvokeAfter(() => Web.Contents(apiUrl & endpoint, [Query = [Page = Text.From(page), Limit = "1000"], Headers = headers, ManualStatusHandling = {503}]), #duration(0, 0, 0, delay)),
        status = try Value.Metadata(call)[Response.Status] otherwise 200,
        content = try Value.Metadata(call)[Content.Type] otherwise "unknown",
        result = if status = 503 or content <> "application/json" then @Get(page, 10) else Json.Document(call)
    in
        result,

    Source = Table.FromRecords(List.Combine(List.Transform(pages, each Get(_, 0.5)[Products])))
in
    Source