-- Creates a view which can be used as an intermediary table to join Invoices and COGS data.
-- Cin7 Core data model does not have a direct relationship between Invoices and COGS.
-- Prior to migrating the data to Azure SQL DB, I had a working solution using Power Query.

CREATE VIEW InvCogs_Link AS

    WITH BaseTable AS (
        SELECT DISTINCT ID AS SaleID, TaskID AS InvTaskID, InvoiceNumber FROM SaleInvoices
        WHERE (Status = 'AUTHORISED' OR Status = 'Paid')
        AND ProductID <> '00000000-0000-0000-0000-000000000000'    
    ),

    InvTable AS (
        SELECT ID, InvoiceNumber, TRIM(value) AS LinkedFulfillmentNumber FROM SaleInvoices
        CROSS APPLY STRING_SPLIT(ISNULL(LinkedFulfillmentNumber, ''), ',')
        WHERE (Status = 'AUTHORISED' OR Status = 'Paid')
        AND ProductID <> '00000000-0000-0000-0000-000000000000'
        AND LinkedFulfillmentNumber LIKE '%,%'
        UNION
        SELECT ID, InvoiceNumber, LinkedFulfillmentNumber FROM SaleInvoices
        WHERE (Status = 'AUTHORISED' OR Status = 'Paid')
        AND ProductID <> '00000000-0000-0000-0000-000000000000'
        AND (LinkedFulfillmentNumber NOT LIKE '%,%' OR LinkedFulfillmentNumber IS NULL)
    ),

    FulTable AS (
        SELECT SaleFulfilments.ID, SaleFulfilments.TaskID, FulfillmentNumber, TRIM(value) AS LinkedInvoiceNumber FROM SaleFulfilments
        CROSS APPLY STRING_SPLIT(ISNULL(LinkedInvoiceNumber, ''), ',')
        JOIN SaleCogs ON SaleFulfilments.TaskID = SaleCogs.TaskID
        WHERE LinkedInvoiceNumber LIKE '%,%'
        UNION
        SELECT SaleFulfilments.ID, SaleFulfilments.TaskID, FulfillmentNumber, LinkedInvoiceNumber FROM SaleFulfilments
        JOIN SaleCogs ON SaleFulfilments.TaskID = SaleCogs.TaskID
        WHERE (LinkedInvoiceNumber NOT LIKE '%,%' OR LinkedInvoiceNumber IS NULL)
    ),

    InvCount AS (
        SELECT SaleID AS ID, COUNT(DISTINCT InvoiceNumber) AS InvCount FROM BaseTable
        GROUP BY SaleID
    ),

    FulCount AS (
        SELECT ID, COUNT(DISTINCT TaskID) AS FulCount FROM SaleCogs
        GROUP BY ID
    ),

    InvQty AS (
        SELECT InvoiceNumber, SUM(Quantity) AS InvQty FROM SaleInvoices
        WHERE (Status = 'AUTHORISED' OR Status = 'Paid')
        AND ProductID <> '00000000-0000-0000-0000-000000000000'
        GROUP BY InvoiceNumber
    ),

    FulQty AS (
        SELECT SaleFulfilments.TaskID AS TaskID, SUM(Quantity) AS FulQty FROM SaleFulfilments
        GROUP BY SaleFulfilments.TaskID
    ),

    InvLinkTable AS (
        SELECT DISTINCT InvoiceNumber, TaskID FROM InvTable
        JOIN FulTable ON InvoiceNumber = LinkedInvoiceNumber
    ),

    FulLinkTable AS (
        SELECT DISTINCT InvoiceNumber, TaskID FROM InvTable
        JOIN FulTable ON LinkedFulfillmentNumber = FulfillmentNumber and InvTable.ID = FulTable.ID
    ),

    FulSum2 AS (
        SELECT InvoiceNumber, SUM(FulQty.FulQty) AS FulSum FROM InvLinkTable
        JOIN FulQty ON InvLinkTable.TaskID = FulQty.TaskID
        GROUP BY InvoiceNumber
    ),

    FulSum3 AS (
        SELECT InvoiceNumber, SUM(FulQty.FulQty) AS FulSum FROM FulLinkTable
        JOIN FulQty ON FulLinkTable.TaskID = FulQty.TaskID
        GROUP BY InvoiceNumber
    ),

    A_SaleIDLink AS (
        SELECT SaleID, InvTaskID, InvoiceNumber, '' AS CreditNoteNumber, SaleCogs.TaskID AS CogsTaskID FROM BaseTable
        JOIN SaleCogs ON SaleID = SaleCogs.ID
        JOIN InvCount ON SaleID = InvCount.ID
        JOIN FulCount ON SaleID = FulCount.ID
        WHERE (InvCount.InvCount = 1 OR FulCount.FulCount = 1)
    ),

    B_InvLink AS (
        SELECT SaleID, InvTaskID, BaseTable.InvoiceNumber AS InvoiceNumber, '' AS CreditNoteNumber, InvLinkTable.TaskID AS CogsTaskID FROM BaseTable
        JOIN InvLinkTable ON BaseTable.InvoiceNumber = InvLinkTable.InvoiceNumber
        JOIN InvQty ON BaseTable.InvoiceNumber = InvQty.InvoiceNumber
        JOIN FulSum2 ON BaseTable.InvoiceNumber = FulSum2.InvoiceNumber
        WHERE InvQty.InvQty = FulSum2.FulSum
    ),

    C_FulLink AS (
        SELECT SaleID, InvTaskID, BaseTable.InvoiceNumber AS InvoiceNumber, '' AS CreditNoteNumber, FulLinkTable.TaskID AS CogsTaskID FROM BaseTable
        JOIN FulLinkTable ON BaseTable.InvoiceNumber = FulLinkTable.InvoiceNumber
        JOIN InvQty ON BaseTable.InvoiceNumber = InvQty.InvoiceNumber
        JOIN FulSum3 ON BaseTable.InvoiceNumber = FulSum3.InvoiceNumber
        WHERE InvQty.InvQty = FulSum3.FulSum
    ),

    D_CreditNotes AS (
        SELECT SaleCN_Link.SaleID AS SaleID, TaskID AS InvTaskID, CreditNoteInvoiceNumber AS InvoiceNumber, CreditNoteNumber, TaskId AS CogsTaskID FROM SaleCreditNotes
		JOIN SaleCN_Link ON SaleCreditNotes.SaleID = SaleCN_Link.CNSaleID
        WHERE (Status = 'AUTHORISED')
        AND ProductID <> '00000000-0000-0000-0000-000000000000'
    ),

    Appended AS (
        SELECT * FROM A_SaleIDLink
        UNION
        SELECT * FROM B_InvLink
        UNION
        SELECT * FROM C_FulLink
        UNION
        SELECT * FROM D_CreditNotes
    )

    SELECT * FROM Appended