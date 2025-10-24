/* 
This query pulls services between July 1 2024 and June 30 2025 that were billable, completed, and billed in proper coordination-of-benefits order—Medicare (Care group) as primary, 
Xover as secondary, and Medicaid (Caid group) as tertiary. It verifies that each service had valid coverage for all three payers on the date of service, 
that a charge existed for each COB level in sequence, and that payments (from ARLedger) were recorded accordingly. 
The final output lists each client’s service with its provider, licensure, procedure details, payer names, charge dates, 
and corresponding payment amounts, effectively identifying services billed and paid under the full Medicare → Xover → Medicaid workflow.
*/

/* ========= 1) Payer groups ========= */
WITH Care AS (
    SELECT v.CoveragePlanId
    FROM (VALUES
        (3),(5),(6),(17),(19),(20),(21),(22),(32),(33),(59),
		(90),(105),(219),(259),(264),(290),(292),(293),(294),(296),(301),
        (328),(329),(330),(359),(363),(378),(379),(388),(455),(468),(469),(470),
		(471),(472),(473),(474),(475),(476),(480),(481),(482),(483),(484),(485),(486),(487),
        (488),(500),(501),(505),(507),(514),(516),(538)
    ) v(CoveragePlanId)
),
Xover AS (
    SELECT v.CoveragePlanId
    FROM (VALUES (105),(107),(206),(489),(490)) v(CoveragePlanId)
),
Caid AS (
    SELECT v.CoveragePlanId
    FROM (VALUES
        (23),(24),(25),(26),(27),(28),(29),(30),(94),(95),(96),
        (97),(98),(99),(100),(101),(102),(103),(104),(106),(108),(109),
        (110),(111),(112),(113),(114),(211),(233),(234),(235),(236),
        (237),(239),(243),(260),(267),(268),(269),(270),(271),(272),
        (273),(275),(276),(277),(280),(281),(283),(284),(286),(297),
        (303),(304),(305),(306),(307),(308),(309),(310),(311),(312),
        (313),(314),(315),(333),(334),(335),(336),(341),(347),(353),
        (354),(355),(356),(364),(365),(366),(367),(368),(375),(376),
        (381),(382),(383),(384),(385),(390),(392),(441),(443),(447),
        (450),(454),(497),(498),(499),(508),(509),(510),(511),(512),
        (513),(520),(522),(523),(524),(525),(533)
    ) v(CoveragePlanId)
),

/* ========= Services pre-filter ========= */
Svc AS (
    SELECT s.ServiceId, s.ClientId, s.DateOfService, s.ClinicianId,
           s.ProcedureCodeId, s.TotalDuration, s.Unit, s.Status, s.Charge
    FROM StreamlineQuery.dbo.Services s
    WHERE ISNULL(s.RecordDeleted, 'N') = 'N'
      AND s.DateOfService >= '2024-07-01'
      AND s.DateOfService <= '2025-06-30'
      AND s.Status = 75
      AND s.Billable = 'Y'
),

/* ========= Main select with DOS-effective COBs and enforced billing order ========= */
Final AS (
    SELECT
        s.ClientId,
        s.ServiceId,
        s.DateOfService,
        st.DisplayAs AS Provider,

        streamlinequery.dbo.ssf_GetGlobalCodeNameByID(
            (SELECT TOP (1) sld.LicenseTypeDegree
             FROM StreamlineQuery.dbo.StaffLicenseDegrees sld
             WHERE sld.StaffId = s.ClinicianId
               AND ISNULL(sld.RecordDeleted,'N')='N'
               AND sld.Billing = 'Y'
               AND (sld.EndDate >= '2023-07-01' OR sld.EndDate IS NULL)
             ORDER BY sld.EndDate DESC)
        ) AS Licensure,
		
		ch1.BillingCode,
        --s.ProcedureCodeId AS ProcedureCode,
        --pc.DisplayAs      AS ProcedureName,   -- << from ProcedureCodes
        s.TotalDuration   AS Duration,
        s.Unit            AS Units,

        /* ===== COB1 (Care) ===== */
        cp1.DisplayAs      AS Payer1,
        ch1.ChargeId       AS ChargeId1,
        ch1.CreatedDate    AS ChargeDate1,
        CASE WHEN pay1.PaymentAmt IS NOT NULL THEN 'Payment' END AS Type1,
        pay1.PaymentAmt    AS Amount1,

        /* ===== COB2 (Xover) ===== */
        cp2.DisplayAs      AS Payer2,
        ch2.ChargeId       AS ChargeId2,
        ch2.CreatedDate    AS ChargeDate2,
        CASE WHEN pay2.PaymentAmt IS NOT NULL THEN 'Payment' END AS Type2,
        pay2.PaymentAmt    AS Amount2,

        /* ===== COB3 (Caid) ===== */
        cp3.DisplayAs      AS Payer3,
        ch3.ChargeId       AS ChargeId3,
        ch3.CreatedDate    AS ChargeDate3,
        CASE WHEN adj3.AdjAmt IS NOT NULL THEN 'Payment' END AS Type3,  -- per your note
        adj3.AdjAmt        AS Amount3

    FROM Svc s

    /* === DOS-effective COB1 (Care) === */
    CROSS APPLY (
        SELECT TOP (1)
               ccp.ClientCoveragePlanId,
               cp.CoveragePlanId
        FROM StreamlineQuery.dbo.ClientCoveragePlans ccp
        JOIN StreamlineQuery.dbo.ClientCoverageHistory cch
          ON cch.ClientCoveragePlanId = ccp.ClientCoveragePlanId
         AND ISNULL(cch.RecordDeleted,'N')='N'
        JOIN StreamlineQuery.dbo.CoveragePlans cp
          ON cp.CoveragePlanId = ccp.CoveragePlanId
         AND ISNULL(cp.RecordDeleted,'N')='N'
        WHERE ccp.ClientId = s.ClientId
          AND ISNULL(ccp.RecordDeleted,'N')='N'
          AND cch.COBOrder = 1
          AND cch.StartDate <= s.DateOfService
          AND (cch.EndDate IS NULL OR cch.EndDate >= s.DateOfService)
          AND EXISTS (SELECT 1 FROM Care c WHERE c.CoveragePlanId = cp.CoveragePlanId)
        ORDER BY cch.EndDate DESC, cch.StartDate DESC
    ) cob1
    JOIN StreamlineQuery.dbo.CoveragePlans cp1
      ON cp1.CoveragePlanId = cob1.CoveragePlanId
     AND ISNULL(cp1.RecordDeleted,'N')='N'

    OUTER APPLY (
        SELECT TOP (1) c.ChargeId, c.CreatedDate, c.BillingCode
        FROM StreamlineQuery.dbo.Charges c
        JOIN StreamlineQuery.dbo.ERClaimLineItemImportStaging ecl
          ON ecl.ChargeId = c.ChargeId
         AND ecl.ClientCoveragePlanId = cob1.ClientCoveragePlanId
        WHERE c.ServiceId = s.ServiceId
          AND ISNULL(c.RecordDeleted,'N')='N'
        ORDER BY c.CreatedDate ASC, c.ChargeId ASC
    ) ch1

    OUTER APPLY (
        SELECT SUM(al.Amount) AS PaymentAmt
        FROM StreamlineQuery.dbo.ARLedger al
        JOIN StreamlineQuery.dbo.GlobalCodes gc
          ON gc.GlobalCodeId = al.LedgerType
         AND gc.Category = 'ARLEDGERTYPE'
         AND gc.CodeName = 'Payment'
        WHERE al.ChargeId = ch1.ChargeId
          AND al.CoveragePlanId = cob1.CoveragePlanId
    ) pay1

    /* === DOS-effective COB2 (Xover) === */
    CROSS APPLY (
        SELECT TOP (1)
               ccp.ClientCoveragePlanId,
               cp.CoveragePlanId
        FROM StreamlineQuery.dbo.ClientCoveragePlans ccp
        JOIN StreamlineQuery.dbo.ClientCoverageHistory cch
          ON cch.ClientCoveragePlanId = ccp.ClientCoveragePlanId
         AND ISNULL(cch.RecordDeleted,'N')='N'
        JOIN StreamlineQuery.dbo.CoveragePlans cp
          ON cp.CoveragePlanId = ccp.CoveragePlanId
         AND ISNULL(cp.RecordDeleted,'N')='N'
        WHERE ccp.ClientId = s.ClientId
          AND ISNULL(ccp.RecordDeleted,'N')='N'
          AND cch.COBOrder = 2
          AND cch.StartDate <= s.DateOfService
          AND (cch.EndDate IS NULL OR cch.EndDate >= s.DateOfService)
          AND EXISTS (SELECT 1 FROM Xover x WHERE x.CoveragePlanId = cp.CoveragePlanId)
        ORDER BY cch.EndDate DESC, cch.StartDate DESC
    ) cob2
    JOIN StreamlineQuery.dbo.CoveragePlans cp2
      ON cp2.CoveragePlanId = cob2.CoveragePlanId
     AND ISNULL(cp2.RecordDeleted,'N')='N'

    OUTER APPLY (
        SELECT TOP (1) c.ChargeId, c.CreatedDate
        FROM StreamlineQuery.dbo.Charges c
        JOIN StreamlineQuery.dbo.ERClaimLineItemImportStaging ecl
          ON ecl.ChargeId = c.ChargeId
         AND ecl.ClientCoveragePlanId = cob2.ClientCoveragePlanId
        WHERE c.ServiceId = s.ServiceId
          AND ISNULL(c.RecordDeleted,'N')='N'
          AND (ch1.ChargeId IS NULL OR c.CreatedDate >= ch1.CreatedDate)
        ORDER BY c.CreatedDate ASC, c.ChargeId ASC
    ) ch2

    OUTER APPLY (
        SELECT SUM(al.Amount) AS PaymentAmt
        FROM StreamlineQuery.dbo.ARLedger al
        JOIN StreamlineQuery.dbo.GlobalCodes gc
          ON gc.GlobalCodeId = al.LedgerType
         AND gc.Category = 'ARLEDGERTYPE'
         AND gc.CodeName = 'Payment'
        WHERE al.ChargeId = ch2.ChargeId
          AND al.CoveragePlanId = cob2.CoveragePlanId
    ) pay2

    /* === DOS-effective COB3 (Caid) === */
    CROSS APPLY (
        SELECT
               ccp.ClientCoveragePlanId,
               cp.CoveragePlanId
        FROM StreamlineQuery.dbo.ClientCoveragePlans ccp
        JOIN StreamlineQuery.dbo.ClientCoverageHistory cch
          ON cch.ClientCoveragePlanId = ccp.ClientCoveragePlanId
         AND ISNULL(cch.RecordDeleted,'N')='N'
        JOIN StreamlineQuery.dbo.CoveragePlans cp
          ON cp.CoveragePlanId = ccp.CoveragePlanId
         AND ISNULL(cp.RecordDeleted,'N')='N'
        WHERE ccp.ClientId = s.ClientId
          AND ISNULL(ccp.RecordDeleted,'N')='N'
          AND cch.COBOrder = 3
          AND cch.StartDate <= s.DateOfService
          AND (cch.EndDate IS NULL OR cch.EndDate >= s.DateOfService)
          AND EXISTS (SELECT 1 FROM Caid c WHERE c.CoveragePlanId = cp.CoveragePlanId)
    ) cob3
    JOIN StreamlineQuery.dbo.CoveragePlans cp3
      ON cp3.CoveragePlanId = cob3.CoveragePlanId
     AND ISNULL(cp3.RecordDeleted,'N')='N'

    OUTER APPLY (
        SELECT DISTINCT c.ChargeId, c.CreatedDate
        FROM StreamlineQuery.dbo.Charges c
        JOIN StreamlineQuery.dbo.ERClaimLineItemImportStaging ecl
          ON ecl.ChargeId = c.ChargeId
         AND ecl.ClientCoveragePlanId = cob3.ClientCoveragePlanId
        WHERE c.ServiceId = s.ServiceId
          AND ISNULL(c.RecordDeleted,'N')='N'
          AND (ch2.ChargeId IS NULL OR c.CreatedDate >= ch2.CreatedDate)
    ) ch3

    OUTER APPLY (
        SELECT SUM(al.Amount) AS AdjAmt
        FROM StreamlineQuery.dbo.ARLedger al
        JOIN StreamlineQuery.dbo.GlobalCodes gc
          ON gc.GlobalCodeId = al.LedgerType
         AND gc.Category = 'ARLEDGERTYPE'
         AND gc.CodeName = 'Payment'   
        WHERE al.ChargeId = ch3.ChargeId
          AND al.CoveragePlanId = cob3.CoveragePlanId
    ) adj3

    JOIN StreamlineQuery.dbo.Staff st
      ON st.StaffId = s.ClinicianId
     AND ISNULL(st.RecordDeleted,'N')='N'

    LEFT JOIN StreamlineQuery.dbo.ProcedureCodes pc
      ON pc.ProcedureCodeId = s.ProcedureCodeId
     AND ISNULL(pc.RecordDeleted,'N')='N'

    WHERE ch1.ChargeId IS NOT NULL
      AND ch2.ChargeId IS NOT NULL
      AND ch3.ChargeId IS NOT NULL
      AND ch2.CreatedDate >= ch1.CreatedDate
      AND ch3.CreatedDate >= ch2.CreatedDate
)

SELECT *
FROM Final
ORDER BY ClientId, ServiceId, DateOfService;


