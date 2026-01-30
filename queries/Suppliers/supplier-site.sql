DECLARE @CHF_RATE DECIMAL(18,9) = 0.000210699;

-- CTE para proveedores válidos (los que pasaron los criterios del header)
;WITH VALID_LOCS AS (
    SELECT lt.OriCod, lt.CiaCod, lt.LocCod
    FROM LocTab lt
    WHERE lt.LocEst = '1'
    UNION
    SELECT lt.OriCod, lt.CiaCod, lt.LocCod
    FROM LocTab lt
    WHERE lt.LocEst = '0'
      AND EXISTS (
          SELECT 1
          FROM DocCab dc
          WHERE dc.OriCod = lt.OriCod
            AND dc.CiaCod = lt.CiaCod
            AND dc.LocCod = lt.LocCod
            AND dc.DocSld > 0
            AND dc.DocTipCod IN ('C2','DE','FD','FE','FP','LG','NO','RG','M2','N2','NA','NP')
            AND dc.DocEst <> '0'
      )
),

-- Proveedores base válidos
BASE_SUPPLIERS AS (
    SELECT 
        ct.CiaIdeNum,
        ct.OriCod,
        ct.CiaCod
    FROM CiaTab ct WITH (NOLOCK)
    INNER JOIN Cid b ON b.OriCod = ct.OriCod AND b.CiaCod = ct.CiaCod
    WHERE ct.OriCod IN ('011','F490401','F490411','F491201','F494101','F494151')
      AND ct.CiaEst = '1'
      AND b.IndGrp = '6'
      AND b.IndCod = '2'
      AND ct.CiaCod NOT IN ('0000000012','0000000011','F491201','F490411','F490411','F490401','F490421','2000022410','2000103488')
      AND ct.CiaIdeNum NOT LIKE 'F%'
),

-- Open Balance agregado por CiaIdeNum (para criterio de prioridad)
AGGREGATED_OB AS (
    SELECT 
        ct.CiaIdeNum,
        SUM(CASE
                WHEN dc.DocSld > 0 THEN
                    CASE
                        WHEN dc.DocTipCod IN ('C2','DE','FD','FE','FP','LG','NO','RG') THEN  dc.DocSld * @CHF_RATE
                        WHEN dc.DocTipCod IN ('M2','N2','NA','NP')                     THEN -dc.DocSld * @CHF_RATE
                        ELSE 0
                    END
                ELSE 0
            END) AS OPEN_BALANCE
    FROM CiaTab ct WITH (NOLOCK)
    INNER JOIN Cid b ON b.OriCod = ct.OriCod AND b.CiaCod = ct.CiaCod
    INNER JOIN DocCab dc WITH (NOLOCK) ON dc.CiaCod = ct.CiaCod AND dc.OriCod = ct.OriCod
    INNER JOIN VALID_LOCS v ON v.OriCod = dc.OriCod AND v.CiaCod = dc.CiaCod AND v.LocCod = dc.LocCod
    WHERE ct.OriCod IN ('011','F490401','F490411','F491201','F494101','F494151')
      AND ct.CiaEst = '1'
      AND b.IndGrp = '6'
      AND b.IndCod = '2'
      AND ct.CiaCod NOT IN ('0000000012','0000000011','F491201','F490411','F490411','F490401','F490421','2000022410','2000103488')
      AND ct.CiaIdeNum NOT LIKE 'F%'
      AND dc.DocTipCod IN ('C2','DE','FD','FE','FP','LG','NO','RG','M2','N2','NA','NP')
      AND dc.DocEst <> '0'
    GROUP BY ct.CiaIdeNum
),

-- Conteo de documentos 2Y agregado por CiaIdeNum
AGGREGATED_DOC_COUNT AS (
    SELECT 
        ct.CiaIdeNum,
        COUNT(*) AS DOC_COUNT_2Y
    FROM CiaTab ct WITH (NOLOCK)
    INNER JOIN Cid b ON b.OriCod = ct.OriCod AND b.CiaCod = ct.CiaCod
    INNER JOIN DocCab d WITH (NOLOCK) ON d.CiaCod = ct.CiaCod AND d.OriCod = ct.OriCod
    INNER JOIN VALID_LOCS v ON v.OriCod = d.OriCod AND v.CiaCod = d.CiaCod AND v.LocCod = d.LocCod
    WHERE ct.OriCod IN ('011','F490401','F490411','F491201','F494101','F494151')
      AND ct.CiaEst = '1'
      AND b.IndGrp = '6'
      AND b.IndCod = '2'
      AND ct.CiaCod NOT IN ('0000000012','0000000011','F491201','F490411','F490411','F490401','F490421','2000022410','2000103488')
      AND ct.CiaIdeNum NOT LIKE 'F%'
      AND d.DocTipCod IN ('FE','FD','DE')
      AND d.DocEst <> '0'
      AND d.DocFecCre >= DATEADD(YEAR, -2, GETDATE())
    GROUP BY ct.CiaIdeNum
),

-- PO 2Y existentes por CiaIdeNum (CON DocSld > 0)
AGGREGATED_PO_2Y_EXISTS AS (
    SELECT DISTINCT ct.CiaIdeNum
    FROM CiaTab ct WITH (NOLOCK)
    INNER JOIN Cid b ON b.OriCod = ct.OriCod AND b.CiaCod = ct.CiaCod
    INNER JOIN DocCab oc WITH (NOLOCK) ON oc.CiaCod = ct.CiaCod AND oc.OriCod = ct.OriCod
    INNER JOIN VALID_LOCS v ON v.OriCod = oc.OriCod AND v.CiaCod = oc.CiaCod AND v.LocCod = oc.LocCod
    WHERE ct.OriCod IN ('011','F490401','F490411','F491201','F494101','F494151')
      AND ct.CiaEst = '1'
      AND b.IndGrp = '6'
      AND b.IndCod = '2'
      AND ct.CiaCod NOT IN ('0000000012','0000000011','F491201','F490411','F490411','F490401','F490421','2000022410','2000103488')
      AND ct.CiaIdeNum NOT LIKE 'F%'
      AND oc.DocFecCre >= DATEADD(YEAR, -2, GETDATE())
      AND oc.DocTipCod IN ('OC', 'OS')
      AND oc.DocEst <> '0'
      AND oc.DocSld > 0  -- Solo OC/OS abiertas
),

-- TRX 2Y existentes por CiaIdeNum
AGGREGATED_TRX_2Y_EXISTS AS (
    SELECT DISTINCT ct.CiaIdeNum
    FROM CiaTab ct WITH (NOLOCK)
    INNER JOIN Cid b ON b.OriCod = ct.OriCod AND b.CiaCod = ct.CiaCod
    INNER JOIN DocCab dc WITH (NOLOCK) ON dc.CiaCod = ct.CiaCod AND dc.OriCod = ct.OriCod
    INNER JOIN VALID_LOCS v ON v.OriCod = dc.OriCod AND v.CiaCod = dc.CiaCod AND v.LocCod = dc.LocCod
    WHERE ct.OriCod IN ('011','F490401','F490411','F491201','F494101','F494151')
      AND ct.CiaEst = '1'
      AND b.IndGrp = '6'
      AND b.IndCod = '2'
      AND ct.CiaCod NOT IN ('0000000012','0000000011','F491201','F490411','F490411','F490401','F490421','2000022410','2000103488')
      AND ct.CiaIdeNum NOT LIKE 'F%'
      AND dc.DocFecCre >= DATEADD(YEAR, -2, GETDATE())
      AND dc.DocTipCod IN ('AP', 'C2', 'FE', 'FP', 'NP', 'NA', 'DE', 'NO')
      AND dc.DocEst <> '0'
),

-- Proveedores excluidos por documentos problemáticos
EXCLUDED_SUPPLIERS AS (
    SELECT DISTINCT ct.CiaIdeNum
    FROM CiaTab ct WITH (NOLOCK)
    INNER JOIN Cid b ON b.OriCod = ct.OriCod AND b.CiaCod = ct.CiaCod
    INNER JOIN DocCab d WITH (NOLOCK) ON d.CiaCod = ct.CiaCod AND d.OriCod = ct.OriCod
    INNER JOIN VALID_LOCS v ON v.OriCod = d.OriCod AND v.CiaCod = d.CiaCod AND v.LocCod = d.LocCod
    WHERE ct.OriCod IN ('011','F490401','F490411','F491201','F494101','F494151')
      AND ct.CiaEst = '1'
      AND b.IndGrp = '6'
      AND b.IndCod = '2'
      AND ct.CiaCod NOT IN ('0000000012','0000000011','F491201','F490411','F490411','F490401','F490421','2000022410','2000103488')
      AND ct.CiaIdeNum NOT LIKE 'F%'
      AND d.DocEst <> '0'
      AND d.DocTipCod IN ('EC','ER','SB','SD')
),

-- CiaIdeNum que cumplen los criterios del header (fusionados)
VALID_HEADER_SUPPLIERS AS (
    SELECT DISTINCT bs.CiaIdeNum
    FROM BASE_SUPPLIERS bs
    LEFT JOIN AGGREGATED_OB ob ON ob.CiaIdeNum = bs.CiaIdeNum
    LEFT JOIN AGGREGATED_DOC_COUNT adc ON adc.CiaIdeNum = bs.CiaIdeNum
    WHERE (
            COALESCE(ob.OPEN_BALANCE, 0) > 0
            OR EXISTS (SELECT 1 FROM AGGREGATED_PO_2Y_EXISTS p2y WHERE p2y.CiaIdeNum = bs.CiaIdeNum)
            OR (
                COALESCE(adc.DOC_COUNT_2Y, 0) > 1
                AND EXISTS (SELECT 1 FROM AGGREGATED_TRX_2Y_EXISTS t2y WHERE t2y.CiaIdeNum = bs.CiaIdeNum)
            )
          )
      AND NOT EXISTS (SELECT 1 FROM EXCLUDED_SUPPLIERS ex WHERE ex.CiaIdeNum = bs.CiaIdeNum)
)

-- Query principal: Sites individuales que aportaron métricas
SELECT
    CASE
    	WHEN ot.oricod = '011' then 'F490101'
    	ELSE ot.oricod
	END [ORG_NAME (M)],
    '' AS [ORGANIZATION_ID (M)],
    '' AS [BUSINESS_GROUP_ID (M)],
    ct.CiaDes AS [VENDOR_NAME (M)],
    ct.CiaIdeNum AS [VENDOR_NUM (M)],
    site_codes.VENDOR_SITE_CODE AS [VENDOR_SITE_CODE (M)],
    ct.CiaFehCre AS [VENDOR_CREATION_DATE (M)],
    '' AS [VENDOR_SITE_CREATION_DATE (M)],
    '' AS [ADDRESS_STYLE (M)],
    '' AS [LANGUAGE],
    pvt.PvnDes AS [PROVINCE],
    pt.PaiDes AS [COUNTRY (M)],
    '' AS [AREA_CODE],
    phone.TelNum AS [PHONE],
    email.TelNum AS [EMAIL_ADDRESS],
    '' AS [CUSTOMER_NUM],
    '' AS [VENDOR_SITE_CODE_ALT],
    lt.LocDir AS [ADDRESS_LINE1 (M)],
    '' AS [ADDRESS_LINE2],
    '' AS [ADDRESS_LINE3],
    '' AS [ADDRESS_LINES_ALT],
    dt.DstDes AS [CITY],
    dpt.DptDes AS [STATE],
    dt.DstPstCod AS [ZIP],
    '' AS [ADDRESS_LINE4],
    '' AS [VAT_REGISTRATION_NUM],
    '' AS [VAT_CODE],
    ot.PaiCod AS [DEFAULT_REP_COUNTRY_CODE],
    ct.CiaIdeNum AS [DEFAULT_REP_REG_NUMBER],
    '' AS [DEFAULT_REP_TAX_REG_TYPE],
    '' AS [PARTY_SITE_ID (M)],
    '' AS [PARTY_ID (M)],
    ct.CiaCod AS [VENDOR_ID (M)],
    CONCAT(RTRIM(lt.CiaCod), lt.LocCod) AS [VENDOR_SITE_ID (M)],
    lt.LocCod AS [LOCATION_ID (M)],
    CONCAT(RTRIM(lt.CiaCod), lt.LocCod) AS [VENDOR_SITE_ID (M)],
    '' AS [ADDRESS_NAME (M)],
    '' AS [ADDRESSEE],
    COALESCE(m.TRX_1Y_AMOUNT_CHF, 0) AS [TRX_1Y_AMOUNT_CHF (M)],
    COALESCE(m.TRX_2Y_AMOUNT_CHF, 0) AS [TRX_2Y_AMOUNT_CHF (M)],
    COALESCE(m.TRX_OP_BAL_CHF, 0) AS [TRX_OP_BAL_CHF (M)],
    COALESCE(m.TRX_1Y_COUNT, 0) AS [TRX_1Y_COUNT (M)],
    COALESCE(m.TRX_2Y_COUNT, 0) AS [TRX_2Y_COUNT (M)],
    COALESCE(m.TRX_OP_COUNT, 0) AS [TRX_OP_COUNT (M)],
    m.MIN_TRX_DATE AS [MIN_TRX_DATE (M)],
    m.MAX_TRX_DATE AS [MAX_TRX_DATE (M)],
    m.MIN_TRX_YEAR AS [MIN_TRX_YEAR (M)],
    m.MAX_TRX_YEAR AS [MAX_TRX_YEAR (M)],
    src.TRX_SOURCE_TAG AS [SUPPLIER_TRX_SOURCE_LIST (M)],
    COALESCE(po.PO_2Y_OP_COUNT, 0) AS [PO_2Y_OP_COUNT (M)],
    COALESCE(po.PO_1Y_OP_COUNT, 0) AS [PO_1Y_OP_COUNT (M)],
    COALESCE(po.PO_OP_COUNT, 0) AS [PO_OP_COUNT (M)],
    po.MIN_PO_DATE AS [MIN_PO_DATE (M)],
    po.MAX_PO_DATE AS [MAX_PO_DATE (M)],
    COALESCE(po.PO_Agreement_COUNT, 0) AS [PO_Agreement_COUNT (M)],
    site_codes.PO_Usage AS [PO_Usage]
FROM CiaTab ct WITH (NOLOCK)
-- Solo proveedores cuyo CiaIdeNum está en el header fusionado
INNER JOIN VALID_HEADER_SUPPLIERS vhs ON vhs.CiaIdeNum = ct.CiaIdeNum
INNER JOIN LocTab lt ON lt.CiaCod = ct.CiaCod AND lt.OriCod = ct.OriCod
INNER JOIN Cid b ON b.OriCod = ct.OriCod AND b.CiaCod = ct.CiaCod
OUTER APPLY (
    SELECT 
        STUFF((
            SELECT ', ' + ind2.IndDes
            FROM LID lid2 WITH (NOLOCK)
            INNER JOIN IndTip ind2 WITH (NOLOCK) ON ind2.IndGrp = lid2.IndGrp
                AND ind2.IndCod = lid2.IndCod
                AND ind2.OriCod = lid2.OriCod
                AND ind2.IndGrp = 9
            WHERE lid2.CiaCod = lt.CiaCod
              AND lid2.LocCod = lt.LocCod
              AND lid2.OriCod = lt.OriCod
            FOR XML PATH(''), TYPE
        ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS VENDOR_SITE_CODE,
        STUFF((
            SELECT ', ' + 
                CASE
                    WHEN lid3.IndCod = 7 THEN 'Purchasing, Payment'
                    WHEN lid3.IndCod = 3 THEN 'Payment'
                    ELSE ''
                END
            FROM LID lid3 WITH (NOLOCK)
            WHERE lid3.CiaCod = lt.CiaCod
              AND lid3.LocCod = lt.LocCod
              AND lid3.OriCod = lt.OriCod
              AND lid3.IndCod IN (3, 7)
            FOR XML PATH(''), TYPE
        ).value('.', 'NVARCHAR(MAX)'), 1, 2, '') AS PO_Usage
) site_codes
LEFT JOIN IdeTip c WITH (NOLOCK) ON c.OriCod = ct.OriCod
    AND c.IdeTipCod = ct.IdeTipCod
LEFT JOIN OriTab ot ON ot.OriCod = lt.OriCod
LEFT JOIN CiaPar cp ON cp.CiaCod = lt.CiaCod AND cp.ParCod = '7941'
LEFT JOIN PaiTab pt ON pt.PaiCod = lt.PaiCod
LEFT JOIN DstTab dt ON dt.DstCod = lt.DstCod
    AND dt.PaiCod = lt.PaiCod
    AND dt.DptCod = lt.DptCod
    AND dt.PvnCod = lt.PvnCod
    AND dt.OriCod = lt.OriCod
LEFT JOIN DptTab dpt ON dpt.DptCod = lt.DptCod
    AND dpt.OriCod = lt.OriCod
    AND dpt.PaiCod = lt.PaiCod
    AND dpt.DptEst = '1'
LEFT JOIN PvnTab pvt ON pvt.PvnCod = lt.PvnCod
    AND pvt.OriCod = lt.OriCod
    AND pvt.PaiCod = lt.PaiCod
    AND pvt.DptCod = dpt.DptCod
OUTER APPLY (
    SELECT TOP (1) phone.TelNum
    FROM TelTab phone
    WHERE phone.CiaCod = lt.CiaCod
      AND phone.OriCod = ot.OriCod
      AND phone.TelEst = '1'
      AND phone.LocCod = lt.LocCod
      AND phone.TelTipCod = 1
    ORDER BY phone.TelCod ASC
) phone
OUTER APPLY (
    SELECT TOP (1) email.TelNum
    FROM TelTab email
    WHERE email.CiaCod = lt.CiaCod
      AND email.OriCod = ot.OriCod
      AND email.TelEst = '1'
      AND email.LocCod = lt.LocCod
      AND email.TelTipCod = 3
    ORDER BY email.TelCod ASC
) email
OUTER APPLY (
    SELECT TOP (1) url.TelNum
    FROM TelTab url
    WHERE url.CiaCod = lt.CiaCod
      AND url.OriCod = ot.OriCod
      AND url.TelEst = '1'
      AND url.LocCod = lt.LocCod
      AND url.TelTipCod = 4
    ORDER BY url.TelCod ASC
) url
LEFT JOIN CiaCtaTab cct ON cct.CiaCod = ct.CiaCod AND cct.Oricod = ct.OriCod
-- Métricas del site individual
OUTER APPLY (
    SELECT
        SUM(CASE
                WHEN dc.DocFecCre >= DATEADD(YEAR, -1, GETDATE()) THEN
                    CASE
                        WHEN dc.DocTipCod IN ('FE','FD','DE') THEN  dc.DocMto * @CHF_RATE
                        WHEN dc.DocTipCod IN ('NP','NA')      THEN -dc.DocMto * @CHF_RATE
                        ELSE 0
                    END
                ELSE 0
            END) AS TRX_1Y_AMOUNT_CHF,
        SUM(CASE
                WHEN dc.DocFecCre >= DATEADD(YEAR, -2, GETDATE())
                 AND dc.DocFecCre <  DATEADD(YEAR, -1, GETDATE()) THEN
                    CASE
                        WHEN dc.DocTipCod IN ('FE','FD','DE') THEN  dc.DocMto * @CHF_RATE
                        WHEN dc.DocTipCod IN ('NP','NA')      THEN -dc.DocMto * @CHF_RATE
                        ELSE 0
                    END
                ELSE 0
            END) AS TRX_2Y_AMOUNT_CHF,
        SUM(CASE
                WHEN dc.DocSld > 0 THEN
                    CASE
                        WHEN dc.DocTipCod IN ('C2','DE','FD','FE','FP','LG','NO','RG') THEN  dc.DocSld * @CHF_RATE
                        WHEN dc.DocTipCod IN ('M2','N2','NA','NP')                     THEN -dc.DocSld * @CHF_RATE
                        ELSE 0
                    END
                ELSE 0
            END) AS TRX_OP_BAL_CHF,
        SUM(CASE WHEN dc.DocFecCre >= DATEADD(YEAR, -1, GETDATE()) THEN 1 ELSE 0 END) AS TRX_1Y_COUNT,
        SUM(CASE
                WHEN dc.DocFecCre >= DATEADD(YEAR, -2, GETDATE())
                 AND dc.DocFecCre <  DATEADD(YEAR, -1, GETDATE()) THEN 1
                ELSE 0
            END) AS TRX_2Y_COUNT,
        SUM(CASE
            WHEN dc.DocSld > 0
            AND dc.DocTipCod IN ('C2','DE','FD','FE','FP','LG','NO','RG','M2','N2','NA','NP')
            THEN 1 ELSE 0
        END) AS TRX_OP_COUNT,
        MIN(dc.DocFecCre) AS MIN_TRX_DATE,
        MAX(dc.DocFecCre) AS MAX_TRX_DATE,
        MIN(YEAR(dc.DocFecCre)) AS MIN_TRX_YEAR,
        MAX(YEAR(dc.DocFecCre)) AS MAX_TRX_YEAR
    FROM DocCab dc WITH (NOLOCK)
    WHERE dc.CiaCod = lt.CiaCod
      AND dc.OriCod = lt.OriCod
      AND dc.LocCod = lt.LocCod
      AND dc.DocTipCod IN ('C2','DE','FD','FE','FP','LG','NO','RG','M2','N2','NA','NP')
      AND dc.DocEst <> '0'
) m
OUTER APPLY (
    SELECT
        SUM(CASE WHEN dc.DocTipCod IN ('OC','OS') AND dc.DocSld > 0 AND dc.DocFecCre >= DATEADD(YEAR, -2, GETDATE()) THEN 1 ELSE 0 END) AS PO_2Y_OP_COUNT,
        SUM(CASE WHEN dc.DocTipCod IN ('OC','OS') AND dc.DocSld > 0 AND dc.DocFecCre >= DATEADD(YEAR, -1, GETDATE()) THEN 1 ELSE 0 END) AS PO_1Y_OP_COUNT,
        SUM(CASE WHEN dc.DocTipCod IN ('OC','OS') AND dc.DocSld > 0 THEN 1 ELSE 0 END) AS PO_OP_COUNT,
        MIN(CASE WHEN dc.DocTipCod IN ('OC','OS') AND dc.DocSld > 0 THEN dc.DocFecCre END) AS MIN_PO_DATE,
        MAX(CASE WHEN dc.DocTipCod IN ('OC','OS') AND dc.DocSld > 0 THEN dc.DocFecCre END) AS MAX_PO_DATE,
        SUM(CASE WHEN dc.DocSld > 0 AND dc.DocTipCod IN ('CX') THEN 1 ELSE 0 END) AS PO_Agreement_COUNT
    FROM DocCab dc WITH (NOLOCK)
    WHERE dc.CiaCod = lt.CiaCod
      AND dc.OriCod = lt.OriCod
      AND dc.LocCod = lt.LocCod
      AND dc.DocEst <> '0'
) po
OUTER APPLY (
    SELECT
        CASE WHEN EXISTS (SELECT 1 FROM DocCab d WITH (NOLOCK) WHERE d.CiaCod = lt.CiaCod AND d.OriCod = lt.OriCod AND d.LocCod = lt.LocCod AND d.DocTipCod IN ('FE','FD','DE') AND d.DocEst <> '0' AND d.DocFecCre >= DATEADD(YEAR, -1, GETDATE())) THEN 1 ELSE 0 END AS HAS_INV_1Y,
        CASE WHEN EXISTS (SELECT 1 FROM DocCab d WITH (NOLOCK) WHERE d.CiaCod = lt.CiaCod AND d.OriCod = lt.OriCod AND d.LocCod = lt.LocCod AND d.DocTipCod IN ('FE','FD','DE') AND d.DocEst <> '0' AND d.DocFecCre >= DATEADD(YEAR, -2, GETDATE())) THEN 1 ELSE 0 END AS HAS_INV_2Y,
        CASE WHEN EXISTS (SELECT 1 FROM DocCab d WITH (NOLOCK) WHERE d.CiaCod = lt.CiaCod AND d.OriCod = lt.OriCod AND d.LocCod = lt.LocCod AND d.DocTipCod IN ('FE','FD','DE') AND d.DocEst <> '0' AND d.DocSld > 0 AND d.DocFecCre < DATEADD(YEAR, -2, GETDATE())) THEN 1 ELSE 0 END AS HAS_INV_OP_OLD,
        CASE WHEN EXISTS (SELECT 1 FROM DocCab d WITH (NOLOCK) WHERE d.CiaCod = lt.CiaCod AND d.OriCod = lt.OriCod AND d.LocCod = lt.LocCod AND d.DocTipCod IN ('OC','OS') AND d.DocEst <> '0' AND d.DocSld > 0 AND d.DocFecCre >= DATEADD(YEAR, -2, GETDATE())) THEN 1 ELSE 0 END AS HAS_PO_2Y
) f
OUTER APPLY (
    SELECT
        CASE
            WHEN f.HAS_INV_OP_OLD = 1 THEN 'INV_OP'
            WHEN f.HAS_INV_1Y = 1 THEN 'INV_1Y'
            WHEN f.HAS_INV_2Y = 1 THEN 'INV_2Y'
            WHEN f.HAS_PO_2Y = 1 THEN 'POH_OP'
            ELSE 'NO_TRX_AT_ALL'
        END AS TRX_SOURCE_TAG
) src
WHERE ct.OriCod IN ('011','F490401','F490411','F491201','F494101','F494151')
  AND ct.CiaEst = '1'
  AND b.IndGrp = '6'
  AND b.IndCod = '2'
  AND ct.CiaCod NOT IN ('0000000012','0000000011','F491201','F490411','F490411','F490401','F490421','2000022410','2000103488')
  AND ct.CiaIdeNum NOT LIKE 'F%'
  -- Locación válida (activa o con balance abierto)
  AND (
      lt.LocEst = '1'
      OR (lt.LocEst <> '1' AND COALESCE(m.TRX_OP_BAL_CHF, 0) <> 0)
  )
  -- FILTRO CLAVE: Solo sites que aportaron al menos una métrica (no todos 0s)
  AND (
      COALESCE(m.TRX_1Y_AMOUNT_CHF, 0) <> 0
      OR COALESCE(m.TRX_2Y_AMOUNT_CHF, 0) <> 0
      OR COALESCE(m.TRX_OP_BAL_CHF, 0) <> 0
      OR COALESCE(m.TRX_1Y_COUNT, 0) > 0
      OR COALESCE(m.TRX_2Y_COUNT, 0) > 0
      OR COALESCE(m.TRX_OP_COUNT, 0) > 0
      OR COALESCE(po.PO_1Y_OP_COUNT, 0) > 0
      OR COALESCE(po.PO_2Y_OP_COUNT, 0) > 0
      OR COALESCE(po.PO_OP_COUNT, 0) > 0
      OR COALESCE(po.PO_Agreement_COUNT, 0) > 0
  )
ORDER BY ct.CiaIdeNum ASC, ct.CiaCod ASC, lt.LocCod ASC;