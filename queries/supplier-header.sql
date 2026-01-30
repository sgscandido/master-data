DECLARE @CHF_RATE DECIMAL(18,9) = 0.000210699;

-- CTE para locaciones válidas
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

-- CTE base: todos los proveedores que cumplen condiciones (antes de fusionar)
BASE_SUPPLIERS AS (
    SELECT 
        ct.CiaIdeNum,
        ct.OriCod,
        ct.CiaCod,
        ct.CiaDes,
        ct.CiaSig,
        ct.CiaFehCre,
        ct.IdeTipCod,
        ROW_NUMBER() OVER (PARTITION BY ct.CiaIdeNum ORDER BY ct.OriCod ASC) AS rn
    FROM CiaTab ct WITH (NOLOCK)
    INNER JOIN Cid b ON b.OriCod = ct.OriCod AND b.CiaCod = ct.CiaCod
    WHERE ct.OriCod IN ('011','F490401','F490411','F491201','F494101','F494151')
      AND ct.CiaEst = '1'
      AND b.IndGrp = '6'
      AND b.IndCod = '2'
      AND ct.CiaCod NOT IN ('0000000012','0000000011','F491201','F490411','F490411','F490401','F490421','2000022410','2000103488')
      AND ct.CiaIdeNum NOT LIKE 'F%'
),

-- Métricas agregadas por CiaIdeNum (fusionando todos los duplicados)
AGGREGATED_METRICS AS (
    SELECT 
        ct.CiaIdeNum,
        -- Métricas de transacciones
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
                        WHEN dc.DocTipCod IN ('M2','N2','NA','NP')      THEN -dc.DocSld * @CHF_RATE
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

-- Métricas de PO agregadas por CiaIdeNum
AGGREGATED_PO AS (
    SELECT 
        ct.CiaIdeNum,
        SUM(CASE WHEN dc.DocTipCod IN ('OC','OS') AND dc.DocSld > 0 AND dc.DocFecCre >= DATEADD(YEAR, -1, GETDATE()) THEN 1 ELSE 0 END) AS PO_1Y_OP_COUNT,
        SUM(CASE WHEN dc.DocTipCod IN ('OC','OS') AND dc.DocSld > 0 AND dc.DocFecCre >= DATEADD(YEAR, -2, GETDATE()) THEN 1 ELSE 0 END) AS PO_2Y_OP_COUNT,
        SUM(CASE WHEN dc.DocTipCod IN ('OC','OS') AND dc.DocSld > 0 THEN 1 ELSE 0 END) AS PO_OP_COUNT,
        MIN(CASE WHEN dc.DocTipCod IN ('OC','OS') AND dc.DocSld > 0 THEN dc.DocFecCre END) AS MIN_PO_DATE,
        MAX(CASE WHEN dc.DocTipCod IN ('OC','OS') AND dc.DocSld > 0 THEN dc.DocFecCre END) AS MAX_PO_DATE,
        SUM(CASE WHEN dc.DocSld > 0 AND dc.DocTipCod IN ('CX') THEN 1 ELSE 0 END) AS PO_Agreement_COUNT
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
      AND dc.DocEst <> '0'
    GROUP BY ct.CiaIdeNum
),

-- Flags de existencia agregados por CiaIdeNum (AHORA CON DocSld > 0 para OC/OS)
AGGREGATED_FLAGS AS (
    SELECT 
        ct.CiaIdeNum,
        MAX(CASE WHEN d.DocTipCod IN ('FE','FD','DE') AND d.DocFecCre >= DATEADD(YEAR, -1, GETDATE()) THEN 1 ELSE 0 END) AS HAS_INV_1Y,
        MAX(CASE WHEN d.DocTipCod IN ('FE','FD','DE') AND d.DocFecCre >= DATEADD(YEAR, -2, GETDATE()) THEN 1 ELSE 0 END) AS HAS_INV_2Y,
        MAX(CASE WHEN d.DocTipCod IN ('FE','FD','DE') AND d.DocSld > 0 AND d.DocFecCre < DATEADD(YEAR, -2, GETDATE()) THEN 1 ELSE 0 END) AS HAS_INV_OP_OLD,
        MAX(CASE WHEN d.DocTipCod IN ('OC','OS') AND d.DocSld > 0 AND d.DocFecCre >= DATEADD(YEAR, -2, GETDATE()) THEN 1 ELSE 0 END) AS HAS_PO_2Y
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
    GROUP BY ct.CiaIdeNum
),

-- Open Balance agregado por CiaIdeNum
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

-- Conteo de documentos 2Y agregado
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

-- Validar criterio de PO 2Y agregado (AHORA CON DocSld > 0)
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
      AND oc.DocSld > 0  -- AÑADIDO: Solo OC/OS abiertas
),

-- Validar criterio de transacciones 2Y agregado
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

-- Excluir los que tienen documentos problemáticos
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
)

-- Query principal: Proveedores fusionados por CiaIdeNum
SELECT
    ct.CiaDes AS [VENDOR_NAME (M)],
    ct.CiaIdeNum AS [VENDOR_NUM (M)],
    ct.CiaCod AS [VENDOR_ID (M)],
    ct.CiaSig AS [VENDOR_NAME_ALT (M)],
    cp.CiaParVal AS [SIC],
    '' AS [EMPLOYEE_ID],
    '' AS [VENDOR_TYPE_LOOKUP_CODE (M)],
    '' AS [CEO_TITLE],
    CONCAT(ctt.CttNom, ctt.CttApePat) AS [CEO_NAME],
    '' AS [PRINCIPAL_TITLE],
    '' AS [PRINCIPAL_NAME],
    ct.CiaIdeNum AS [TAX_REGISTRATION_NUM (M)],
    ct.CiaIdeNum AS [TAXPAYER_ID (M)],
    email.TelNum AS [REMITTANCE_EMAIL],
    ot.PaiCod AS [DEFAULT_REP_COUNTRY_CODE],
    ct.CiaIdeNum AS [DEFAULT_REP_REG_NUMBER],
    rt.CiaParVal AS [DEFAULT_REP_TAX_REG_TYPE],
    '' AS [PARENT_PARTY_ID],
    '' AS [PARENT_VENDOR_ID],
    ct.CiaSig AS [PARTY_ALIAS],
    url.TelNum AS [URL],
    ct.CiaCod AS [PARTY_ID (M)],
    ct.CiaIdeNum AS [REGISTRY_ID (M)],
    ct.CiaDes AS [PARTY_NAME (M)],
    it.IdeTipDes AS [PARTY_TYPE (M)],
    '' AS [TAX_NAME],
    pt.PaiDes AS [COUNTRY (M)],
    lt.LocDir AS [ADDRESS1 (M)],
    '' AS [ADDRESS2],
    '' AS [ADDRESS3],
    '' AS [ADDRESS4],
    dt.DstDes AS [CITY (M)],
    dt.DstPstCod AS [POSTAL_CODE (M)],
    dpt.DptDes AS [STATE],
    pvt.PvnDes AS [PROVINCE],
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
    ct.CiaFehCre AS [VENDOR_CREATION_DATE (M)],
    CASE
        WHEN f.HAS_INV_OP_OLD = 1 THEN 'INV_OP'
        WHEN f.HAS_INV_1Y     = 1 THEN 'INV_1Y'
        WHEN f.HAS_INV_2Y     = 1 THEN 'INV_2Y'
        WHEN f.HAS_PO_2Y      = 1 THEN 'POH_OP'
        ELSE 'NO_TRX_AT_ALL'
    END AS [SUPPLIER_TRX_SOURCE_LIST (M)],
    COALESCE(po.PO_1Y_OP_COUNT, 0) AS [PO_1Y_OP_COUNT (M)],
    COALESCE(po.PO_2Y_OP_COUNT, 0) AS [PO_2Y_OP_COUNT (M)],
    COALESCE(po.PO_OP_COUNT, 0) AS [PO_OP_COUNT (M)],
    po.MIN_PO_DATE AS [MIN_PO_DATE (M)],
    po.MAX_PO_DATE AS [MAX_PO_DATE (M)],
    COALESCE(po.PO_Agreement_COUNT, 0) AS [PO_Agreement_COUNT (M)]
FROM BASE_SUPPLIERS bs
-- Tomar datos del proveedor con menor OriCod (rn = 1)
INNER JOIN CiaTab ct WITH (NOLOCK) ON ct.CiaIdeNum = bs.CiaIdeNum 
    AND ct.OriCod = bs.OriCod 
    AND ct.CiaCod = bs.CiaCod
LEFT JOIN OriTab ot ON ot.OriCod = ct.OriCod
LEFT JOIN CiaPar cp ON cp.CiaCod = ct.CiaCod AND cp.ParCod = '7941'
CROSS APPLY (
    SELECT TOP (1) lt2.*
    FROM LocTab lt2 WITH (NOLOCK)
    WHERE lt2.CiaCod = ct.CiaCod
      AND lt2.OriCod = ct.OriCod
      AND lt2.LocEst = '1'
    ORDER BY lt2.LocCod ASC
) lt
LEFT JOIN CiaPar rt ON rt.CiaCod = ct.CiaCod AND rt.OriCod = '011' AND rt.ParCod = '140'
LEFT JOIN PaiTab pt ON pt.PaiCod = lt.PaiCod
LEFT JOIN DstTab dt ON dt.DstCod = lt.DstCod
    AND dt.PaiCod = pt.PaiCod
    AND dt.DptCod = lt.DptCod
    AND dt.PvnCod = lt.PvnCod
    AND dt.OriCod = ot.OriCod
LEFT JOIN DptTab dpt ON dpt.DptCod = lt.DptCod
    AND dpt.OriCod = ot.OriCod
    AND dpt.PaiCod = lt.PaiCod
    AND dpt.DptEst = '1'
LEFT JOIN PvnTab pvt ON pvt.PvnCod = lt.PvnCod
    AND pvt.OriCod = ot.OriCod
    AND pvt.PaiCod = lt.PaiCod
    AND pvt.DptCod = dpt.DptCod
LEFT JOIN IdeTip it ON it.IdeTipCod = ct.IdeTipCod AND it.OriCod = '011'
LEFT JOIN CttTab ctt ON ctt.CiaCod = ct.CiaCod
    AND ctt.LocCod = lt.LocCod
    AND ctt.CrgDes = 'Director Ejecutivo'
OUTER APPLY (
    SELECT TOP (1) email.TelNum
    FROM TelTab email
    WHERE email.CiaCod = ct.CiaCod
      AND email.OriCod = ot.OriCod
      AND email.TelEst = '1'
      AND email.LocCod = lt.LocCod
      AND email.TelTipCod = 3
    ORDER BY email.TelCod ASC
) email
OUTER APPLY (
    SELECT TOP (1) url.TelNum
    FROM TelTab url
    WHERE url.CiaCod = ct.CiaCod
      AND url.OriCod = ot.OriCod
      AND url.TelEst = '1'
      AND url.LocCod = lt.LocCod
      AND url.TelTipCod = 4
    ORDER BY url.TelCod ASC
) url
-- Métricas agregadas (fusionadas por CiaIdeNum)
LEFT JOIN AGGREGATED_METRICS m ON m.CiaIdeNum = bs.CiaIdeNum
LEFT JOIN AGGREGATED_PO po ON po.CiaIdeNum = bs.CiaIdeNum
LEFT JOIN AGGREGATED_FLAGS f ON f.CiaIdeNum = bs.CiaIdeNum
LEFT JOIN AGGREGATED_OB ob ON ob.CiaIdeNum = bs.CiaIdeNum
LEFT JOIN AGGREGATED_DOC_COUNT adc ON adc.CiaIdeNum = bs.CiaIdeNum
WHERE bs.rn = 1  -- Solo tomar uno por CiaIdeNum (el de menor OriCod)
  AND (
        /* PRIORIDAD 1: Open Balance > 0 */
        COALESCE(ob.OPEN_BALANCE, 0) > 0
        /* PRIORIDAD 2: Tiene OC/OS ABIERTAS en los últimos 2 años */
        OR EXISTS (SELECT 1 FROM AGGREGATED_PO_2Y_EXISTS p2y WHERE p2y.CiaIdeNum = bs.CiaIdeNum)
        /* PRIORIDAD 3: Tiene transacciones + documentos 2Y */
        OR (
            COALESCE(adc.DOC_COUNT_2Y, 0) > 1
            AND EXISTS (SELECT 1 FROM AGGREGATED_TRX_2Y_EXISTS t2y WHERE t2y.CiaIdeNum = bs.CiaIdeNum)
        )
      )
  AND NOT EXISTS (SELECT 1 FROM EXCLUDED_SUPPLIERS ex WHERE ex.CiaIdeNum = bs.CiaIdeNum)
;