SET NOCOUNT ON;
DECLARE @CHF_RATE DECIMAL(18,9) = 0.000210699;

-- ============================================
-- PASO 1: Crear tabla temporal de locaciones válidas
-- ============================================
IF OBJECT_ID('tempdb..#VALID_LOCS') IS NOT NULL DROP TABLE #VALID_LOCS;

SELECT lt.OriCod, lt.CiaCod, lt.LocCod
INTO #VALID_LOCS
FROM LocTab lt WITH (NOLOCK)
WHERE lt.LocEst = '1'
UNION
SELECT lt.OriCod, lt.CiaCod, lt.LocCod
FROM LocTab lt WITH (NOLOCK)
WHERE lt.LocEst = '0'
  AND EXISTS (
      SELECT 1
      FROM DocCab dc WITH (NOLOCK)
      WHERE dc.OriCod = lt.OriCod
        AND dc.CiaCod = lt.CiaCod
        AND dc.LocCod = lt.LocCod
        AND dc.DocSld > 0
        AND dc.DocTipCod IN ('AB','AG','AN','FA','FV','NC')
        AND dc.DocEst <> '0'
  );

CREATE INDEX IX_VALID_LOCS ON #VALID_LOCS(OriCod, CiaCod, LocCod);

-- ============================================
-- PASO 2: Crear tabla temporal de clientes base
-- ============================================
IF OBJECT_ID('tempdb..#BASE_CUSTOMERS') IS NOT NULL DROP TABLE #BASE_CUSTOMERS;

SELECT 
    ct.CiaIdeNum,
    ct.OriCod,
    ct.CiaCod
INTO #BASE_CUSTOMERS
FROM CiaTab ct WITH (NOLOCK)
INNER JOIN Cid b WITH (NOLOCK) ON b.OriCod = ct.OriCod AND b.CiaCod = ct.CiaCod
WHERE ct.OriCod IN ('011','F490401','F490411','F491201','F494101','F494151')
  AND ct.CiaEst = '1'
  AND b.IndGrp = '6'
  AND b.IndCod = '4'
  AND ct.CiaCod NOT IN ('0000000012','0000000011','F491201','F490411','F490411','F490401','F490421')
  AND ct.CiaIdeNum NOT LIKE 'F%';

CREATE INDEX IX_BASE_CUST ON #BASE_CUSTOMERS(CiaIdeNum);
CREATE INDEX IX_BASE_CUST2 ON #BASE_CUSTOMERS(OriCod, CiaCod);

-- ============================================
-- PASO 3: Open Balance agregado por CiaIdeNum (para criterio header)
-- ============================================
IF OBJECT_ID('tempdb..#AGGREGATED_OB') IS NOT NULL DROP TABLE #AGGREGATED_OB;

SELECT 
    bc.CiaIdeNum,
    SUM(CASE
            WHEN dc.DocSld > 0 THEN
                CASE
                    WHEN dc.DocTipCod IN ('FA','FV')           THEN  dc.DocSld * @CHF_RATE
                    WHEN dc.DocTipCod IN ('AB','AG','AN','NC') THEN -dc.DocSld * @CHF_RATE
                    ELSE 0
                END
            ELSE 0
        END) AS OPEN_BALANCE,
    MAX(CASE WHEN dc.DocTipCod IN ('FA','NC','ND') AND dc.DocFecCre >= DATEADD(YEAR, -5, GETDATE()) THEN 1 ELSE 0 END) AS HAS_TRX_5Y
INTO #AGGREGATED_OB
FROM #BASE_CUSTOMERS bc
INNER JOIN DocCab dc WITH (NOLOCK) ON dc.CiaCod = bc.CiaCod AND dc.OriCod = bc.OriCod
INNER JOIN #VALID_LOCS v ON v.OriCod = dc.OriCod AND v.CiaCod = dc.CiaCod AND v.LocCod = dc.LocCod
WHERE dc.DocTipCod IN ('AB','AG','AN','FA','FV','NC','ND')
  AND dc.DocEst <> '0'
GROUP BY bc.CiaIdeNum;

CREATE INDEX IX_AGG_OB ON #AGGREGATED_OB(CiaIdeNum);

-- ============================================
-- PASO 4: OCM 5Y existentes
-- ============================================
IF OBJECT_ID('tempdb..#OCM_EXISTS') IS NOT NULL DROP TABLE #OCM_EXISTS;

SELECT DISTINCT bc.CiaIdeNum
INTO #OCM_EXISTS
FROM #BASE_CUSTOMERS bc
INNER JOIN OcmCab oc WITH (NOLOCK) ON oc.CiaCod = bc.CiaCod AND oc.OriCod = bc.OriCod
WHERE oc.OcmFehCre >= DATEADD(YEAR, -5, GETDATE())
  AND oc.OcmEst NOT IN (0, 6);

CREATE INDEX IX_OCM_EXISTS ON #OCM_EXISTS(CiaIdeNum);

-- ============================================
-- PASO 5: CiaIdeNum válidos del header
-- ============================================
IF OBJECT_ID('tempdb..#VALID_HEADER') IS NOT NULL DROP TABLE #VALID_HEADER;

SELECT DISTINCT bc.CiaIdeNum
INTO #VALID_HEADER
FROM #BASE_CUSTOMERS bc
LEFT JOIN #AGGREGATED_OB ob ON ob.CiaIdeNum = bc.CiaIdeNum
WHERE (
        COALESCE(ob.OPEN_BALANCE, 0) > 0
        OR ob.HAS_TRX_5Y = 1
        OR EXISTS (SELECT 1 FROM #OCM_EXISTS o WHERE o.CiaIdeNum = bc.CiaIdeNum)
      );

CREATE INDEX IX_VALID_HEADER ON #VALID_HEADER(CiaIdeNum);

-- ============================================
-- PASO 6: Métricas por site individual (una sola pasada de DocCab)
-- ============================================
IF OBJECT_ID('tempdb..#SITE_METRICS') IS NOT NULL DROP TABLE #SITE_METRICS;

SELECT
    dc.CiaCod,
    dc.OriCod,
    dc.LocCod,
    -- Métricas de montos
    ROUND(SUM(CASE
            WHEN dc.DocFecCre >= DATEADD(YEAR, -2, GETDATE()) THEN
                CASE
                    WHEN dc.DocTipCod IN ('FA','ND') THEN  dc.DocMto * @CHF_RATE
                    WHEN dc.DocTipCod =  'NC'        THEN -dc.DocMto * @CHF_RATE
                    ELSE 0
                END
            ELSE 0
        END) * CAST(@CHF_RATE AS DECIMAL(38,18)), 6) AS TOT_TRX_0Y2_AMOUNT_CHF,
    ROUND(SUM(CASE
            WHEN dc.DocFecCre <= DATEADD(YEAR, -2, GETDATE())
            AND  dc.DocFecCre >= DATEADD(YEAR, -5, GETDATE()) THEN
                CASE
                    WHEN dc.DocTipCod IN ('FA','ND') THEN  dc.DocMto * @CHF_RATE
                    WHEN dc.DocTipCod =  'NC'        THEN -dc.DocMto * @CHF_RATE
                    ELSE 0
                END
            ELSE 0
        END) * CAST(@CHF_RATE AS DECIMAL(38,18)), 6) AS TOT_TRX_2Y5_AMOUNT_CHF,
    ROUND(SUM(CASE
            WHEN dc.DocSld > 0 THEN
                CASE
                    WHEN dc.DocTipCod IN ('FA','ND') THEN  dc.DocSld * @CHF_RATE
                    WHEN dc.DocTipCod =  'NC'        THEN -dc.DocSld * @CHF_RATE
                    ELSE 0
                END
            ELSE 0
        END) * CAST(@CHF_RATE AS DECIMAL(38,18)), 6) AS TOT_TRX_OP_AMOUNT_CHF,
    -- Conteos
    SUM(CASE WHEN dc.DocFecCre >= DATEADD(YEAR, -2, GETDATE()) THEN 1 ELSE 0 END) AS DOC_TRX_0Y2_COUNT,
    SUM(CASE
            WHEN dc.DocFecCre <= DATEADD(YEAR, -2, GETDATE())
            AND  dc.DocFecCre >= DATEADD(YEAR, -5, GETDATE())
        THEN 1 ELSE 0 END) AS DOC_TRX_2Y5_COUNT,
    SUM(CASE
        WHEN dc.DocSld > 0
        AND dc.DocFecVct >= DATEADD(YEAR, -2, GETDATE())
        AND dc.DocTipCod IN ('AB','AG','AN','FA','FV','NC')
        THEN 1 ELSE 0
    END) AS DOC_TRX_OP_COUNT,
    -- Fechas
    MIN(dc.DocFecCre) AS MIN_TRX_DATE,
    MAX(dc.DocFecCre) AS MAX_TRX_DATE,
    MIN(YEAR(dc.DocFecCre)) AS MIN_TRX_YEAR,
    MAX(YEAR(dc.DocFecCre)) AS MAX_TRX_YEAR,
    -- Flags
    MAX(CASE WHEN dc.DocTipCod IN ('FA','ND') AND dc.DocSld > 0 THEN 1 ELSE 0 END) AS HAS_OP_TRX,
    MAX(CASE WHEN dc.DocTipCod IN ('FA','NC','ND') AND dc.DocFecCre >= DATEADD(YEAR, -2, GETDATE()) THEN 1 ELSE 0 END) AS HAS_TRX_02Y,
    MAX(CASE WHEN dc.DocTipCod IN ('FA','NC','ND') AND dc.DocFecCre >= DATEADD(YEAR, -5, GETDATE()) THEN 1 ELSE 0 END) AS HAS_TRX_5Y,
    -- Open Balance del site
    COALESCE(SUM(CASE
        WHEN dc.DocSld > 0 THEN
            CASE
                WHEN dc.DocTipCod IN ('FA','FV')           THEN  dc.DocSld * @CHF_RATE
                WHEN dc.DocTipCod IN ('AB','AG','AN','NC') THEN -dc.DocSld * @CHF_RATE
                ELSE 0
            END
        ELSE 0
    END), 0) AS SITE_OPEN_BALANCE
INTO #SITE_METRICS
FROM DocCab dc WITH (NOLOCK)
INNER JOIN #VALID_LOCS v ON v.OriCod = dc.OriCod AND v.CiaCod = dc.CiaCod AND v.LocCod = dc.LocCod
WHERE dc.DocTipCod IN ('AB','AG','AN','FA','FV','NC','ND')
  AND dc.DocEst <> '0'
GROUP BY dc.CiaCod, dc.OriCod, dc.LocCod;

CREATE INDEX IX_SITE_METRICS ON #SITE_METRICS(CiaCod, OriCod, LocCod);

-- ============================================
-- PASO 7: Query final - Sites individuales
-- ============================================
SELECT
    CASE
    	WHEN ot.oricod = '011' then 'F490101'
    	ELSE ot.oricod
	END [ORG_NAME (M)],
    ct.CiaDes AS [PARTY_NAME (M)],
    CASE
        WHEN ct.CiaEst = '1' THEN 'ACTIVO'
        WHEN ct.CiaEst = '0' THEN 'INACTIVO'
    END [PARTY_STATUS],
    cct.CiaCtaNum AS [PARTY_ID (M)],
    ct.CiaIdeNum AS [REGISTRY_ID],
    cp.CiaParVal AS [SIC_CODE],
    ot.PaiCod AS [DEFAULT_REP_COUNTRY_CODE (M)],
    ct.CiaIdeNum AS [DEFAULT_REP_REG_NUMBER (M)],
    c.ideTipDes AS [DEFAULT_REP_TAX_REG_TYPE (M)],
    ct.CiaIdeNum AS [TAXPAYER_ID (M)],
    ct.CiaIdeNum AS [TAX_REGISTRATION_NUMBER (M)],
    cct.CiaCtaNum AS [ACCOUNT_NUMBER (M)],
    lt.LocDes AS [PARTY_SITE_NAME],
    cct.CiaCtaTip AS [ACCOUNT_DESCRIPTION],
    cct.CiaCtaNum AS [CUST_ACCOUNT_ID],
    cct.CiaCtaEst AS [ACCOUNT_STATUS (M)],
    lt.LocCod AS [PARTY_SITE_NUMBER],
    CONCAT(RTRIM(lt.CiaCod), lt.LocCod) AS [PARTY_SITE_ID],
    pt.PaiDes AS [COUNTRY (M)],
    lt.LocDir AS [ADDRESS1 (M)],
    dt.DstDes AS [CITY (M)],
    dt.DstPstCod AS [POSTAL_CODE (M)],
    dpt.DptDes AS [STATE (M)],
    pvt.PvnDes AS [PROVINCE (M)],
    dt.DstDes AS [COUNTY (M)],
    lt.LocEst AS [STATUS (M)],
    phone.TelNum AS [ACCOUNT_SITE_PHONE_NUMBER],
    email.TelNum AS [ACCOUNT_SITE_EMAIL],
    COALESCE(m.TOT_TRX_0Y2_AMOUNT_CHF, 0) AS [TOT_TRX_0Y2_AMOUNT_CHF (M)],
    COALESCE(m.TOT_TRX_2Y5_AMOUNT_CHF, 0) AS [TOT_TRX_2Y5_AMOUNT_CHF (M)],
    COALESCE(m.TOT_TRX_OP_AMOUNT_CHF, 0) AS [TOT_TRX_OP_AMOUNT_CHF (M)],
    COALESCE(m.TOT_TRX_0Y2_AMOUNT_CHF, 0) + COALESCE(m.TOT_TRX_2Y5_AMOUNT_CHF, 0) + COALESCE(m.TOT_TRX_OP_AMOUNT_CHF, 0) AS [TOT_TRX_0Y5_OP_AMOUNT_CHF (M)],
    COALESCE(m.DOC_TRX_0Y2_COUNT, 0) AS [DOC_TRX_0Y2_COUNT (M)],
    COALESCE(m.DOC_TRX_2Y5_COUNT, 0) AS [DOC_TRX_2Y5_COUNT (M)],
    COALESCE(m.DOC_TRX_OP_COUNT, 0) AS [DOC_TRX_OP_COUNT (M)],
    COALESCE(m.DOC_TRX_0Y2_COUNT, 0) + COALESCE(m.DOC_TRX_2Y5_COUNT, 0) + COALESCE(m.DOC_TRX_OP_COUNT, 0) AS [DOC_TRX_0Y5_OP_COUNT (M)],
    m.MIN_TRX_DATE AS [MIN_TRX_DATE (M)],
    m.MAX_TRX_DATE AS [MAX_TRX_DATE (M)],
    m.MIN_TRX_YEAR AS [MIN_TRX_YEAR (M)],
    m.MAX_TRX_YEAR AS [MAX_TRX_YEAR (M)],
    CASE
        WHEN m.HAS_OP_TRX = 1 THEN 'OP_TRX'
        WHEN m.HAS_TRX_02Y = 1 THEN 'TRX_02Y'
        WHEN m.HAS_TRX_5Y = 1 THEN 'TRX_2Y5'
        ELSE 'PAY_SCHE_NO_TRX'
    END AS [CUSTOMER_TRX_SOURCE_LIST (M)]
FROM CiaTab ct WITH (NOLOCK)
-- Solo clientes válidos del header
INNER JOIN #VALID_HEADER vh ON vh.CiaIdeNum = ct.CiaIdeNum
INNER JOIN Cid b WITH (NOLOCK) ON b.OriCod = ct.OriCod AND ct.CiaCod = b.CiaCod
INNER JOIN IdeTip c WITH (NOLOCK) ON ct.OriCod = c.OriCod AND ct.IdeTipCod = c.IdeTipCod
INNER JOIN OriTab ot WITH (NOLOCK) ON ot.OriCod = ct.OriCod
INNER JOIN LocTab lt WITH (NOLOCK) ON lt.CiaCod = ct.CiaCod AND lt.OriCod = ct.OriCod
LEFT JOIN CiaPar cp WITH (NOLOCK) ON cp.CiaCod = ct.CiaCod AND cp.ParCod = '7941'
LEFT JOIN PaiTab pt WITH (NOLOCK) ON pt.PaiCod = lt.PaiCod
LEFT JOIN DstTab dt WITH (NOLOCK) ON dt.DstCod = lt.DstCod
    AND dt.PaiCod = lt.PaiCod
    AND dt.DptCod = lt.DptCod
    AND dt.PvnCod = lt.PvnCod
    AND dt.OriCod = lt.OriCod
LEFT JOIN DptTab dpt WITH (NOLOCK) ON dpt.DptCod = lt.DptCod
    AND dpt.OriCod = lt.OriCod
    AND dpt.PaiCod = lt.PaiCod
    AND dpt.DptEst = '1'
LEFT JOIN PvnTab pvt WITH (NOLOCK) ON pvt.PvnCod = lt.PvnCod
    AND pvt.OriCod = lt.OriCod
    AND pvt.PaiCod = lt.PaiCod
    AND pvt.DptCod = dpt.DptCod
OUTER APPLY (
    SELECT TOP (1) phone.TelNum
    FROM TelTab phone WITH (NOLOCK)
    WHERE phone.CiaCod = ct.CiaCod
      AND phone.OriCod = ot.OriCod
      AND phone.TelEst = '1'
      AND phone.LocCod = lt.LocCod
      AND phone.TelTipCod = 1
    ORDER BY phone.TelCod ASC
) phone
OUTER APPLY (
    SELECT TOP (1) email.TelNum
    FROM TelTab email WITH (NOLOCK)
    WHERE email.CiaCod = ct.CiaCod
      AND email.OriCod = ot.OriCod
      AND email.TelEst = '1'
      AND email.LocCod = lt.LocCod
      AND email.TelTipCod = 3
    ORDER BY email.TelCod ASC
) email
LEFT JOIN CiaCtaTab cct WITH (NOLOCK) ON cct.CiaCod = ct.CiaCod AND cct.Oricod = ct.OriCod
-- Métricas pre-calculadas del site
LEFT JOIN #SITE_METRICS m ON m.CiaCod = lt.CiaCod AND m.OriCod = lt.OriCod AND m.LocCod = lt.LocCod
WHERE ct.OriCod IN ('011','F490401','F490411','F491201','F494101','F494151')
  AND ct.CiaEst = '1'
  AND b.IndGrp = '6'
  AND b.IndCod = '4'
  AND ct.CiaIdeNum NOT LIKE 'F%'
  AND ct.CiaCod NOT IN ('0000000012','0000000011','F491201','F490411','F490411','F490401','F490421')
  -- Locación válida (activa o con balance abierto)
  AND (
      lt.LocEst = '1'
      OR (lt.LocEst <> '1' AND COALESCE(m.SITE_OPEN_BALANCE, 0) > 0)
  )
  -- FILTRO CLAVE: Solo sites que aportaron al menos una métrica
  AND (
      COALESCE(m.TOT_TRX_0Y2_AMOUNT_CHF, 0) <> 0
      OR COALESCE(m.TOT_TRX_2Y5_AMOUNT_CHF, 0) <> 0
      OR COALESCE(m.TOT_TRX_OP_AMOUNT_CHF, 0) <> 0
      OR COALESCE(m.DOC_TRX_0Y2_COUNT, 0) > 0
      OR COALESCE(m.DOC_TRX_2Y5_COUNT, 0) > 0
      OR COALESCE(m.DOC_TRX_OP_COUNT, 0) > 0
  )
ORDER BY ct.CiaIdeNum ASC, ct.CiaCod ASC, lt.LocCod ASC;

-- Limpieza
DROP TABLE IF EXISTS #VALID_LOCS;
DROP TABLE IF EXISTS #BASE_CUSTOMERS;
DROP TABLE IF EXISTS #AGGREGATED_OB;
DROP TABLE IF EXISTS #OCM_EXISTS;
DROP TABLE IF EXISTS #VALID_HEADER;
DROP TABLE IF EXISTS #SITE_METRICS;