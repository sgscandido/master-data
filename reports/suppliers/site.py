"""
Reporte Supplier Site
======================
Extrae datos a nivel de sitio/ubicación del proveedor.
"""
import sys
sys.path.insert(0, '../..')
from reports.base import BaseReport
from config.settings import CHF_RATE, DOC_TYPES, VALID_ORIGIN_CODES, EXCLUDED_VENDOR_CODES


class SupplierSiteReport(BaseReport):
    """
    Reporte de proveedores a nivel de sitio/ubicación.
    
    Incluye:
    - Múltiples ubicaciones por vendor
    - Información de contacto por sitio
    - Métricas de transacciones por ubicación
    - Uso de PO (Purchasing/Payment)
    """
    
    def get_report_name(self) -> str:
        return "supplier_site"
    
    def get_sheet_name(self) -> str:
        return "Supplier Site"
    
    def get_query(self) -> str:
        """Query para supplier site - basado en queries/supplier-site.sql"""
        
        # Constantes para el query
        chf_rate = CHF_RATE
        invoice_debit = "','".join(DOC_TYPES.invoice_debit)
        credit_notes = "','".join(DOC_TYPES.credit_notes)
        balance_positive = "','".join(DOC_TYPES.balance_positive)
        balance_negative = "','".join(DOC_TYPES.balance_negative)
        purchase_orders = "','".join(DOC_TYPES.purchase_orders)
        all_transactional = "','".join(DOC_TYPES.all_transactional)
        excluded_docs = "','".join(DOC_TYPES.excluded)
        valid_origins = "','".join(VALID_ORIGIN_CODES)
        excluded_vendors = "','".join(EXCLUDED_VENDOR_CODES)
        
        return f"""
DECLARE @CHF_RATE DECIMAL(18,9) = {chf_rate};
SELECT
    CASE
        WHEN ot.oricod = '011' then 'F490101'
        ELSE ot.oricod
    END [ORG_NAME (M)],
    '' AS [ORGANIZATION_ID (M)],
    '' AS [BUSINESS_GROUP_ID (M)],
    ct.CiaDes AS [VENDOR_NAME (M)],
    ct.CiaIdeNum AS [VENDOR_NUM (M)],
    ind.IndDes AS [VENDOR_SITE_CODE (M)],
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
    CONCAT(RTRIM(lt.CiaCod), lt.LocCod) AS [VENDOR_SITE_ID_2 (M)],
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
    CASE
        WHEN lid.IndCod = 7 THEN 'Purchasing, Payment'
        WHEN lid.IndCod = 3 THEN 'Payment'
        ELSE NULL
    END AS [PO_Usage]
FROM CiaTab ct WITH (NOLOCK)
INNER JOIN LocTab lt ON lt.CiaCod = ct.CiaCod
   AND lt.OriCod = ct.OriCod
LEFT JOIN LID lid WITH (NOLOCK) ON lid.CiaCod = lt.CiaCod
   AND lid.LocCod = lt.LocCod
   AND lid.OriCod = lt.OriCod
LEFT JOIN IndTip ind WITH (NOLOCK) ON ind.IndGrp = lid.IndGrp
   AND ind.IndCod = lid.IndCod
   AND ind.OriCod = lt.OriCod
   AND ind.IndGrp = 9
LEFT JOIN Cid b ON b.OriCod = ct.OriCod
   AND b.CiaCod = ct.CiaCod
LEFT JOIN IdeTip c WITH (NOLOCK) ON c.OriCod = ct.OriCod
   AND c.IdeTipCod = ct.IdeTipCod
LEFT JOIN OriTab ot ON ot.OriCod = lt.OriCod
LEFT JOIN CiaPar cp ON cp.CiaCod = lt.CiaCod
   AND cp.ParCod = '7941'
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
LEFT JOIN CiaCtaTab cct
    ON cct.CiaCod = ct.CiaCod
   AND cct.Oricod = ct.OriCod
OUTER APPLY (
    SELECT
        SUM(CASE
                WHEN dc.DocFecCre >= DATEADD(YEAR, -1, GETDATE()) THEN
                    CASE
                        WHEN dc.DocTipCod IN ('{invoice_debit}') THEN  dc.DocMto * @CHF_RATE
                        WHEN dc.DocTipCod IN ('{credit_notes}')      THEN -dc.DocMto * @CHF_RATE
                        ELSE 0
                    END
                ELSE 0
            END) AS TRX_1Y_AMOUNT_CHF,
        SUM(CASE
                WHEN dc.DocFecCre >= DATEADD(YEAR, -2, GETDATE())
                 AND dc.DocFecCre <  DATEADD(YEAR, -1, GETDATE()) THEN
                    CASE
                        WHEN dc.DocTipCod IN ('{invoice_debit}') THEN  dc.DocMto * @CHF_RATE
                        WHEN dc.DocTipCod IN ('{credit_notes}')      THEN -dc.DocMto * @CHF_RATE
                        ELSE 0
                    END
                ELSE 0
            END) AS TRX_2Y_AMOUNT_CHF,
        SUM(CASE
                WHEN dc.DocSld > 0 THEN
                    CASE
                        WHEN dc.DocTipCod IN ('{balance_positive}') THEN  dc.DocSld * @CHF_RATE
                        WHEN dc.DocTipCod IN ('{balance_negative}')                     THEN -dc.DocSld * @CHF_RATE
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
        SUM(
        CASE
            WHEN dc.DocSld > 0
            AND dc.DocTipCod IN ('{all_transactional}')
            THEN 1 ELSE 0
        END
        ) AS TRX_OP_COUNT,
        MIN(dc.DocFecCre) AS MIN_TRX_DATE,
        MAX(dc.DocFecCre) AS MAX_TRX_DATE,
        MIN(YEAR(dc.DocFecCre)) AS MIN_TRX_YEAR,
        MAX(YEAR(dc.DocFecCre)) AS MAX_TRX_YEAR
    FROM DocCab dc WITH (NOLOCK)
    WHERE dc.CiaCod = lt.CiaCod
      AND dc.OriCod = lt.OriCod
      AND dc.LocCod = lt.LocCod
      AND dc.DocTipCod IN ('{all_transactional}')
      AND dc.DocEst <> '0'
) m
OUTER APPLY (
    SELECT
        SUM(CASE WHEN dc.DocTipCod IN ('{purchase_orders}') AND dc.DocSld > 0 AND dc.DocFecCre >= DATEADD(YEAR, -2, GETDATE()) THEN 1 ELSE 0 END) AS PO_2Y_OP_COUNT,
        SUM(CASE WHEN dc.DocTipCod IN ('{purchase_orders}') AND dc.DocSld > 0 AND dc.DocFecCre >= DATEADD(YEAR, -1, GETDATE()) THEN 1 ELSE 0 END) AS PO_1Y_OP_COUNT,
        SUM(CASE WHEN dc.DocTipCod IN ('{purchase_orders}') AND dc.DocSld > 0 THEN 1 ELSE 0 END) AS PO_OP_COUNT,
        MIN(CASE WHEN dc.DocTipCod IN ('{purchase_orders}') AND dc.DocSld > 0 THEN dc.DocFecCre END) AS MIN_PO_DATE,
        MAX(CASE WHEN dc.DocTipCod IN ('{purchase_orders}') AND dc.DocSld > 0 THEN dc.DocFecCre END) AS MAX_PO_DATE,
        SUM(CASE WHEN dc.DocSld > 0 AND dc.DocTipCod IN ('CX') THEN 1 ELSE 0 END) AS PO_Agreement_COUNT
    FROM DocCab dc WITH (NOLOCK)
    WHERE dc.CiaCod = lt.CiaCod
      AND dc.OriCod = lt.OriCod
      AND dc.LocCod = lt.LocCod
      AND dc.DocEst <> '0'
) po
OUTER APPLY (
    SELECT
        CASE WHEN EXISTS (SELECT 1 FROM DocCab d WITH (NOLOCK) WHERE d.CiaCod = lt.CiaCod AND d.OriCod = lt.OriCod AND d.LocCod = lt.LocCod AND d.DocTipCod IN ('{invoice_debit}') AND d.DocEst <> '0' AND d.DocFecCre >= DATEADD(YEAR, -1, GETDATE())) THEN 1 ELSE 0 END AS HAS_INV_1Y,
        CASE WHEN EXISTS (SELECT 1 FROM DocCab d WITH (NOLOCK) WHERE d.CiaCod = lt.CiaCod AND d.OriCod = lt.OriCod AND d.LocCod = lt.LocCod AND d.DocTipCod IN ('{invoice_debit}') AND d.DocEst <> '0' AND d.DocFecCre >= DATEADD(YEAR, -2, GETDATE())) THEN 1 ELSE 0 END AS HAS_INV_2Y,
        CASE WHEN EXISTS (SELECT 1 FROM DocCab d WITH (NOLOCK) WHERE d.CiaCod = lt.CiaCod AND d.OriCod = lt.OriCod AND d.LocCod = lt.LocCod AND d.DocTipCod IN ('{invoice_debit}') AND d.DocEst <> '0' AND d.DocSld > 0 AND d.DocFecCre < DATEADD(YEAR, -2, GETDATE())) THEN 1 ELSE 0 END AS HAS_INV_OP_OLD,
        CASE WHEN EXISTS (SELECT 1 FROM DocCab d WITH (NOLOCK) WHERE d.CiaCod = lt.CiaCod AND d.OriCod = lt.OriCod AND d.LocCod = lt.LocCod AND d.DocTipCod IN ('{purchase_orders}') AND d.DocEst <> '0' AND d.DocFecCre >= DATEADD(YEAR, -2, GETDATE())) THEN 1 ELSE 0 END AS HAS_PO_2Y
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
OUTER APPLY (
    SELECT COUNT(*) AS DOC_COUNT_2Y
    FROM DocCab d WITH (NOLOCK)
    WHERE d.CiaCod = ct.CiaCod
      AND d.OriCod = ct.OriCod
      AND d.DocTipCod IN ('{invoice_debit}')
      AND d.DocEst <> '0'
      AND d.DocFecCre >= DATEADD(YEAR, -2, GETDATE())
) trans
OUTER APPLY (
    SELECT
        COALESCE(SUM(CASE
            WHEN dc.DocSld > 0 THEN
                CASE
                    WHEN dc.DocTipCod IN ('{balance_positive}') THEN  dc.DocSld * @CHF_RATE
                    WHEN dc.DocTipCod IN ('{balance_negative}')                     THEN -dc.DocSld * @CHF_RATE
                    ELSE 0
                END
            ELSE 0
        END), 0) AS OPEN_BALANCE
    FROM DocCab dc WITH (NOLOCK)
    WHERE dc.CiaCod = lt.CiaCod
      AND dc.OriCod = lt.OriCod
      AND dc.LocCod = lt.LocCod
      AND dc.DocTipCod IN ('{all_transactional}')
      AND dc.DocEst <> '0'
) priority
WHERE ct.OriCod IN ('{valid_origins}')
  AND ct.CiaEst = '1'
  AND b.IndGrp = '6'
  AND b.IndCod = '2'
  AND ct.CiaCod NOT IN ('{excluded_vendors}')
  AND ct.CiaIdeNum NOT LIKE 'F%'
  AND (
      lt.LocEst = '1'
      OR (lt.LocEst <> '1' AND priority.OPEN_BALANCE > 0)
  )
  AND (
        /* PRIORIDAD 1 */
        priority.OPEN_BALANCE > 0
        /* PRIORIDAD 2 */
        OR EXISTS (
            SELECT 1
            FROM DocCab oc WITH (NOLOCK)
            WHERE oc.CiaCod = lt.CiaCod
              AND oc.OriCod = lt.OriCod
              AND oc.LocCod = lt.LocCod
              AND oc.DocFecCre >= DATEADD(YEAR, -2, GETDATE())
              AND oc.DocTipCod IN ('{purchase_orders}')
              AND oc.DocEst <> '0'
        )
        /* PRIORIDAD 3 (aquí sí exige transacciones + documento 2y) */
        OR (
            trans.DOC_COUNT_2Y > 1
            AND EXISTS (
                SELECT 1
                FROM DocCab dc WITH (NOLOCK)
                WHERE dc.CiaCod = lt.CiaCod
                    AND dc.OriCod = lt.OriCod
                    AND dc.LocCod = lt.LocCod
                    AND dc.DocFecCre >= DATEADD(YEAR, -2, GETDATE())
                    AND dc.DocTipCod IN ('AP','C2','FE','FP','NP','NA','DE', 'NO')
                    AND dc.DocEst <> '0'
            )
        )
    )
    AND NOT EXISTS (
        SELECT 1
        FROM DocCab d WITH (NOLOCK)
        WHERE d.CiaCod = ct.CiaCod
            AND d.OriCod = ct.OriCod
            AND d.DocEst <> '0'
            AND d.DocTipCod IN ('{excluded_docs}')
    )
ORDER BY ct.CiaCod ASC;
"""
