"""
Reporte Supplier Header
========================
Extrae datos a nivel de proveedor (cabecera).
"""
import sys
sys.path.insert(0, '../..')
from reports.base import BaseReport
from config.settings import CHF_RATE, DOC_TYPES, VALID_ORIGIN_CODES, EXCLUDED_VENDOR_CODES


class SupplierHeaderReport(BaseReport):
    """
    Reporte de proveedores a nivel de cabecera.
    
    Incluye:
    - Información básica del vendor (nombre, ID, NIT)
    - Dirección principal
    - Métricas de transacciones (montos CHF, conteos 1Y/2Y)
    - Métricas de órdenes de compra
    """
    
    def get_report_name(self) -> str:
        return "supplier_header"
    
    def get_sheet_name(self) -> str:
        return "Supplier Header"
    
    def get_query(self) -> str:
        """Query para supplier header - basado en queries/supplier-header.sql"""
        
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
            AND dc.DocTipCod IN ('{all_transactional}')
            AND dc.DocEst <> '0'
      )
)
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
    src.TRX_SOURCE_TAG AS [SUPPLIER_TRX_SOURCE_LIST (M)],
    COALESCE(po.PO_1Y_OP_COUNT, 0) AS [PO_1Y_OP_COUNT (M)],
    COALESCE(po.PO_2Y_OP_COUNT, 0) AS [PO_2Y_OP_COUNT (M)],
    COALESCE(po.PO_OP_COUNT, 0) AS [PO_OP_COUNT (M)],
    po.MIN_PO_DATE AS [MIN_PO_DATE (M)],
    po.MAX_PO_DATE AS [MAX_PO_DATE (M)],
    COALESCE(po.PO_Agreement_COUNT, 0) AS [PO_Agreement_COUNT (M)]
FROM CiaTab ct WITH (NOLOCK)
LEFT JOIN Cid b ON b.OriCod = ct.OriCod
   AND b.CiaCod = ct.CiaCod
LEFT JOIN IdeTip c WITH (NOLOCK) ON c.OriCod = ct.OriCod
   AND c.IdeTipCod = ct.IdeTipCod
LEFT JOIN OriTab ot ON ot.OriCod = ct.OriCod
LEFT JOIN CiaPar cp ON cp.CiaCod = ct.CiaCod
   AND cp.ParCod = '7941'
CROSS APPLY (
    SELECT TOP (1) lt.*
    FROM LocTab lt WITH (NOLOCK)
    WHERE lt.CiaCod = ct.CiaCod
      AND lt.OriCod = ct.OriCod
      AND lt.LocEst = '1'
    ORDER BY lt.LocCod ASC
) lt
LEFT JOIN CiaPar rt ON rt.CiaCod = ct.CiaCod
   AND rt.OriCod = '011'
   AND rt.ParCod = '140'
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
LEFT JOIN IdeTip it ON it.IdeTipCod = ct.IdeTipCod
   AND it.OriCod = '011'
LEFT JOIN CttTab ctt ON ctt.CiaCod = ct.CiaCod
   AND ctt.LocCod = lt.LocCod
   AND ctt.CrgDes = 'Director Ejecutivo'
OUTER APPLY (
    SELECT TOP (1) phone.TelNum
    FROM TelTab phone
    WHERE phone.CiaCod = ct.CiaCod
      AND phone.OriCod = ot.OriCod
      AND phone.TelEst = '1'
      AND phone.LocCod = lt.LocCod
      AND phone.TelTipCod = 1
    ORDER BY phone.TelCod ASC
) phone
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
                        WHEN dc.DocTipCod IN ('{balance_negative}')      THEN -dc.DocSld * @CHF_RATE
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
    WHERE dc.CiaCod = ct.CiaCod
      AND dc.OriCod = ct.OriCod
      AND dc.LocCod IN (
        SELECT v.LocCod
        FROM VALID_LOCS v
        WHERE v.OriCod = dc.OriCod
          AND v.CiaCod = dc.CiaCod
      )
      AND dc.DocTipCod IN ('{all_transactional}')
      AND dc.DocEst <> '0'
) m
OUTER APPLY (
    SELECT
        SUM(CASE WHEN dc.DocTipCod IN ('{purchase_orders}') AND dc.DocSld > 0 AND dc.DocFecCre >= DATEADD(YEAR, -1, GETDATE()) THEN 1 ELSE 0 END) AS PO_1Y_OP_COUNT,
        SUM(CASE WHEN dc.DocTipCod IN ('{purchase_orders}') AND dc.DocSld > 0 AND dc.DocFecCre >= DATEADD(YEAR, -2, GETDATE()) THEN 1 ELSE 0 END) AS PO_2Y_OP_COUNT,
        SUM(CASE WHEN dc.DocTipCod IN ('{purchase_orders}') AND dc.DocSld > 0 THEN 1 ELSE 0 END) AS PO_OP_COUNT,
        MIN(CASE WHEN dc.DocTipCod IN ('{purchase_orders}') AND dc.DocSld > 0 THEN dc.DocFecCre END) AS MIN_PO_DATE,
        MAX(CASE WHEN dc.DocTipCod IN ('{purchase_orders}') AND dc.DocSld > 0 THEN dc.DocFecCre END) AS MAX_PO_DATE,
        SUM(CASE WHEN dc.DocSld > 0 AND dc.DocTipCod IN ('CX') THEN 1 ELSE 0 END) AS PO_Agreement_COUNT
    FROM DocCab dc WITH (NOLOCK)
    WHERE dc.CiaCod = ct.CiaCod
      AND dc.OriCod = ct.OriCod
      AND dc.DocEst <> '0'
      AND dc.LocCod IN (
        SELECT v.LocCod
        FROM VALID_LOCS v
        WHERE v.OriCod = dc.OriCod
          AND v.CiaCod = dc.CiaCod
      )
) po
OUTER APPLY (
    SELECT
        CASE WHEN EXISTS (
            SELECT 1
            FROM DocCab d WITH (NOLOCK)
            WHERE d.CiaCod = ct.CiaCod
              AND d.OriCod = ct.OriCod
              AND d.DocTipCod IN ('{invoice_debit}')
              AND d.DocEst <> '0'
              AND d.DocFecCre >= DATEADD(YEAR, -1, GETDATE())
              AND d.LocCod IN (
                SELECT v.LocCod
                FROM VALID_LOCS v
                WHERE v.OriCod = d.OriCod
                  AND v.CiaCod = d.CiaCod
              )
        ) THEN 1 ELSE 0 END AS HAS_INV_1Y,
        CASE WHEN EXISTS (
            SELECT 1
            FROM DocCab d WITH (NOLOCK)
            WHERE d.CiaCod = ct.CiaCod
              AND d.OriCod = ct.OriCod
              AND d.DocTipCod IN ('{invoice_debit}')
              AND d.DocEst <> '0'
              AND d.DocFecCre >= DATEADD(YEAR, -2, GETDATE())
              AND d.LocCod IN (
                SELECT v.LocCod
                FROM VALID_LOCS v
                WHERE v.OriCod = d.OriCod
                  AND v.CiaCod = d.CiaCod
              )
        ) THEN 1 ELSE 0 END AS HAS_INV_2Y,
        CASE WHEN EXISTS (
            SELECT 1
            FROM DocCab d WITH (NOLOCK)
            WHERE d.CiaCod = ct.CiaCod
              AND d.OriCod = ct.OriCod
              AND d.DocTipCod IN ('{invoice_debit}')
              AND d.DocEst <> '0'
              AND d.DocSld > 0
              AND d.DocFecCre < DATEADD(YEAR, -2, GETDATE())
              AND d.LocCod IN (
                SELECT v.LocCod
                FROM VALID_LOCS v
                WHERE v.OriCod = d.OriCod
                  AND v.CiaCod = d.CiaCod
              )
        ) THEN 1 ELSE 0 END AS HAS_INV_OP_OLD,
        CASE WHEN EXISTS (
            SELECT 1
            FROM DocCab d WITH (NOLOCK)
            WHERE d.CiaCod = ct.CiaCod
              AND d.OriCod = ct.OriCod
              AND d.DocTipCod IN ('{purchase_orders}')
              AND d.DocEst <> '0'
              AND d.DocFecCre >= DATEADD(YEAR, -2, GETDATE())
              AND d.LocCod IN (
                SELECT v.LocCod
                FROM VALID_LOCS v
                WHERE v.OriCod = d.OriCod
                  AND v.CiaCod = d.CiaCod
              )
        ) THEN 1 ELSE 0 END AS HAS_PO_2Y
) f
OUTER APPLY (
    SELECT
        CASE
            WHEN f.HAS_INV_OP_OLD = 1 THEN 'INV_OP'
            WHEN f.HAS_INV_1Y     = 1 THEN 'INV_1Y'
            WHEN f.HAS_INV_2Y     = 1 THEN 'INV_2Y'
            WHEN f.HAS_PO_2Y      = 1 THEN 'POH_OP'
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
      AND d.LocCod IN (
        SELECT v.LocCod
        FROM VALID_LOCS v
        WHERE v.OriCod = d.OriCod
          AND v.CiaCod = d.CiaCod
      )
) trans
OUTER APPLY (
    SELECT
        SUM(CASE
                WHEN dc.DocSld > 0 THEN
                    CASE
                        WHEN dc.DocTipCod IN ('{balance_positive}') THEN  dc.DocSld * @CHF_RATE
                        WHEN dc.DocTipCod IN ('{balance_negative}')                     THEN -dc.DocSld * @CHF_RATE
                        ELSE 0
                    END
                ELSE 0
            END) AS OPEN_BALANCE
    FROM DocCab dc WITH (NOLOCK)
    WHERE dc.CiaCod = ct.CiaCod
      AND dc.OriCod = ct.OriCod
      AND dc.DocTipCod IN ('{all_transactional}')
      AND dc.DocEst <> '0'
      AND dc.LocCod IN (
        SELECT v.LocCod
        FROM VALID_LOCS v
        WHERE v.OriCod = dc.OriCod
          AND v.CiaCod = dc.CiaCod
      )
) priority
WHERE ct.OriCod IN ('{valid_origins}')
  AND ct.CiaEst = '1'
  AND b.IndGrp = '6'
  AND b.IndCod = '2'
  AND ct.CiaCod NOT IN ('{excluded_vendors}')
  AND ct.CiaIdeNum NOT LIKE 'F%'
  AND (
        /* PRIORIDAD 1 */
        priority.OPEN_BALANCE > 0
        /* PRIORIDAD 2 */
        OR EXISTS (
            SELECT 1
            FROM DocCab oc WITH (NOLOCK)
            WHERE oc.CiaCod = ct.CiaCod
              AND oc.OriCod = ct.OriCod
              AND oc.DocFecCre >= DATEADD(YEAR, -2, GETDATE())
              AND oc.DocTipCod IN ('{purchase_orders}')
              AND oc.DocEst <> '0'
              AND oc.LocCod IN (
                SELECT v.LocCod
                FROM VALID_LOCS v
                WHERE v.OriCod = oc.OriCod
                  AND v.CiaCod = oc.CiaCod
              )
        )
        /* PRIORIDAD 3 (aquí sí exige transacciones + documento 2y) */
        OR (
            trans.DOC_COUNT_2Y > 1
            AND EXISTS (
                SELECT 1
                FROM DocCab dc WITH (NOLOCK)
                WHERE dc.CiaCod = ct.CiaCod
                  AND dc.OriCod = ct.OriCod
                  AND dc.DocFecCre >= DATEADD(YEAR, -2, GETDATE())
                  AND dc.DocTipCod IN ('AP', 'C2', 'FE', 'FP', 'NP', 'NA', 'DE', 'NO')
                  AND dc.DocEst <> '0'
                  AND dc.LocCod IN (
                    SELECT v.LocCod
                    FROM VALID_LOCS v
                    WHERE v.OriCod = dc.OriCod
                      AND v.CiaCod = dc.CiaCod
                  )
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
            AND d.LocCod IN (
                SELECT v.LocCod
                FROM VALID_LOCS v
                WHERE v.OriCod = d.OriCod
                  AND v.CiaCod = d.CiaCod
            )
    )
;
"""
