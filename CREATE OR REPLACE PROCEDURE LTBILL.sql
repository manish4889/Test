CREATE OR REPLACE PROCEDURE LTBILL.SMART_METER_MASTER_SYNC(P_CONS_NO                  IN NUMBER,
                                                    V_MR_NO                    OUT VARCHAR2,
                                                    V_BOOK_NO                  OUT VARCHAR2,
                                                    V_ROUTE_CODE               OUT VARCHAR2,
                                                    V_CUSCODE                  OUT VARCHAR2,
                                                    V_CYCLE_NO                 OUT VARCHAR2,
                                                    V_CNAME                    OUT VARCHAR2,
                                                    V_ADDRESS1                 OUT VARCHAR2,
                                                    V_ADDRESS2                 OUT VARCHAR2,
                                                    V_ADDRESS3                 OUT VARCHAR2,
                                                    V_TCODE                    OUT VARCHAR2,
                                                    V_SEASONAL_INDICATOR       OUT VARCHAR2,
                                                    V_FIXED_CHARGE_TXT         OUT VARCHAR2,
                                                    V_FIXED_CHARGE             OUT VARCHAR2,
                                                    V_METER_CAPACITOR_RENT     OUT VARCHAR2,
                                                    V_CAPACITOR_RENT           OUT VARCHAR2,
                                                    V_TIME_SWITCH_RENT         OUT VARCHAR2,
                                                    V_MISCHARGE                OUT VARCHAR2,
                                                    V_TOT_ARREAR               OUT VARCHAR2,
                                                    V_THEFT_ARREARS            OUT VARCHAR2,
                                                    V_LITIGATION_ARREARS       OUT VARCHAR2,
                                                    V_COLLAMT                  OUT VARCHAR2,
                                                    V_METER_NO                 OUT VARCHAR2,
                                                    V_METER_STATUS             OUT VARCHAR2,
                                                    V_MRENT_CODE               OUT VARCHAR2,
                                                    V_AVG_UNIT_NOD             OUT VARCHAR2,
                                                    V_PREVIOUS_CONSUMPTION     OUT VARCHAR2,
                                                    V_PAST_READING_KWH         OUT VARCHAR2,
                                                    V_OLD_NUMBER               OUT VARCHAR2,
                                                    V_EDUTY_PERCENTAGE         OUT VARCHAR2,
                                                    V_ADJUSTMENT_AMOUNT        OUT VARCHAR2,
                                                    V_TAX_ON_SALE_RATE         OUT VARCHAR2,
                                                    V_FUEL_RATE                OUT VARCHAR2,
                                                    V_D_P_C                    OUT VARCHAR2,
                                                    V_FINAL_MF                 OUT VARCHAR2,
                                                    V_BILLING_PERIOD           OUT VARCHAR2,
                                                    V_PD                       OUT VARCHAR2,
                                                    V_NIGHT_REBATE             OUT VARCHAR2,
                                                    V_NEW_IND_DATE             OUT VARCHAR2,
                                                    V_ED_EXEMPTION_DATE        OUT VARCHAR2,
                                                    V_PREVIOUS_BILL_AMOUNT     OUT VARCHAR2,
                                                    V_TOTAL_VILLAGE_KWH        OUT VARCHAR2,
                                                    V_TOTAL_VILLAGE_AMOUNT     OUT VARCHAR2,
                                                    V_NO_DAYS                  OUT VARCHAR2,
                                                    V_LOCK_DAYS                OUT VARCHAR2,
                                                    V_LOCK_INDICATOR           OUT VARCHAR2,
                                                    V_EMPLOYEE_CODE            OUT VARCHAR2,
                                                    V_FREE_UNIT                OUT VARCHAR2,
                                                    V_PROVISIONAL_BILL_AMOUNT  OUT VARCHAR2,
                                                    V_RELIEF_ARREAR_IND        OUT VARCHAR2,
                                                    V_DIFFAMT                  OUT VARCHAR2,
                                                    V_THEFT_INDICATOR          OUT VARCHAR2,
                                                    V_ADVANCE_PAYMENT          OUT VARCHAR2,
                                                    V_ADVANCE_INTEREST         OUT VARCHAR2,
                                                    V_ADVANCE_INTEREST_DATE    OUT VARCHAR2,
                                                    V_SECURITY                 OUT VARCHAR2,
                                                    V_FEEDER_NO                OUT VARCHAR2,
                                                    V_INDUSTRY_TYPE            OUT VARCHAR2,
                                                    V_REACTIVE_READING         OUT VARCHAR2,
                                                    V_AVERAGE_ACTUAL_DEMANT    OUT VARCHAR2,
                                                    V_TRANS_LOCATION           OUT VARCHAR2,
                                                    V_POLE_NO                  OUT VARCHAR2,
                                                    V_CENCUS_CD                OUT VARCHAR2,
                                                    V_NEW_FUEL_RATE            OUT VARCHAR2,
                                                    V_EFFECTIVE_DATE           OUT VARCHAR2,
                                                    V_PREVIOUS_DATE            OUT VARCHAR2,
                                                    V_FIRST_MONTH_CONSUMPTION  OUT VARCHAR2,
                                                    V_SECOND_MONTH_CONSUMPTION OUT VARCHAR2,
                                                    V_THIRD_MONTH_CONSUMPTION  OUT VARCHAR2,
                                                    V_FIRST_MONTH_BILL_AMOUNT  OUT VARCHAR2,
                                                    V_SECOND_MONTH_BILL_AMOUNT OUT VARCHAR2,
                                                    V_THIRD_MONTH_BILL_AMOUNT  OUT VARCHAR2,
                                                    V_FIRST_MONTH              OUT VARCHAR2,
                                                    V_SECOND_MONTH             OUT VARCHAR2,
                                                    V_THIRD_MONTH              OUT VARCHAR2,
                                                    V_SOLAR_CONSUMER           OUT VARCHAR2,
                                                    V_SUB_METER_NO             OUT VARCHAR2,
                                                    V_METER_TYPE               OUT VARCHAR2,
                                                    V_METER_SEQ                OUT VARCHAR2,
                                                    V_PAST_IMP_READING         OUT VARCHAR2,
                                                    V_PAST_EXP_READING         OUT VARCHAR2,
                                                    V_SUB_METER_COUNT          OUT VARCHAR2,
                                                    V_OFF_PICK                 OUT VARCHAR2,
                                                    V_NIGHT_READING            OUT VARCHAR2,
                                                    V_CMD                      OUT VARCHAR2,
                                                    V_CMD_NIGHT                OUT VARCHAR2,
                                                    V_CMD_OFF_PICK             OUT VARCHAR2,
                                                    V_KVAH                     OUT VARCHAR2,
                                                    V_TEMP_DATE                OUT VARCHAR2,
                                                    V_APPC                     OUT VARCHAR2,
                                                    V_BLOCKDWN_DT              OUT VARCHAR2,
                                                    V_MST_NO_BIL               OUT VARCHAR2,
                                                    V_AMR_IND                  OUT VARCHAR2,
                                                    V_VILLAGE                  OUT VARCHAR2,
                                                    V_TALUKA                   OUT VARCHAR2,
                                                    V_DISTRICT                 OUT VARCHAR2,
                                                    V_FEEDER_CD                OUT VARCHAR2,
                                                    V_FEEDER_NAME              OUT VARCHAR2,
                                                    V_MTR_STATUS               OUT VARCHAR2,
                                                    V_EDUTY_CODE               OUT VARCHAR2) IS

  P_SUBDIV_CODE             VARCHAR2(3);
  P_CYCLE_NO                VARCHAR2(3);
  V_CUT_OFF_DATE            DATE;
  V_PREV_CUT_OFF_DATE       DATE;
  V_CLOAD                   VARCHAR2(20);
  V_ARREARS_AMOUNT          VARCHAR2(20);
  V_PREVIOUS_PAYMENT_AMOUNT VARCHAR2(20);
  V_CUST_STATUS             VARCHAR2(1);
  V_PAYEMENT_UPTO_DATE      DATE;
  V_CONTRACT_DEMAND         VARCHAR2(20);
  V_CCAT                    VARCHAR2(6);
  V_PAST_READING_KVARH      VARCHAR2(15);
  TEMPVAR                   VARCHAR2(10);
  V_BILLED_MONTH            VARCHAR2(2);
  V_BILLED_YEAR             VARCHAR2(4);
  V_ADJ_YEAR                VARCHAR2(4);
  V_ADJ_MONTH               VARCHAR2(2);
  V_COUNT                   VARCHAR2(20);
  V_FS_CHG                  VARCHAR2(20);
  V_ED_CODE                 VARCHAR2(3);

BEGIN

  SELECT C.SUBDIV_CODE, C.CYCLE_NO
    INTO P_SUBDIV_CODE, P_CYCLE_NO
    FROM CUSTMASTER C
   WHERE C.CUSCODE = P_CONS_NO;

 /* SELECT MAX(CMIN.PAY_UPD_DATE)
    INTO V_CUT_OFF_DATE
    FROM CUTOFFDATE_MASTER CMIN
   WHERE CMIN.SUB_DIVISION_CODE = P_SUBDIV_CODE
     AND CMIN.CYCLE_NO = P_CYCLE_NO;

  SELECT MAX(CM.PAY_UPD_DATE)
    INTO V_PREV_CUT_OFF_DATE
    FROM CUTOFFDATE_MASTER CM
   WHERE CM.SUB_DIVISION_CODE = P_SUBDIV_CODE
     AND CM.CYCLE_NO = P_CYCLE_NO
     AND CM.PAY_UPD_DATE <
         (SELECT MAX(CMIN.PAY_UPD_DATE)
            FROM CUTOFFDATE_MASTER CMIN
           WHERE CMIN.SUB_DIVISION_CODE = P_SUBDIV_CODE
             AND CMIN.CYCLE_NO = P_CYCLE_NO);*/

  FOR C_DATA IN (SELECT *
                   FROM (SELECT /*+ index(MM, IDX_SUBDIVN_MM1)*/
                          LPAD(CUS.MR_NO, 2, '0') AS MR_NO,
                          LPAD(CUS.BOOK_NO, 2, '0') AS BOOK_NO,
                          LPAD(CUS.ROUTE_CODE, 3, '0') AS ROUTE_CODE,
                          CUS.CUSCODE,
                          CUS.CYCLE_NO,
                          RPAD(REGEXP_REPLACE(CUS.CNAME, '[[:cntrl:]]', NULL),
                               30,
                               ' ') CNAME,
                          RPAD(REGEXP_REPLACE(CUS.ADDRESS1,
                                              '[[:cntrl:]]',
                                              NULL),
                               25,
                               ' ') ADDRESS1,
                          RPAD(REGEXP_REPLACE(CUS.ADDRESS2,
                                              '[[:cntrl:]]',
                                              NULL),
                               25,
                               ' ') ADDRESS2,
                          RPAD(REGEXP_REPLACE(CUS.ADDRESS3,
                                              '[[:cntrl:]]',
                                              NULL),
                               25,
                               ' ') ADDRESS3,
                          RPAD(REPLACE(CUS.TCODE, '.', ''), 5, ' ') TCODE,
                          CUS.SEASONAL_INDICATOR,
                          PB.CONTRACT_LOAD CLOAD,
                          CUS.EDUTY_CODE,
                          PB.FIXED_CHARGE,
                          PB.CAPACITOR_RENT,
                          NVL(PB.METER_CAPACITOR_RENT, 0) +
                          NVL(PB.CAPACITOR_RENT, 0) METER_CAPACITOR_RENT,
                          (CASE
                            WHEN LENGTH(PB.V_ARREARS_AMOUNT) > 11 THEN
                             SUBSTR(PB.V_ARREARS_AMOUNT,
                                    LENGTH(PB.V_ARREARS_AMOUNT) - 10,
                                    11)
                            ELSE
                             TRIM(PB.V_ARREARS_AMOUNT)
                          END) AS V_ARREARS_AMOUNT,
                          NVL(PB.THEFT_ARREARS, 0) THEFT_ARREARS,
                          NVL(PB.LITIGATION_ARREARS, 0) LITIGATION_ARREARS,
                          PB.PREVIOUS_PAYMENT_AMOUNT,
                         /* NVL(SUBSTR(LPAD(TRIM(MM.METER_NO), 25), -12),
                              '            ') METER_NO,*/
                              TRIM(MM.METER_NO) METER_NO,
                          CUS.CUST_STATUS,
                          
                          MM.MRENT_CODE,
                          --TRUNC(SUBSTR((nvl(PB.AVG_UNIT_NOD, 0) * nvl(pb.billing_factor, 0)), 0, 6)) AVG_UNIT_NOD,
                          CASE
                            WHEN PB.NO_OF_DAYS IS NOT NULL THEN
                             TRUNC(SUBSTR((NVL(PB.AVG_UNIT_NOD, 0) *
                                          (NVL(PB.NO_OF_DAYS, 30) / 30)),
                                          0,
                                          6))
                            ELSE
                             TRUNC(SUBSTR((NVL(PB.AVG_UNIT_NOD, 0) *
                                          NVL(PB.BILLING_FACTOR, 0)),
                                          0,
                                          6))
                          END AVG_UNIT_NOD, /*ADD BY PARBHU 28-09-2016*/
                          PB.METER_STATUS,
                          PB.PREVIOUS_CONSUMPTION,
                          DECODE(PB.NEW_PAST_RDNG,
                                 NULL,
                                 PB.PAST_READING_KWH,
                                 PB.NEW_PAST_RDNG) PAST_READING_KWH,
                          PB.D_P_C,
                          MM.FINAL_MF,
                          LPAD(CASE
                                 WHEN LENGTH(PB.BILLING_PERIOD) > 9 THEN
                                  SUBSTR(PB.BILLING_PERIOD, 0, 3) ||
                                  SUBSTR(PB.BILLING_PERIOD,
                                         LENGTH(PB.BILLING_PERIOD) - 6,
                                         7)
                                 ELSE
                                  PB.BILLING_PERIOD
                               END,
                               10,
                               ' ') BILLING_PERIOD,
                          PB.PAYEMENT_UPTO_DATE,
                          PB.MISCHARGE,
                          TO_CHAR(PB.PAYEMENT_UPTO_DATE, 'YYYYMMDD') AS PD,
                          (CASE
                            WHEN LENGTH(PB.PREVIOUS_BILL_AMOUNT) > 7 THEN
                             SUBSTR(PB.PREVIOUS_BILL_AMOUNT, 0, 7)
                            ELSE
                             TRIM(PB.PREVIOUS_BILL_AMOUNT)
                          END) AS PREVIOUS_BILL_AMOUNT,
                          PB.CONTRACT_DEMAND,
                          PB.PREVIOUS_PAYMENT_AMOUNT COLLAMT,
                          PB.NO_OF_DAYS NO_DAYS,
                          PB.LOCK_DAYS,
                          PB.LOCK_DAYS / 30 AS LOCK_MONTH,
                          CUS.EMPLOYEE_CODE,
                          (CASE
                            WHEN LENGTH(PB.PROVAMT_REF) > 7 THEN
                             SUBSTR(PB.PROVAMT_REF, 0, 7)
                            ELSE
                             TRIM(PB.PROVAMT_REF)
                          END) AS PROVISIONAL_BILL_AMOUNT,
                          CUS.SECURITY,
                          PB.ADJUSTMENT_AMOUNT,
                          CUS.CCAT,
                          CUS.FEEDER_NO,
                          CUS.INDUSTRY_TYPE,
                          DECODE(PB.NEW_REACTIVE_REDG,
                                 NULL,
                                 NVL(PB.PAST_READING_KVARH, 0),
                                 PB.NEW_REACTIVE_REDG) PAST_READING_KVARH,
                          CUS.TRANS_LOCATION,
                          SUBSTR(CUS.POLE_NO, 0, 4) POLE_NO,
                          PB.FUEL_RATE,
                          PB.LOCK_INDICATOR,
                          PB.BILLED_MONTH,
                          PB.BILLED_YEAR,
                          PB.BULB_AMT,
                          LPAD(NVL(CLS.EMI_COUNTER, '0000'), 4, '0') AS EMI_COUNTER,
                          LPAD(NVL(CLS.NO_OF_EMI, '0000'), 4, '0') AS NO_OF_EMI,
                          LPAD(CUS.CENCUS_CD, 8, '0') AS CENCUS_CD,
                          PB.FIRST_MONTH_CONSUMPTION, /*ADD BY PARBHU 28-09-2016*/
                          PB.SECOND_MONTH_CONSUMPTION, /*ADD BY PARBHU 28-09-2016*/
                          PB.THIRD_MONTH_CONSUMPTION, /*ADD BY PARBHU 28-09-2016*/
                          PB.FIRST_MONTH_BILL_AMOUNT, /*ADD BY PARBHU 28-09-2016*/
                          PB.SECOND_MONTH_BILL_AMOUNT, /*ADD BY PARBHU 28-09-2016*/
                          PB.THIRD_MONTH_BILL_AMOUNT, /*ADD BY PARBHU 28-09-2016*/
                          PB.FIRST_MONTH, /*ADD BY PARBHU 28-09-2016*/
                          PB.SECOND_MONTH, /*ADD BY PARBHU 28-09-2016*/
                          PB.THIRD_MONTH, /*ADD BY PARBHU 30-09-2016*/
                          NVL(LPAD(TRIM(CUS.SOLAR_CONSUMER), 1), 'N') AS SOLAR_CONSUMER,
                          '            ' AS SUB_METER_NO,
                          CASE
                            WHEN CUS.TCODE = 'TMP' THEN
                             NVL(LPAD(TRIM(PB.MT_TYPE), 1), 'T')
                            ELSE
                             NVL(LPAD(TRIM(PB.MT_TYPE), 1), 'M')
                          END AS METER_TYPE,
                          NVL(LPAD(TRIM(PB.METER_SEQ), 1), '1') AS METER_SEQ,
                          DECODE(PB.NEW_IMP_KWH,
                                 NULL,
                                 PB.END_IMP_RDNG,
                                 PB.NEW_IMP_KWH) PAST_IMP_READING,
                          DECODE(PB.NEW_EXP_KWH,
                                 NULL,
                                 PB.END_EXP_RDNG,
                                 PB.NEW_EXP_KWH) PAST_EXP_READING,
                          CUS.TEMP_CONN_DATE
                           FROM CUSTMASTER   CUS,
                                METER_MASTER MM,
                                PRINTED_BILL PB
                           LEFT OUTER JOIN CONSUMER_LED_SUBSIDY CLS
                             ON (PB.CONSUMER_NO = CLS.ACCOUNT_NUMBER)
                          WHERE CUS.SUBDIV_CODE = MM.SUBDIVISION_CODE
                            AND CUS.SUBDIV_CODE = PB.SUB_DIVISION_CODE
                            AND MM.SUBDIVISION_CODE = PB.SUB_DIVISION_CODE
                            AND CUS.CUSCODE = MM.CUSTOMER_NO
                            AND CUS.CUSCODE = PB.CONSUMER_NO
                            AND MM.CUSTOMER_NO = PB.CONSUMER_NO
                            AND MM.CUSTOMER_NO = PB.CONSUMER_NO
                            AND CUS.SUBDIV_CODE = P_SUBDIV_CODE
                            AND CUS.CYCLE_NO = P_CYCLE_NO
                            AND CUS.CUSCODE = P_CONS_NO
                            AND CUS.TCODE NOT IN ('A1')
                               /* AND PB.BILLED_YEAR = P_YEAR
                               AND PB.BILLED_MONTH = P_MONTH*/
                            AND PB.PAYEMENT_UPTO_DATE IN (SELECT MAX(PP.PAYEMENT_UPTO_DATE) FROM PRINTED_BILL PP WHERE PP.SUB_DIVISION_CODE=PB.SUB_DIVISION_CODE
                            AND PP.CONSUMER_NO=PB.CONSUMER_NO)
                            AND NVL(PB.MT_TYPE, 'M') IN ('M', 'T')
                            AND NVL(PB.METER_SEQ, '1') = '1'
                               --AND (PB.BILLED_YEAR * 100 + PB.BILLED_MONTH) = (P_YEAR * 100 + P_MONTH)
                               /*               (SELECT MAX(P.BILLED_YEAR * 100 + P.BILLED_MONTH)
                                FROM PRINTED_BILL P
                               WHERE P.SUB_DIVISION_CODE = P_SUBDIV_CODE
                                 and P.TARIFF NOT IN ('A1')
                                 AND P.CYCLE_NUMBER = P_CYCLE_NO)*/
                            AND (CUS.CUST_STATUS IS NULL OR
                                CUS.CUST_STATUS NOT IN ('X', 'Y'))
                         UNION ALL
                         SELECT /*+ index(MM, IDX_SUBDIVN_MM1)*/
                          LPAD(CUS.MR_NO, 2, '0') AS MR_NO,
                          LPAD(CUS.BOOK_NO, 2, '0') AS BOOK_NO,
                          LPAD(CUS.ROUTE_CODE, 3, '0') AS ROUTE_CODE,
                          CUS.CUSCODE,
                          CUS.CYCLE_NO,
                          RPAD(REGEXP_REPLACE(CUS.CNAME, '[[:cntrl:]]', NULL),
                               30,
                               ' ') CNAME,
                          RPAD(REGEXP_REPLACE(CUS.ADDRESS1,
                                              '[[:cntrl:]]',
                                              NULL),
                               25,
                               ' ') ADDRESS1,
                          RPAD(REGEXP_REPLACE(CUS.ADDRESS2,
                                              '[[:cntrl:]]',
                                              NULL),
                               25,
                               ' ') ADDRESS2,
                          RPAD(REGEXP_REPLACE(CUS.ADDRESS3,
                                              '[[:cntrl:]]',
                                              NULL),
                               25,
                               ' ') ADDRESS3,
                          RPAD(REPLACE(CUS.TCODE, '.', ''), 5, ' ') TCODE,
                          CUS.SEASONAL_INDICATOR,
                          PB.CONTRACT_LOAD CLOAD,
                          CUS.EDUTY_CODE,
                          PB.FIXED_CHARGE,
                          PB.CAPACITOR_RENT,
                          NVL(PB.METER_CAPACITOR_RENT, 0) +
                          NVL(PB.CAPACITOR_RENT, 0) METER_CAPACITOR_RENT,
                          (CASE
                            WHEN LENGTH(PB.V_ARREARS_AMOUNT) > 11 THEN
                             SUBSTR(PB.V_ARREARS_AMOUNT,
                                    LENGTH(PB.V_ARREARS_AMOUNT) - 10,
                                    11)
                            ELSE
                             TRIM(PB.V_ARREARS_AMOUNT)
                          END) AS V_ARREARS_AMOUNT,
                          NVL(PB.THEFT_ARREARS, 0) THEFT_ARREARS,
                          NVL(PB.LITIGATION_ARREARS, 0) LITIGATION_ARREARS,
                          PB.PREVIOUS_PAYMENT_AMOUNT,
                          '            ' METER_NO,
                          CUS.CUST_STATUS,
                          
                          SMM.MRENT_CODE,
                          --TRUNC(SUBSTR((nvl(PB.AVG_UNIT_NOD, 0) * nvl(pb.billing_factor, 0)), 0, 6)) AVG_UNIT_NOD,
                          CASE
                            WHEN PB.NO_OF_DAYS IS NOT NULL THEN
                             TRUNC(SUBSTR((NVL(PB.AVG_UNIT_NOD, 0) *
                                          (NVL(PB.NO_OF_DAYS, 30) / 30)),
                                          0,
                                          6))
                            ELSE
                             TRUNC(SUBSTR((NVL(PB.AVG_UNIT_NOD, 0) *
                                          NVL(PB.BILLING_FACTOR, 0)),
                                          0,
                                          6))
                          END AVG_UNIT_NOD, /*ADD BY PARBHU 30-09-2016*/
                          PB.METER_STATUS,
                          PB.PREVIOUS_CONSUMPTION,
                          DECODE(PB.NEW_PAST_RDNG,
                                 NULL,
                                 PB.PAST_READING_KWH,
                                 PB.NEW_PAST_RDNG) PAST_READING_KWH,
                          PB.D_P_C,
                          SMM.FINAL_MF,
                          LPAD(CASE
                                 WHEN LENGTH(PB.BILLING_PERIOD) > 9 THEN
                                  SUBSTR(PB.BILLING_PERIOD, 0, 3) ||
                                  SUBSTR(PB.BILLING_PERIOD,
                                         LENGTH(PB.BILLING_PERIOD) - 6,
                                         7)
                                 ELSE
                                  PB.BILLING_PERIOD
                               END,
                               10,
                               ' ') BILLING_PERIOD,
                          PB.PAYEMENT_UPTO_DATE,
                          PB.MISCHARGE,
                          TO_CHAR(PB.PAYEMENT_UPTO_DATE, 'YYYYMMDD') AS PD,
                          (CASE
                            WHEN LENGTH(PB.PREVIOUS_BILL_AMOUNT) > 7 THEN
                             SUBSTR(PB.PREVIOUS_BILL_AMOUNT, 0, 7)
                            ELSE
                             TRIM(PB.PREVIOUS_BILL_AMOUNT)
                          END) AS PREVIOUS_BILL_AMOUNT,
                          PB.CONTRACT_DEMAND,
                          PB.PREVIOUS_PAYMENT_AMOUNT COLLAMT,
                          PB.NO_OF_DAYS NO_DAYS,
                          PB.LOCK_DAYS,
                          PB.LOCK_DAYS / 30 AS LOCK_MONTH,
                          CUS.EMPLOYEE_CODE,
                          (CASE
                            WHEN LENGTH(PB.PROVAMT_REF) > 7 THEN
                             SUBSTR(PB.PROVAMT_REF, 0, 7)
                            ELSE
                             TRIM(PB.PROVAMT_REF)
                          END) AS PROVISIONAL_BILL_AMOUNT,
                          CUS.SECURITY,
                          PB.ADJUSTMENT_AMOUNT,
                          CUS.CCAT,
                          CUS.FEEDER_NO,
                          CUS.INDUSTRY_TYPE,
                          DECODE(PB.NEW_REACTIVE_REDG,
                                 NULL,
                                 NVL(PB.PAST_READING_KVARH, 0),
                                 PB.NEW_REACTIVE_REDG) PAST_READING_KVARH,
                          CUS.TRANS_LOCATION,
                          SUBSTR(CUS.POLE_NO, 0, 4) POLE_NO,
                          PB.FUEL_RATE,
                          PB.LOCK_INDICATOR,
                          PB.BILLED_MONTH,
                          PB.BILLED_YEAR,
                          PB.BULB_AMT,
                          '0000' AS EMI_COUNTER,
                          '0000' AS NO_OF_EMI,
                          LPAD(CUS.CENCUS_CD, 8, '0') AS CENCUS_CD,
                          PB.FIRST_MONTH_CONSUMPTION, /*ADD BY PARBHU 30-09-2016*/
                          PB.SECOND_MONTH_CONSUMPTION, /*ADD BY PARBHU 30-09-2016*/
                          PB.THIRD_MONTH_CONSUMPTION, /*ADD BY PARBHU 30-09-2016*/
                          PB.FIRST_MONTH_BILL_AMOUNT, /*ADD BY PARBHU 30-09-2016*/
                          PB.SECOND_MONTH_BILL_AMOUNT, /*ADD BY PARBHU 30-09-2016*/
                          PB.THIRD_MONTH_BILL_AMOUNT, /*ADD BY PARBHU 30-09-2016*/
                          PB.FIRST_MONTH, /*ADD BY PARBHU 30-09-2016*/
                          PB.SECOND_MONTH, /*ADD BY PARBHU 30-09-2016*/
                          PB.THIRD_MONTH, /*ADD BY PARBHU 30-09-2016*/
                          NVL(LPAD(TRIM(CUS.SOLAR_CONSUMER), 1), 'N') AS SOLAR_CONSUMER,
                          NVL(SUBSTR(LPAD(TRIM(SMM.SUB_METER_NO), 25), -12),
                              '            ') AS SUB_METER_NO,
                          NVL(LPAD(TRIM(PB.MT_TYPE), 1), 'M') AS METER_TYPE,
                          NVL(LPAD(TRIM(PB.METER_SEQ), 1), '1') AS METER_SEQ,
                          DECODE(PB.NEW_IMP_KWH,
                                 NULL,
                                 PB.END_IMP_RDNG,
                                 PB.NEW_IMP_KWH) PAST_IMP_READING,
                          DECODE(PB.NEW_EXP_KWH,
                                 NULL,
                                 PB.END_EXP_RDNG,
                                 PB.NEW_EXP_KWH) PAST_EXP_READING,
                          CUS.TEMP_CONN_DATE
                           FROM CUSTMASTER       CUS,
                                SUB_METER_MASTER SMM,
                                PRINTED_BILL     PB
                          WHERE CUS.SUBDIV_CODE = SMM.SUBDIVISION_CODE
                            AND CUS.SUBDIV_CODE = PB.SUB_DIVISION_CODE
                            AND SMM.SUBDIVISION_CODE = PB.SUB_DIVISION_CODE
                            AND CUS.CUSCODE = SMM.CUSTOMER_NO
                            AND CUS.CUSCODE = PB.CONSUMER_NO
                            AND SMM.CUSTOMER_NO = PB.CONSUMER_NO
                            AND SMM.CUSTOMER_NO = PB.CONSUMER_NO
                            AND SMM.METER_SEQ = PB.METER_SEQ
                            AND CUS.SUBDIV_CODE = P_SUBDIV_CODE
                            AND CUS.CYCLE_NO = P_CYCLE_NO
                            AND CUS.CUSCODE = P_CONS_NO
                            AND CUS.TCODE NOT IN ('A1')
                               /*    AND PB.BILLED_YEAR = P_YEAR
                               AND PB.BILLED_MONTH = P_MONTH*/
                            AND PB.PAYEMENT_UPTO_DATE IN (SELECT MAX(PP.PAYEMENT_UPTO_DATE) FROM PRINTED_BILL PP WHERE PP.SUB_DIVISION_CODE=PB.SUB_DIVISION_CODE
                            AND PP.CONSUMER_NO=PB.CONSUMER_NO)
                            AND NVL(PB.MT_TYPE, 'M') = 'S'
                               --AND (PB.BILLED_YEAR * 100 + PB.BILLED_MONTH) = (P_YEAR * 100 + P_MONTH)
                               /*               (SELECT MAX(P.BILLED_YEAR * 100 + P.BILLED_MONTH)
                                FROM PRINTED_BILL P
                               WHERE P.SUB_DIVISION_CODE = P_SUBDIV_CODE
                                 and P.TARIFF NOT IN ('A1')
                                 AND P.CYCLE_NUMBER = P_CYCLE_NO)*/
                            AND (CUS.CUST_STATUS IS NULL OR
                                CUS.CUST_STATUS NOT IN ('X', 'Y'))) SUB
                  ORDER BY LPAD(SUB.MR_NO, 2, '0'),
                           LPAD(SUB.BOOK_NO, 2, '0'),
                           LPAD(SUB.ROUTE_CODE, 3, '0')) LOOP
    BEGIN
      V_MR_NO          := C_DATA.MR_NO;
      V_BOOK_NO        := C_DATA.BOOK_NO;
      V_ROUTE_CODE     := C_DATA.ROUTE_CODE;
      V_CUSCODE        := C_DATA.CUSCODE;
      V_CYCLE_NO       := C_DATA.CYCLE_NO;
      V_SOLAR_CONSUMER := C_DATA.SOLAR_CONSUMER;
      V_METER_TYPE     := C_DATA.METER_TYPE;
      V_METER_SEQ      := C_DATA.METER_SEQ;
      IF (C_DATA.CNAME IS NULL) THEN
        V_CNAME := '                              ';
      ELSE
        V_CNAME := C_DATA.CNAME;
      END IF;
      IF (C_DATA.ADDRESS1 IS NULL) THEN
        V_ADDRESS1 := '                         ';
      ELSE
        V_ADDRESS1 := C_DATA.ADDRESS1;
      END IF;
      IF (C_DATA.ADDRESS2 IS NULL) THEN
        V_ADDRESS2 := '                         ';
      ELSE
        V_ADDRESS2 := C_DATA.ADDRESS2;
      END IF;
      IF (C_DATA.ADDRESS3 IS NULL) THEN
        V_ADDRESS3 := '                         ';
      ELSE
        V_ADDRESS3 := C_DATA.ADDRESS3;
      END IF;
      V_TCODE      := C_DATA.TCODE;
      V_EDUTY_CODE := C_DATA.EDUTY_CODE;
      IF (C_DATA.SEASONAL_INDICATOR IS NULL) THEN
        V_SEASONAL_INDICATOR := ' ';
      ELSE
        V_SEASONAL_INDICATOR := C_DATA.SEASONAL_INDICATOR;
      END IF;
      IF (C_DATA.CLOAD IS NULL) THEN
        V_CLOAD := '0000.00';
      ELSE
        IF (LENGTH(C_DATA.CLOAD) < 8) THEN
          V_CLOAD := LPAD(TO_CHAR(C_DATA.CLOAD, 'fm99999999.90'), 7, '0');
        ELSE
          V_CLOAD := '0000.00';
        END IF;
      END IF;
      V_EDUTY_CODE := C_DATA.EDUTY_CODE;
      IF (C_DATA.FIXED_CHARGE IS NULL) THEN
        V_FIXED_CHARGE := '0000000.00';
      ELSE
        IF (LENGTH(C_DATA.FIXED_CHARGE) < 11) THEN
          V_FIXED_CHARGE := LPAD(TO_CHAR(C_DATA.FIXED_CHARGE,
                                         'fm9999999.90'),
                                 10,
                                 0);
        ELSE
          V_FIXED_CHARGE := '0000000.00';
        END IF;
      END IF;
      IF (C_DATA.METER_CAPACITOR_RENT IS NULL) THEN
        V_METER_CAPACITOR_RENT := '0000000.00';
      ELSE
        IF (LENGTH(C_DATA.METER_CAPACITOR_RENT) < 11) THEN
          V_METER_CAPACITOR_RENT := LPAD(TO_CHAR(C_DATA.METER_CAPACITOR_RENT,
                                                 'fm9999999.90'),
                                         10,
                                         0);
        ELSE
          V_METER_CAPACITOR_RENT := '0000000.00';
        END IF;
      END IF;
      V_MISCHARGE               := C_DATA.MISCHARGE;
      V_TEMP_DATE               := TRIM(TO_CHAR(NVL(TO_CHAR(C_DATA.TEMP_CONN_DATE,
                                                            'YYYYMMDD'),
                                                    0),
                                                '00000000'));
      V_ARREARS_AMOUNT          := C_DATA.V_ARREARS_AMOUNT;
      V_THEFT_ARREARS           := C_DATA.THEFT_ARREARS;
      V_LITIGATION_ARREARS      := C_DATA.LITIGATION_ARREARS;
      V_PREVIOUS_PAYMENT_AMOUNT := C_DATA.PREVIOUS_PAYMENT_AMOUNT;
      V_METER_NO                := C_DATA.METER_NO;
      V_SUB_METER_NO            := LPAD(C_DATA.SUB_METER_NO, 12, ' ');
      V_CUST_STATUS             := C_DATA.CUST_STATUS;
      IF (C_DATA.TEMP_CONN_DATE IS NULL) THEN
        V_TEMP_DATE := '00000000';
      END IF;
      IF (C_DATA.MRENT_CODE IS NULL) THEN
        V_MRENT_CODE := ' ';
      ELSE
        V_MRENT_CODE := C_DATA.MRENT_CODE;
      END IF;
      IF (C_DATA.AVG_UNIT_NOD IS NULL) THEN
        V_AVG_UNIT_NOD := '000000';
      ELSE
        V_AVG_UNIT_NOD := LPAD(C_DATA.AVG_UNIT_NOD, 6, '0');
      END IF;
      IF (C_DATA.METER_STATUS IS NULL) THEN
        V_METER_STATUS := ' ';
      ELSE
        V_METER_STATUS := C_DATA.METER_STATUS;
      END IF;
      ---Start Add BY Prabhu on 30-Sep-2016
      IF (C_DATA.FIRST_MONTH_CONSUMPTION IS NULL) THEN
        V_FIRST_MONTH_CONSUMPTION := '00000000';
      ELSE
        V_FIRST_MONTH_CONSUMPTION := LPAD(C_DATA.FIRST_MONTH_CONSUMPTION,
                                          8,
                                          '0');
      END IF;
      IF (C_DATA.SECOND_MONTH_CONSUMPTION IS NULL) THEN
        V_SECOND_MONTH_CONSUMPTION := '00000000';
      ELSE
        V_SECOND_MONTH_CONSUMPTION := LPAD(C_DATA.SECOND_MONTH_CONSUMPTION,
                                           8,
                                           '0');
      END IF;
      IF (C_DATA.THIRD_MONTH_CONSUMPTION IS NULL) THEN
        V_THIRD_MONTH_CONSUMPTION := '00000000';
      ELSE
        V_THIRD_MONTH_CONSUMPTION := LPAD(C_DATA.THIRD_MONTH_CONSUMPTION,
                                          8,
                                          '0');
      END IF;
      IF (C_DATA.FIRST_MONTH IS NULL) THEN
        V_FIRST_MONTH := '00';
      ELSE
        V_FIRST_MONTH := LPAD(C_DATA.FIRST_MONTH, 2, '0');
      END IF;
      IF (C_DATA.SECOND_MONTH IS NULL) THEN
        V_SECOND_MONTH := '00';
      ELSE
        V_SECOND_MONTH := LPAD(C_DATA.SECOND_MONTH, 2, '0');
      END IF;
      IF (C_DATA.THIRD_MONTH IS NULL) THEN
        V_THIRD_MONTH := '00';
      ELSE
        V_THIRD_MONTH := LPAD(C_DATA.THIRD_MONTH, 2, '0');
      END IF;
      --End
      IF (C_DATA.PREVIOUS_CONSUMPTION IS NULL) THEN
        V_PREVIOUS_CONSUMPTION := '00000000';
      ELSE
        V_PREVIOUS_CONSUMPTION := LPAD(C_DATA.PREVIOUS_CONSUMPTION, 8, '0');
      END IF;
      IF (C_DATA.PAST_READING_KWH IS NULL) THEN
        V_PAST_READING_KWH := '00000000';
      ELSE
        V_PAST_READING_KWH := LPAD(C_DATA.PAST_READING_KWH, 8, '0');
      END IF;
      IF (C_DATA.PAST_IMP_READING IS NULL) THEN
        V_PAST_IMP_READING := '00000000';
      ELSE
        V_PAST_IMP_READING := LPAD(C_DATA.PAST_IMP_READING, 8, '0');
      END IF;
      IF (C_DATA.PAST_EXP_READING IS NULL) THEN
        V_PAST_EXP_READING := '00000000';
      ELSE
        V_PAST_EXP_READING := LPAD(C_DATA.PAST_EXP_READING, 8, '0');
      END IF;
      IF (C_DATA.D_P_C IS NULL) THEN
        V_D_P_C := '0000000.00';
      ELSE
        IF (LENGTH(C_DATA.D_P_C) < 11) THEN
          V_D_P_C := LPAD(TO_CHAR(C_DATA.D_P_C, 'fm9999999.90'), 10, 0);
        ELSE
          V_D_P_C := '0000000.00';
        END IF;
      END IF;
      IF (C_DATA.FINAL_MF IS NULL) THEN
        V_FINAL_MF := '000.0';
      ELSE
        IF (LENGTH(C_DATA.FINAL_MF) < 6) THEN
          V_FINAL_MF := LPAD(TO_CHAR(C_DATA.FINAL_MF, 'fm9999999.0'),
                             5,
                             '0');
        ELSE
          V_FINAL_MF := '000.0';
        END IF;
      END IF;
      V_BILLING_PERIOD     := UPPER(C_DATA.BILLING_PERIOD);
      V_PAYEMENT_UPTO_DATE := C_DATA.PAYEMENT_UPTO_DATE;
      IF (C_DATA.MISCHARGE < 0) THEN
        IF (LENGTH(C_DATA.MISCHARGE) < 12) THEN
          V_MISCHARGE := '-' || LPAD(TO_CHAR((C_DATA.MISCHARGE) * (-1),
                                             'fm9999999.90'),
                                     10,
                                     '0');
        ELSE
          V_MISCHARGE := ' 0000000.00';
        END IF;
      ELSE
        IF (LENGTH(C_DATA.MISCHARGE) < 11) THEN
          V_MISCHARGE := ' ' ||
                         LPAD(TO_CHAR((C_DATA.MISCHARGE), 'fm9999999.90'),
                              10,
                              '0');
        ELSE
          V_MISCHARGE := ' 0000000.00';
        END IF;
      END IF;
      IF (C_DATA.PD IS NULL) THEN
        V_PD := '00000000';
      ELSE
        V_PD := LPAD(C_DATA.PD, 8, '0');
      END IF;
      --Start Add By Prabhu on 28-Sep-2016
      IF (C_DATA.FIRST_MONTH_BILL_AMOUNT < 0) THEN
        IF (LENGTH(C_DATA.FIRST_MONTH_BILL_AMOUNT) < 12) THEN
          V_FIRST_MONTH_BILL_AMOUNT := '-' || LPAD(TO_CHAR((C_DATA.FIRST_MONTH_BILL_AMOUNT) * (-1),
                                                           'fm9999999.90'),
                                                   10,
                                                   '0');
        ELSE
          V_FIRST_MONTH_BILL_AMOUNT := ' 0000000.00';
        END IF;
      ELSE
        IF (LENGTH(C_DATA.FIRST_MONTH_BILL_AMOUNT) < 11) THEN
          V_FIRST_MONTH_BILL_AMOUNT := ' ' || LPAD(TO_CHAR((C_DATA.FIRST_MONTH_BILL_AMOUNT),
                                                           'fm9999999.90'),
                                                   10,
                                                   '0');
        ELSE
          V_FIRST_MONTH_BILL_AMOUNT := ' 0000000.00';
        END IF;
      END IF;
      IF (C_DATA.SECOND_MONTH_BILL_AMOUNT < 0) THEN
        IF (LENGTH(C_DATA.SECOND_MONTH_BILL_AMOUNT) < 12) THEN
          V_SECOND_MONTH_BILL_AMOUNT := '-' || LPAD(TO_CHAR((C_DATA.SECOND_MONTH_BILL_AMOUNT) * (-1),
                                                            'fm9999999.90'),
                                                    10,
                                                    '0');
        ELSE
          V_SECOND_MONTH_BILL_AMOUNT := ' 0000000.00';
        END IF;
      ELSE
        IF (LENGTH(C_DATA.SECOND_MONTH_BILL_AMOUNT) < 11) THEN
          V_SECOND_MONTH_BILL_AMOUNT := ' ' || LPAD(TO_CHAR((C_DATA.SECOND_MONTH_BILL_AMOUNT),
                                                            'fm9999999.90'),
                                                    10,
                                                    '0');
        ELSE
          V_SECOND_MONTH_BILL_AMOUNT := ' 0000000.00';
        END IF;
      END IF;
      IF (C_DATA.THIRD_MONTH_BILL_AMOUNT < 0) THEN
        IF (LENGTH(C_DATA.THIRD_MONTH_BILL_AMOUNT) < 12) THEN
          V_THIRD_MONTH_BILL_AMOUNT := '-' || LPAD(TO_CHAR((C_DATA.THIRD_MONTH_BILL_AMOUNT) * (-1),
                                                           'fm9999999.90'),
                                                   10,
                                                   '0');
        ELSE
          V_THIRD_MONTH_BILL_AMOUNT := ' 0000000.00';
        END IF;
      ELSE
        IF (LENGTH(C_DATA.THIRD_MONTH_BILL_AMOUNT) < 11) THEN
          V_THIRD_MONTH_BILL_AMOUNT := ' ' || LPAD(TO_CHAR((C_DATA.THIRD_MONTH_BILL_AMOUNT),
                                                           'fm9999999.90'),
                                                   10,
                                                   '0');
        ELSE
          V_THIRD_MONTH_BILL_AMOUNT := ' 0000000.00';
        END IF;
      END IF;
      --end
      IF (C_DATA.PREVIOUS_BILL_AMOUNT < 0) THEN
        IF (LENGTH(C_DATA.PREVIOUS_BILL_AMOUNT) < 12) THEN
          V_PREVIOUS_BILL_AMOUNT := '-' || LPAD(TO_CHAR((C_DATA.PREVIOUS_BILL_AMOUNT) * (-1),
                                                        'fm9999999.90'),
                                                10,
                                                '0');
        ELSE
          V_PREVIOUS_BILL_AMOUNT := ' 0000000.00';
        END IF;
      ELSE
        IF (LENGTH(C_DATA.PREVIOUS_BILL_AMOUNT) < 11) THEN
          V_PREVIOUS_BILL_AMOUNT := ' ' || LPAD(TO_CHAR((C_DATA.PREVIOUS_BILL_AMOUNT),
                                                        'fm9999999.90'),
                                                10,
                                                '0');
        ELSE
          V_PREVIOUS_BILL_AMOUNT := ' 0000000.00';
        END IF;
      END IF;
      IF (C_DATA.CONTRACT_DEMAND IS NULL) THEN
        V_CONTRACT_DEMAND := '0000.00';
      ELSE
        IF (LENGTH(C_DATA.D_P_C) < 8) THEN
          V_CONTRACT_DEMAND := LPAD(TO_CHAR(C_DATA.CONTRACT_DEMAND,
                                            'fm9999999.90'),
                                    7,
                                    '0');
        ELSE
          V_CONTRACT_DEMAND := '0000.00';
        END IF;
      END IF;
      IF (C_DATA.COLLAMT < 0) THEN
        IF (LENGTH(C_DATA.COLLAMT) < 12) THEN
          V_COLLAMT := '-' ||
                       LPAD(TO_CHAR((C_DATA.COLLAMT) * (-1), 'fm9999999.90'),
                            10,
                            '0');
        ELSE
          V_COLLAMT := ' 0000000.00';
        END IF;
      ELSE
        IF (LENGTH(C_DATA.COLLAMT) < 11) THEN
          V_COLLAMT := ' ' || LPAD(TO_CHAR((C_DATA.COLLAMT), 'fm9999999.90'),
                                   10,
                                   '0');
        ELSE
          V_COLLAMT := ' 0000000.00';
        END IF;
      END IF;
      IF (C_DATA.NO_DAYS IS NULL) THEN
        V_NO_DAYS := '000';
      ELSE
        V_NO_DAYS := LPAD(C_DATA.NO_DAYS, 3, '0');
      END IF;
      IF (C_DATA.LOCK_DAYS IS NULL) THEN
        V_LOCK_DAYS := '000';
      ELSE
        V_LOCK_DAYS := LPAD(C_DATA.LOCK_DAYS, 3, '0');
      END IF;
      --V_LOCK_MONTH := C_DATA.LOCK_MONTH;
      IF (C_DATA.EMPLOYEE_CODE IS NULL) THEN
        V_EMPLOYEE_CODE := ' ';
      ELSE
        V_EMPLOYEE_CODE := C_DATA.EMPLOYEE_CODE;
      END IF;
      IF (C_DATA.PROVISIONAL_BILL_AMOUNT < 0) THEN
        IF (LENGTH(C_DATA.PROVISIONAL_BILL_AMOUNT) < 12) THEN
          V_PROVISIONAL_BILL_AMOUNT := '-' || LPAD(TO_CHAR(C_DATA.PROVISIONAL_BILL_AMOUNT * (-1),
                                                           'fm9999999.90'),
                                                   10,
                                                   '0');
        ELSE
          V_PROVISIONAL_BILL_AMOUNT := ' 0000000.00';
        END IF;
      ELSE
        IF (LENGTH(C_DATA.PROVISIONAL_BILL_AMOUNT) < 11) THEN
          V_PROVISIONAL_BILL_AMOUNT := ' ' || LPAD(TO_CHAR(C_DATA.PROVISIONAL_BILL_AMOUNT,
                                                           'fm9999999.90'),
                                                   10,
                                                   '0');
        ELSE
          V_PROVISIONAL_BILL_AMOUNT := ' 0000000.00';
        END IF;
      END IF;
      IF (C_DATA.SECURITY < 0) THEN
        IF (LENGTH(C_DATA.SECURITY) < 12) THEN
          V_SECURITY := '-' || LPAD(TO_CHAR((C_DATA.SECURITY) * (-1),
                                            'fm9999999.90'),
                                    10,
                                    '0');
        ELSE
          V_SECURITY := ' 0000000.00';
        END IF;
      ELSE
        IF (LENGTH(C_DATA.SECURITY) < 11) THEN
          V_SECURITY := ' ' ||
                        LPAD(TO_CHAR((C_DATA.SECURITY), 'fm9999999.90'),
                             10,
                             '0');
        ELSE
          V_SECURITY := ' 0000000.00';
        END IF;
      END IF;
      IF (C_DATA.ADJUSTMENT_AMOUNT < 0) THEN
        IF (LENGTH(C_DATA.ADJUSTMENT_AMOUNT) < 12) THEN
          V_ADJUSTMENT_AMOUNT := '-' || LPAD(TO_CHAR((C_DATA.ADJUSTMENT_AMOUNT) * (-1),
                                                     'fm9999999.90'),
                                             10,
                                             '0');
        ELSE
          V_ADJUSTMENT_AMOUNT := ' 0000000.00';
        END IF;
      ELSE
        IF (LENGTH(C_DATA.ADJUSTMENT_AMOUNT) < 11) THEN
          V_ADJUSTMENT_AMOUNT := ' ' || LPAD(TO_CHAR((C_DATA.ADJUSTMENT_AMOUNT),
                                                     'fm9999999.90'),
                                             10,
                                             '0');
        ELSE
          V_ADJUSTMENT_AMOUNT := ' 0000000.00';
        END IF;
      END IF;
      V_CCAT := C_DATA.CCAT;
      IF (C_DATA.FEEDER_NO IS NULL) THEN
        V_FEEDER_NO := '00';
      ELSE
        V_FEEDER_NO := LPAD(C_DATA.FEEDER_NO, 2, '0');
      END IF;
      IF (C_DATA.INDUSTRY_TYPE IS NULL) THEN
        V_INDUSTRY_TYPE := '  ';
      ELSE
        V_INDUSTRY_TYPE := RPAD(C_DATA.INDUSTRY_TYPE, 2, ' ');
      END IF;
      V_PAST_READING_KVARH := C_DATA.PAST_READING_KVARH;
      IF (C_DATA.TRANS_LOCATION IS NULL) THEN
        V_TRANS_LOCATION := '      ';
      ELSE
        V_TRANS_LOCATION := RPAD(C_DATA.TRANS_LOCATION, 6, ' ');
      END IF;
      IF (C_DATA.POLE_NO IS NULL) THEN
        V_POLE_NO := '0000';
      ELSE
        V_POLE_NO := LPAD(C_DATA.POLE_NO, 4, '0');
      END IF;
    
      V_SUB_METER_COUNT := 0;
      SELECT COUNT(1)
        INTO V_SUB_METER_COUNT
        FROM SUB_METER_MASTER SM
       WHERE SM.SUBDIVISION_CODE = P_SUBDIV_CODE
         AND SM.CUSTOMER_NO = V_CUSCODE;
      V_SUB_METER_COUNT := LPAD(V_SUB_METER_COUNT, 2, '0');
      TEMPVAR           := C_DATA.BILLED_MONTH;
      TEMPVAR           := LPAD(TEMPVAR, 2, 0);
    
      SELECT MAX(P.PARAM_CODE)
        INTO TEMPVAR
        FROM PARAMETER_TABLE P
       WHERE P.DISP_ORDER = '27'
         AND TO_CHAR(P.UPDATED_ON, 'YYYYMM') <=
             C_DATA.BILLED_YEAR || TEMPVAR;
    
      SELECT NUM_VALUE
        INTO V_FUEL_RATE
        FROM PARAMETER_TABLE
       WHERE PARAM_CODE = TEMPVAR;
    
      /*TEMPVAR := V_BILLED_MONTH;
      TEMPVAR := LPAD(TEMPVAR,2,0);
      IF((V_BILLED_YEAR||TEMPVAR) >= '201601') THEN
        TEMPVAR := 'BLT093';
      ELSE
        TEMPVAR  := 'BLT092';
      END IF;
      
      SELECT NUM_VALUE INTO V_FUEL_RATE FROM PARAMETER_TABLE WHERE PARAM_CODE =TEMPVAR;*/
    
      IF (C_DATA.LOCK_INDICATOR IS NULL) THEN
        V_LOCK_INDICATOR := '0';
      ELSE
        V_LOCK_INDICATOR := C_DATA.LOCK_INDICATOR;
      END IF;
      V_BILLED_MONTH := C_DATA.BILLED_MONTH;
      V_BILLED_YEAR  := C_DATA.BILLED_YEAR;
      V_CENCUS_CD    := C_DATA.CENCUS_CD;
    
      SELECT CS.VILLAGE_NAME, CS.DIST_NAME, CS.TALUK_NAME
        INTO V_VILLAGE, V_DISTRICT, V_TALUKA
        FROM CENCUS_MASTER CS
       WHERE TO_NUMBER(CS.CENCUS_CD) = TO_NUMBER(C_DATA.CENCUS_CD);
    
      SELECT FF.FEEDER_CODE,FF.FEEDER_NAME
        INTO V_FEEDER_CD,  V_FEEDER_NAME
        FROM FEEDER_MASTER FF
       WHERE FF.SUB_DIVISION_CODE = P_SUBDIV_CODE
         AND TO_NUMBER(FF.FEEDER_NUMBER) = TO_NUMBER(C_DATA.FEEDER_NO);
    
      IF (V_TCODE = 'LTMD ') THEN
        V_FIXED_CHARGE_TXT := V_CONTRACT_DEMAND;
      ELSE
        V_FIXED_CHARGE_TXT := V_CLOAD;
      END IF;
      V_TOT_ARREAR := V_ARREARS_AMOUNT + V_THEFT_ARREARS +
                      V_LITIGATION_ARREARS;
      IF (V_TOT_ARREAR < 0) THEN
        IF (LENGTH(V_TOT_ARREAR) < 13) THEN
          V_TOT_ARREAR := '-' || LPAD(TO_CHAR((V_TOT_ARREAR) * (-1),
                                              'fm99999999.90'),
                                      11,
                                      '0');
        ELSE
          V_TOT_ARREAR := ' 00000000.00';
        END IF;
      ELSE
        IF (LENGTH(V_TOT_ARREAR) < 12) THEN
          V_TOT_ARREAR := ' ' ||
                          LPAD(TO_CHAR((V_TOT_ARREAR), 'fm99999999.90'),
                               11,
                               '0');
        ELSE
          V_TOT_ARREAR := ' 00000000.00';
        END IF;
      END IF;
      IF (V_THEFT_ARREARS < 0) THEN
        IF (LENGTH(V_THEFT_ARREARS) < 12) THEN
          V_THEFT_ARREARS := '-' || LPAD(TO_CHAR((V_THEFT_ARREARS) * (-1),
                                                 'fm9999999.90'),
                                         10,
                                         '0');
        ELSE
          V_THEFT_ARREARS := ' 0000000.00';
        END IF;
      ELSE
        IF (LENGTH(V_THEFT_ARREARS) < 11) THEN
          V_THEFT_ARREARS := ' ' ||
                             LPAD(TO_CHAR((V_THEFT_ARREARS), 'fm9999999.90'),
                                  10,
                                  '0');
        ELSE
          V_THEFT_ARREARS := ' 0000000.00';
        END IF;
      END IF;
      IF (V_LITIGATION_ARREARS < 0) THEN
        IF (LENGTH(V_LITIGATION_ARREARS) < 12) THEN
          V_LITIGATION_ARREARS := '-' || LPAD(TO_CHAR((V_LITIGATION_ARREARS) * (-1),
                                                      'fm9999999.90'),
                                              10,
                                              '0');
        ELSE
          V_LITIGATION_ARREARS := ' 0000000.00';
        END IF;
      ELSE
        IF (LENGTH(V_LITIGATION_ARREARS) < 11) THEN
          V_LITIGATION_ARREARS := ' ' || LPAD(TO_CHAR((V_LITIGATION_ARREARS),
                                                      'fm9999999.90'),
                                              10,
                                              '0');
        ELSE
          V_LITIGATION_ARREARS := ' 0000000.00';
        END IF;
      END IF;
      IF (LENGTH(V_FUEL_RATE) < 7) THEN
        V_FUEL_RATE := LPAD(TO_CHAR((V_FUEL_RATE), 'fm999.90'), 6, '0');
      ELSE
        V_FUEL_RATE := '000.00';
      END IF;
      IF (V_CCAT = 'CAT05') THEN
        V_FUEL_RATE := '000.00';
      ELSE
        V_FUEL_RATE := V_FUEL_RATE;
      END IF;
      IF (V_TCODE = 'LTMD ') THEN
        V_REACTIVE_READING := LPAD(V_PAST_READING_KVARH, 8, 0);
      ELSE
        V_REACTIVE_READING := '00000000';
      END IF;
      V_ADJ_YEAR  := EXTRACT(YEAR FROM V_PAYEMENT_UPTO_DATE);
      V_ADJ_MONTH := EXTRACT(MONTH FROM V_PAYEMENT_UPTO_DATE);
    
      V_ADVANCE_PAYMENT       := 0;
      V_ADVANCE_INTEREST      := 0;
      V_ADVANCE_INTEREST_DATE := 0;
    
      SELECT MIN(CM.PER_OF_DUTY)
        INTO V_EDUTY_PERCENTAGE
        FROM COMBINATION_MASTER CM
       WHERE TO_NUMBER(CM.COM_CODE) = TO_NUMBER(V_EDUTY_CODE)
         AND (V_BILLED_YEAR * 100 + V_BILLED_MONTH) BETWEEN
             TO_CHAR(CM.EFFECT_DATE, 'YYYYMM') AND
             TO_CHAR(NVL(CM.TO_DT, SYSDATE), 'YYYYMM');
    
      IF (V_BILLED_YEAR = '2012' AND V_BILLED_MONTH <= '4') THEN
        IF (V_EDUTY_PERCENTAGE IS NULL) THEN
          V_EDUTY_PERCENTAGE := '00';
        ELSE
          V_EDUTY_PERCENTAGE := LPAD(V_EDUTY_PERCENTAGE, 2, 0);
        END IF;
      ELSE
        IF (V_EDUTY_PERCENTAGE IS NULL) THEN
          V_EDUTY_PERCENTAGE := '00.00';
        ELSE
          IF (LENGTH(V_EDUTY_PERCENTAGE) < 6) THEN
            V_EDUTY_PERCENTAGE := LPAD(TO_CHAR((V_EDUTY_PERCENTAGE),
                                               'fm99.90'),
                                       5,
                                       0);
          ELSE
            V_EDUTY_PERCENTAGE := '00.00';
          END IF;
        END IF;
      END IF;
      BEGIN
        SELECT MAX(DISTINCT(TRIM(TO_CHAR(RED.NEW_FUEL_RATE, '000.00')))),
               MAX(TRIM((SELECT TO_CHAR(NVL(TO_CHAR(MAX(T.FROM_DT),
                                                   'YYYYMMDD'),
                                           0),
                                       '00000000')
                          FROM TARIFF_ENERGY_MASTER T
                         WHERE T.TO_DT IS NULL))) AS EFFECTIVE_DATE,
               MAX(TRIM(TO_CHAR(NVL(TO_CHAR(RED.TRANS_DT, 'YYYYMMDD'), 0),
                                '00000000'))) PREVIOUS_DATE
          INTO V_NEW_FUEL_RATE, V_EFFECTIVE_DATE, V_PREVIOUS_DATE
          FROM METERREADING RED
         WHERE RED.SUBDIV_CODE = P_SUBDIV_CODE
           AND RED.CUSTOMER_NO = V_CUSCODE
           AND RED.METER_TYPE = NVL(C_DATA.METER_TYPE, 'M')
           AND RED.METER_SEQ = NVL(C_DATA.METER_SEQ, '1')
           AND (RED.YEAR * 100 + RED.MON) =
               (SELECT MAX(R1.YEAR * 100 + R1.MON)
                  FROM METERREADING R1
                 WHERE R1.SUBDIV_CODE = P_SUBDIV_CODE
                   AND R1.CUSTOMER_NO = V_CUSCODE
                   AND R1.METER_TYPE = NVL(C_DATA.METER_TYPE, 'M')
                   AND R1.METER_SEQ = NVL(C_DATA.METER_SEQ, '1'));
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          SELECT MAX(DISTINCT(TRIM(TO_CHAR(0.00, '000.00')))),
                 MAX(TRIM((SELECT TO_CHAR(NVL(TO_CHAR(MAX(T.FROM_DT),
                                                     'YYYYMMDD'),
                                             0),
                                         '00000000')
                            FROM TARIFF_ENERGY_MASTER T
                           WHERE T.TO_DT IS NULL))) AS EFFECTIVE_DATE,
                 MAX(TRIM(TO_CHAR(NVL(TO_CHAR(RED.TRANS_DT, 'YYYYMMDD'), 0),
                                  '00000000'))) PREVIOUS_DATE
            INTO V_NEW_FUEL_RATE, V_EFFECTIVE_DATE, V_PREVIOUS_DATE
            FROM METERREADING_TEMP RED
           WHERE RED.SUBDIV_CODE = P_SUBDIV_CODE
             AND RED.CUSTOMER_NO = V_CUSCODE
                /* AND RED.METER_TYPE = NVL(C_DATA.METER_TYPE, 'M')
                AND RED.METER_SEQ = NVL(C_DATA.METER_SEQ, '1')*/
             AND (RED.YEAR * 100 + RED.MON) =
                 (SELECT MAX(R1.YEAR * 100 + R1.MON)
                    FROM METERREADING R1
                   WHERE R1.SUBDIV_CODE = P_SUBDIV_CODE
                     AND R1.CUSTOMER_NO = V_CUSCODE
                  /* AND R1.METER_TYPE = NVL(C_DATA.METER_TYPE, 'M')
                  AND R1.METER_SEQ = NVL(C_DATA.METER_SEQ, '1')*/
                  );
      END;
      IF (V_NEW_FUEL_RATE IS NULL) THEN
        V_NEW_FUEL_RATE  := '000.00';
        V_EFFECTIVE_DATE := '20160401';
        V_PREVIOUS_DATE  := '00000000';
      ELSE
        IF (V_CCAT = 'CAT05') THEN
          V_NEW_FUEL_RATE := '000.00';
        ELSE
          IF (LENGTH(V_NEW_FUEL_RATE) < 7) THEN
            V_NEW_FUEL_RATE := LPAD(TO_CHAR((V_NEW_FUEL_RATE * 100),
                                            'fm999.90'),
                                    6,
                                    0);
          ELSE
            V_NEW_FUEL_RATE := '000.00';
          END IF;
        END IF;
      END IF;
      IF TRIM(V_TCODE) = 'TMP' AND
         (V_PREVIOUS_DATE IS NULL OR V_PREVIOUS_DATE = '00000000') THEN
        SELECT TO_CHAR(C.CONNECTION_RELEASE_DT, 'YYYYMMDD')
          INTO V_PREVIOUS_DATE
          FROM CUSTMASTER C
         WHERE C.CUSCODE = V_CUSCODE;
      END IF;
      V_TOTAL_VILLAGE_KWH    := 0;
      V_TOTAL_VILLAGE_AMOUNT := 0;
    
      SELECT COUNT(1)
        INTO V_COUNT
        FROM METERREADING
       WHERE SUBDIV_CODE = P_SUBDIV_CODE
         AND CUSTOMER_NO = V_CUSCODE
         AND METER_TYPE = NVL(C_DATA.METER_TYPE, 'M')
         AND METER_SEQ = NVL(C_DATA.METER_SEQ, '1');
    
      IF (V_COUNT <> 0) THEN
        BEGIN
          SELECT TO_CHAR(NVL(MR.FS_CHG, 0), '0000000.00') AS FS_CHG,
                 NVL(MR.DIFFAMT, 0) AS DIFFAMT,
                 (CASE
                   WHEN LENGTH(MR.VILLAGE_UNIT) > 7 THEN
                    SUBSTR(MR.VILLAGE_UNIT, 0, 7)
                   ELSE
                    LPAD(MR.VILLAGE_UNIT, 7, 0)
                 END) AS VILLAGE_UNIT,
                 (CASE
                   WHEN LENGTH(MR.VILLAGE_AMT) > 7 THEN
                    SUBSTR(MR.VILLAGE_AMT, 0, 7)
                   ELSE
                    LPAD(MR.VILLAGE_AMT, 7, 0)
                 END) AS VILLAGE_AMT
            INTO V_FS_CHG,
                 V_DIFFAMT,
                 V_TOTAL_VILLAGE_KWH,
                 V_TOTAL_VILLAGE_AMOUNT
            FROM METERREADING MR
           WHERE MR.CUSTOMER_NO = V_CUSCODE
             AND MR.METER_TYPE = NVL(C_DATA.METER_TYPE, 'M')
             AND MR.METER_SEQ = NVL(C_DATA.METER_SEQ, '1')
             AND MR.YEAR * 100 + MR.MON =
                 (SELECT MAX(MRIN.YEAR * 100 + MRIN.MON)
                    FROM METERREADING MRIN
                   WHERE MRIN.SUBDIV_CODE = P_SUBDIV_CODE
                     AND MRIN.CUSTOMER_NO = V_CUSCODE
                     AND MRIN.METER_TYPE = NVL(C_DATA.METER_TYPE, 'M')
                     AND MRIN.METER_SEQ = NVL(C_DATA.METER_SEQ, '1'))
             AND MR.SUBDIV_CODE = P_SUBDIV_CODE;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            SELECT TO_CHAR(NVL(MR.FS_CHG, 0), '0000000.00') AS FS_CHG,
                   NVL(MR.DIFFAMT, 0) AS DIFFAMT,
                   (CASE
                     WHEN LENGTH(MR.VILLAGE_UNIT) > 7 THEN
                      SUBSTR(MR.VILLAGE_UNIT, 0, 7)
                     ELSE
                      LPAD(MR.VILLAGE_UNIT, 7, 0)
                   END) AS VILLAGE_UNIT,
                   (CASE
                     WHEN LENGTH(MR.VILLAGE_AMT) > 7 THEN
                      SUBSTR(MR.VILLAGE_AMT, 0, 7)
                     ELSE
                      LPAD(MR.VILLAGE_AMT, 7, 0)
                   END) AS VILLAGE_AMT
              INTO V_FS_CHG,
                   V_DIFFAMT,
                   V_TOTAL_VILLAGE_KWH,
                   V_TOTAL_VILLAGE_AMOUNT
              FROM METERREADING_TEMP MR
             WHERE MR.CUSTOMER_NO = V_CUSCODE
               AND MR.YEAR * 100 + MR.MON =
                   (SELECT MAX(MRIN.YEAR * 100 + MRIN.MON)
                      FROM METERREADING_TEMP MRIN
                     WHERE MRIN.SUBDIV_CODE = P_SUBDIV_CODE
                       AND MRIN.CUSTOMER_NO = V_CUSCODE)
               AND MR.SUBDIV_CODE = P_SUBDIV_CODE;
        END;
        IF (V_DIFFAMT < 0) THEN
          IF (LENGTH(V_DIFFAMT) < 12) THEN
            V_DIFFAMT := '-' ||
                         LPAD(TO_CHAR((V_DIFFAMT) * (-1), 'fm9999999.90'),
                              10,
                              '0');
          ELSE
            V_DIFFAMT := ' 0000000.00';
          END IF;
        ELSE
          IF (LENGTH(V_DIFFAMT) < 11) THEN
            V_DIFFAMT := ' ' ||
                         LPAD(TO_CHAR((V_DIFFAMT), 'fm9999999.90'), 10, '0');
          ELSE
            V_DIFFAMT := ' 0000000.00';
          END IF;
        END IF;
      ELSE
        V_DIFFAMT              := ' 0000000.00';
        V_TOTAL_VILLAGE_KWH    := 0;
        V_TOTAL_VILLAGE_AMOUNT := 0;
      END IF;
    
      IF (V_TOTAL_VILLAGE_KWH > 250 AND
         (V_EDUTY_CODE = 91 OR V_EDUTY_CODE = 2)) THEN
        IF (V_EDUTY_CODE = 91) THEN
          V_ED_CODE := 90;
        ELSIF (V_EDUTY_CODE = 2) THEN
          V_ED_CODE := 1;
        END IF;
      
        SELECT MIN(CM.PER_OF_DUTY)
          INTO V_EDUTY_PERCENTAGE
          FROM COMBINATION_MASTER CM
         WHERE TO_NUMBER(CM.COM_CODE) = TO_NUMBER(V_ED_CODE)
           AND (V_BILLED_YEAR * 100 + V_BILLED_MONTH) BETWEEN
               TO_CHAR(CM.EFFECT_DATE, 'YYYYMM') AND
               TO_CHAR(NVL(CM.TO_DT, SYSDATE), 'YYYYMM');
        IF (V_BILLED_YEAR = 2012 AND V_BILLED_MONTH <= 4) THEN
          IF (V_EDUTY_PERCENTAGE IS NULL) THEN
            V_EDUTY_PERCENTAGE := '00';
          ELSE
            V_EDUTY_PERCENTAGE := LPAD(V_EDUTY_PERCENTAGE, 2, 0);
          END IF;
        ELSE
          IF (V_EDUTY_PERCENTAGE IS NULL) THEN
            V_EDUTY_PERCENTAGE := '00.00';
          ELSE
            V_EDUTY_PERCENTAGE := LPAD(TO_CHAR((V_EDUTY_PERCENTAGE),
                                               'fm99.90'),
                                       5,
                                       0);
          END IF;
        END IF;
      END IF;
      /* V_EDUTY_PERCENTAGE := '00.00';*/
      IF (V_TOTAL_VILLAGE_KWH IS NULL) THEN
        V_TOTAL_VILLAGE_KWH := '0000000';
      ELSE
        V_TOTAL_VILLAGE_KWH := LPAD(V_TOTAL_VILLAGE_KWH, 7, '0');
      END IF;
      IF (V_TOTAL_VILLAGE_AMOUNT < 0) THEN
        IF (LENGTH(V_TOTAL_VILLAGE_AMOUNT) < 12) THEN
          V_TOTAL_VILLAGE_AMOUNT := '-' || LPAD(TO_CHAR((V_TOTAL_VILLAGE_AMOUNT) * (-1),
                                                        'fm9999999.90'),
                                                10,
                                                '0');
        ELSE
          V_TOTAL_VILLAGE_AMOUNT := ' 0000000.00';
        END IF;
      ELSE
        IF (LENGTH(V_TOTAL_VILLAGE_AMOUNT) < 11) THEN
          V_TOTAL_VILLAGE_AMOUNT := ' ' || LPAD(TO_CHAR((V_TOTAL_VILLAGE_AMOUNT),
                                                        'fm9999999.90'),
                                                10,
                                                '0');
        ELSE
          V_TOTAL_VILLAGE_AMOUNT := ' 0000000.00';
        END IF;
      END IF;
      /*I*/
      V_CAPACITOR_RENT := C_DATA.CAPACITOR_RENT;
      IF (V_CAPACITOR_RENT IS NULL) THEN
        V_CAPACITOR_RENT := '0000000.00';
      ELSE
        IF (LENGTH(V_CAPACITOR_RENT) < 11) THEN
          V_CAPACITOR_RENT := LPAD(TO_CHAR(V_CAPACITOR_RENT,
                                           'fm99999999.90'),
                                   10,
                                   '0');
        ELSE
          V_CAPACITOR_RENT := '0000000.00';
        END IF;
      END IF;
      ----Modify By Prabhu
      IF (C_DATA.BULB_AMT IS NULL) THEN
        V_TIME_SWITCH_RENT := '0000000.00';
      ELSE
        IF (LENGTH(C_DATA.BULB_AMT) < 11) THEN
          V_TIME_SWITCH_RENT := LPAD(TO_CHAR(C_DATA.BULB_AMT,
                                             'fm99999999.90'),
                                     10,
                                     '0');
        ELSE
          V_TIME_SWITCH_RENT := '0000000.00';
        END IF;
      END IF;
      /*V_TIME_SWITCH_RENT  :='      0.00';*/
      V_TAX_ON_SALE_RATE := ' . ';
      /*P_SUBDIV_CODE:=SUBSTR(V_CUSCODE,1,3);*/
    
      /*V_OLD_NUMBER  :='00000000';*/
      ----Modify By Prabhu
      V_OLD_NUMBER        := C_DATA.EMI_COUNTER || C_DATA.NO_OF_EMI;
      V_NIGHT_REBATE      := '00.00';
      V_NEW_IND_DATE      := '00000000';
      V_ED_EXEMPTION_DATE := '00000000';
    
      V_ADVANCE_PAYMENT       := 0;
      V_ADVANCE_INTEREST      := 0;
      V_ADVANCE_INTEREST_DATE := 0;
      BEGIN
        SELECT ADV.INTEREST_AMOUNT,
               ADV.DEPOSIT_CLOSING,
               TO_CHAR(ADV.INTEREST_TO_DATE, 'YYYYMMDD') AS ADATE
          INTO V_ADVANCE_INTEREST,
               V_ADVANCE_PAYMENT,
               V_ADVANCE_INTEREST_DATE
          FROM ADVANCE_PAYMENT_TRANS ADV
         WHERE ADV.SUB_DIVISION_CODE = P_SUBDIV_CODE
           AND ADV.CONSUMER_NUMBER = V_CUSCODE
           AND (ADV.CUTOFF_DATE, ADV.CREATION_DATE) IN
               (SELECT MAX(ADV1.CUTOFF_DATE), MAX(ADV1.CREATION_DATE)
                  FROM ADVANCE_PAYMENT_TRANS ADV1
                 WHERE ADV1.SUB_DIVISION_CODE = P_SUBDIV_CODE
                   AND ADV1.CONSUMER_NUMBER = V_CUSCODE);
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          V_ADVANCE_PAYMENT       := '';
          V_ADVANCE_INTEREST      := '';
          V_ADVANCE_INTEREST_DATE := '';
      END;
      IF (V_ADVANCE_PAYMENT IS NULL) THEN
        V_ADVANCE_PAYMENT := '0000000.00';
      ELSE
        IF (LENGTH(V_ADVANCE_PAYMENT) < 11) THEN
          V_ADVANCE_PAYMENT := LPAD(TO_CHAR(V_ADVANCE_PAYMENT,
                                            'fm99999999.90'),
                                    10,
                                    '0');
        ELSE
          V_ADVANCE_PAYMENT := '0000000.00';
        END IF;
      END IF;
      IF (V_ADVANCE_INTEREST IS NULL) THEN
        V_ADVANCE_INTEREST := '0000000.00';
      ELSE
        IF (LENGTH(V_ADVANCE_INTEREST) < 11) THEN
          V_ADVANCE_INTEREST := LPAD(TO_CHAR(V_ADVANCE_INTEREST,
                                             'fm99999999.90'),
                                     10,
                                     '0');
        ELSE
          V_ADVANCE_INTEREST := '0000000.00';
        END IF;
      END IF;
      IF (V_ADVANCE_INTEREST_DATE IS NULL) THEN
        V_ADVANCE_INTEREST_DATE := '00000000';
      ELSE
        V_ADVANCE_INTEREST_DATE := LPAD(V_ADVANCE_INTEREST_DATE, 8, '0');
      END IF;
    
      BEGIN
        SELECT RATE INTO V_APPC FROM SOLAR_RATE_MASTER WHERE TO_DT IS NULL;
      END;
      IF (LENGTH(V_APPC) < 5) THEN
        V_APPC := LPAD(TO_CHAR(V_APPC, 'fm999.90'), 5, '0');
      ELSE
        V_APPC := '000.00';
      END IF;
      V_FREE_UNIT             := '000';
      V_RELIEF_ARREAR_IND     := 0;
      V_THEFT_INDICATOR       := ' ';
      V_AVERAGE_ACTUAL_DEMANT := '000';
      V_OFF_PICK              := '00000000';
      V_NIGHT_READING         := '00000000';
      V_CMD                   := '000';
      V_CMD_NIGHT             := '000';
      V_CMD_OFF_PICK          := '000';
      V_KVAH                  := '00000000';
    
    END;
  
  END LOOP;

END SMART_METER_MASTER_SYNC;
/