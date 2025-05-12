import pandas as pd
import cx_Oracle
import dbconn
import cx_Oracle
import pandas as pd
import datetime
from datetime import timedelta

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

pd.set_option('display.width', 800)
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 1190)
from datetime import datetime, timedelta

date = datetime.today()- timedelta(days=1)   # get the date 7 days ago
date1 = datetime.today() - timedelta(days=1)  # get the date 7 days ago
y = date1.strftime("%Y/%m/%d")

date_from = date.strftime("%d-%m-%Y")  # convert to format yyyy-mm-dd

conn = cx_Oracle.connect(dbconn.user, dbconn.password, dbconn.dns)
cur = conn.cursor()
# path = """\\\\\\\\\\\\\\10.222.140.144\d\d\MIS\NSSF"""
path = "\\\\\\\\10.222.140.144\d\d\MIS\Reports\EOD"
query = """
WITH t
     AS (SELECT distinct pa.account_number,
                pa.dep_acc_number,
                c0.surname,
                c.short_descr currency_code,
                TO_DATE(:trxdate_from, 'yyyy/mm/dd') statement_date,
                bp.bank_name
           FROM PRFPRD.bank_parameters bp,
                PRFPRD.cp_agreement a
                JOIN PRFPRD.deposit_account d
                JOIN PRFPRD.profits_account pa
                   ON     pa.dep_acc_number = d.account_number
                      AND pa.secondary_acc != '1'
                JOIN PRFPRD.currency c
                   ON d.fk_currencyid_curr = c.id_currency
                   ON d.account_number = a.tp_dep_account_no
                LEFT JOIN PRFPRD.customer c0 ON c0.cust_id = pa.cust_id
          WHERE a.cp_agreement_no IN (10084, 61)),
     g
     AS (SELECT distinct ROW_NUMBER ()
                OVER (
                   ORDER BY fk_deposit_accoacc, trans_ser_num, entry_ser_num)
                   seq_no,
                COUNT (*) OVER () cnt,
                
                B.DIAS_REFERENCE_NO  AS bank_transaction_number,
                f.prev_acc_balance,
                f.reverse_flag,
                f.value_date,
                case when B.CP_AGREEMENT_NO=61 then 'E'
                      when B.CP_AGREEMENT_NO=10084 then 'I'
                        else 'M' END narration,
                f.trx_date,
                f.debit_credit_flag,
                f.entry_amount,
                f.entry_comments, 
                f.cheque_number
                ,TRIM(SUBSTR (b.orig_ref_no, 4,26))
      
            AS transaction_narration
           FROM PRFPRD.cp_trx_recording t2
         INNER JOIN           
           PRFPRD.fst_demand_extrait f
            ON     t2.trx_date = f.trx_date
               AND t2.trx_unit = f.trx_unit
               AND t2.trx_usr = f.trx_usr
               AND t2.trx_credit_sn = f.trx_sn
         INNER JOIN PRFPRD.cp_ol_collection b
            ON     b.trx_date = t2.trx_date
             and b.trx_unit = t2.trx_unit
               AND b.trx_usr = t2.trx_usr
               AND b.trx_usr_sn_gl = t2.trx_sn
               AND B.CP_AGREEMENT_NO IN ('61', '10084')
         inner join    
                t
               on fk_deposit_accoacc = t.dep_acc_number
               AND f.trx_date = t.statement_date
                LEFT JOIN PRFPRD.generic_detail g
                   ON     g.parameter_type = 'WSDES'
                      AND g.short_description = 'NSSF'                      
          )
    SELECT  distinct 'H' head,
            'CRDB' CRDB,
            'CENTENARY BANK' BANK,
            t.account_number,
            t.surname,
            TO_CHAR (t.statement_date, 'YYYYMMDD'),
            TO_CHAR (g.prev_acc_balance, '999999999990.00'),
            currency_code,
            ''
    FROM t LEFT JOIN g ON g.seq_no = 1
UNION ALL
    SELECT distinct 'D',
            to_char(g.bank_transaction_number),
            g.entry_comments,
            DECODE(g.cheque_number, 0, '', g.cheque_number),
            TO_CHAR (g.entry_amount, '999999999990'),
            DECODE (g.debit_credit_flag, '1', 'D', 'C'),
            TO_CHAR (g.trx_date, 'YYYYMMDD'),
              TO_CHAR (g.value_date, 'YYYYMMDD'),
            narration

    FROM g
UNION ALL
    SELECT  distinct 'T',
            to_char(NVL(g.cnt, 0)),
            TO_CHAR (NVL (DECODE (debit_credit_flag, '1', -1, 1) * entry_amount + g.prev_acc_balance, 0), '999999999990'),
            TO_CHAR (t.statement_date, 'YYYYMMDD'),
            '',
            '',
            '',
            '',
            ''
    FROM t LEFT JOIN g ON g.seq_no = g.cnt 
"""
data = pd.read_sql(query, conn, params={'trxdate_from': y})

query_sta = """
SELECT TRX_SN SN,TRANS_SER_NUM, A.TRX_UNIT UNIT, TO_CHAR(TRX_DATE,'DD Mon YYYY') TRANSACTIONDATE,a.TIMESTAMP, A.TRX_USR USERID,A.ID_TRANSACT TRANSACTIONS,A.ID_JUSTIFIC JUSTIFICation,A.ENTRY_AMOUNT ENTRYAMOUNT, case     when A.DEBIT_CREDIT_FLAG = '1' then A.PREV_ACC_BALANCE - A.ENTRY_AMOUNT   else A.PREV_ACC_BALANCE + A.ENTRY_AMOUNT     end AS Balance,        case         when A.DEBIT_CREDIT_FLAG = '1' then 'D'        else 'C'    end AS DRCR, A.CHEQUE_NUMBER CHEQUENO,A.ENTRY_COMMENTS||' '|| a.COMMENTS1||' '||a.comments2||' '||a.comments3||' '||a.comments4 COMMENTS from prfprd.FST_DEMAND_EXTRAIT A, prfprd.PROFITS_ACCOUNT  B WHERE A.FK_DEPOSIT_ACCOACC = B.DEP_ACC_NUMBER AND B.ACCOUNT_NUMBER='3100003712' AND A.TRX_DATE  = TO_DATE (:trxdate_to, 'dd-mm-yyyy') order by A.TIMESTAMP asc


"""
data_st = pd.read_sql(query_sta, conn, params={'trxdate_to': date_from})
# print(data)
# data.to_excel("NSSF_DATA.xlsx")
data_st['BANK'] = data_st['COMMENTS'].str[:14]
# print(data_st)
outer_merge = pd.merge(data, data_st, on='BANK', how='outer',
                       indicator=True)
tx_nssf = outer_merge.query("_merge=='right_only'").reset_index(drop=True)
tx_nssf = tx_nssf[
    ['TRANS_SER_NUM', 'UNIT', 'TRANSACTIONDATE', 'TIMESTAMP', 'USERID', 'TRANSACTIONS', 'JUSTIFICATION', 'ENTRYAMOUNT', 'BALANCE',
     'DRCR', 'CHEQUENO', 'COMMENTS', '_merge']]

tx_nssf['bank_transaction_number'] = (pd.to_datetime(tx_nssf['TIMESTAMP'], dayfirst=True)).apply(
    lambda x: x.strftime('%Y%m%d')) + tx_nssf['TRANS_SER_NUM'].astype(int).astype(str)
tx_nssf['transaction_narration'] = tx_nssf['UNIT'].astype(int).astype(str) + (
    pd.to_datetime(tx_nssf['TIMESTAMP'], dayfirst=True)).apply(lambda x: x.strftime('%Y%m%d')) + tx_nssf['TRANS_SER_NUM'].astype(int).astype(str)+ tx_nssf['USERID']
tx_nssf['Date'] = (pd.to_datetime(tx_nssf['TIMESTAMP'], dayfirst=True)).apply(lambda x: x.strftime('%Y%m%d'))
nssf_eod = tx_nssf
nssf_eod['HEAD'] = 'D'
nssf_eod['ACCOUNT_NUMBER '] = ''
nssf_eod['CRDB'] = tx_nssf['bank_transaction_number'].apply(lambda x: x.strip())
nssf_eod['BANK'] = nssf_eod['COMMENTS'].str[:14].apply(lambda x: x.strip())
nssf_eod['SURNAME'] = (nssf_eod['ENTRYAMOUNT']).astype('int64')
nssf_eod["TO_CHAR(T.STATEMENT_DATE,'YYYYMMDD')"] = nssf_eod['DRCR']
nssf_eod["TO_CHAR(G.PREV_ACC_BALANCE,'999999999990.00')"] = nssf_eod['Date']
nssf_eod['CURRENCY_CODE'] = nssf_eod['Date']
# nssf_eod['NARATION'] = nssf_eod['transaction_narration'].apply(lambda x: x.strip())
# nssf_eod['NARATION']=nssf_eod['NARATION'].replace(' ','')

nssf_eod = nssf_eod[['HEAD', 'CRDB', 'BANK', 'ACCOUNT_NUMBER ', 'SURNAME', "TO_CHAR(T.STATEMENT_DATE,'YYYYMMDD')", "TO_CHAR(G.PREV_ACC_BALANCE,'999999999990.00')", 'CURRENCY_CODE']]

nssf_eod.rename(columns=lambda x: x.strip())
nssf_eod.columns
df1 = data.query("HEAD=='T'").reset_index(drop=True)
df2 = data.query("HEAD=='D'").reset_index(drop=True)
df3 = data.query("CRDB=='CRDB'").reset_index(drop=True)

# print(df2['SURNAME'])
union = pd.concat([df2, nssf_eod], ignore_index=True)
df1['CRDB'] = union[union.columns[1]].count()
df4 = data_st.tail(1)
df1['BANK'] = df4.iloc[0]['BALANCE']
df3 = df3.reset_index(drop=True)
nssf_eod = nssf_eod.reset_index(drop=True)
df2 = df2.reset_index(drop=True)
df1 = df1.reset_index(drop=True)
union1 = pd.concat([df3, df2, nssf_eod, df1], ignore_index=True)
union1 = union1.reset_index(drop=True)
# print(df2)
mxdate = ((pd.to_datetime(tx_nssf['TIMESTAMP'], dayfirst=True)).apply(lambda x: x.strftime("%d%m%Y"))).min()


# remove white spaces on a dataframe
def panda_strip(x):
    r = []
    for y in x:
        if isinstance(y, str):
            y = y.strip()

        r.append(y)
    return pd.Series(r)

union1 = union1.drop_duplicates()
union1 = union1.apply(lambda x: panda_strip(x))
union1.to_csv(f'NSSFCRDB{mxdate}.csv', index=False, header=False)
# Open the input file and a temporary output file
with open(f'NSSFCRDB{mxdate}.csv', 'r') as infile, open(f'NSSFCRDB.csv', 'w') as outfile:
    for line in infile:
        # Strip any trailing commas and write to the output file
        cleaned_line = line.rstrip(',\n')  # Remove trailing commas and newline
        outfile.write(cleaned_line + '\n')  # Add back the newline character

# sending mail
# sending mail
mail_from = "cente_reports@centenarybank.co.ug"
mail_to = ['mis@centenarybank.co.ug']
mail_cc = ['MIS@centenarybank.co.ug']
mail_subject = f"Centenary Bank EOD File for NSSF {mxdate}"
mail_body = f"Good Morning MIS,\nKindly receive NSSF EOD File under path below, pick it annd have it shared via ftp\n\n {path}"

mail_attachment = f'NSSFCRDB.csv'
mail_attachment_name = f'NSSFCRDB.csv'
mimemsg = MIMEMultipart()
mimemsg['From'] = mail_from
mimemsg['To'] = ','.join(mail_to)
mimemsg['CC'] = ','.join(mail_cc)
mimemsg['Subject'] = mail_subject
mimemsg.attach(MIMEText(mail_body, 'plain'))
with open(mail_attachment, "rb") as attachment:
    mimefile = MIMEBase('application', 'octet-stream')
    mimefile.set_payload((attachment).read())
    encoders.encode_base64(mimefile)
    mimefile.add_header('Content-Disposition', "attachment; filename= %s" % mail_attachment_name)
    mimemsg.attach(mimefile)
    s = smtplib.SMTP('10.222.140.233')
    s.send_message(mimemsg)
    s.quit()
del conn  # close the connection
