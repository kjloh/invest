#!/usr/bin/python -tt
from __future__ import print_function
import sys
import os
import csv
import sqlite3
import pandas as pd
import numpy as np

file_db = 'db/invest.db'

#
# Convert none to empty string
#
def xstr(s):
    if s is None:
        return ''
    return str(s)

#
# return None is error or no result
#
def query_db_one(conn, sql, values):
    try:
        c = conn.cursor()
        if values is None:
            c.execute(sql)
        else:
            c.execute(sql, values)
    except Exception, e:
        print(str(e))
        return None
    rows = c.fetchone()  
    return rows 

#
# return None is error or no result
#
def query_db_all(conn, sql, values):
    try:
        c = conn.cursor()
        if values is None:
            c.execute(sql)
        else:
            c.execute(sql, values)
    except Exception, e:
        print(str(e))
        return None
    rows = c.fetchall()  
    return rows 

#
# Return 0 if error, 1 if successful
#
def update_db(conn, sql, values):
    try:
        conn.execute(sql, values)
    except Exception, e:
        print(str(e))
        return 0
    return 1

#
# create an empty dataframe, then fill it with data in the correct rows
#
def create_df(df1):
    ROWS = 378    # number of rows of a complete data set

    num_cols = len(df1.columns)
    num_rows = len(df1.index)
    loans_is = False    # if loans of income statement encountered, toggle it to True

    df = pd.DataFrame(np.nan, index=range(0, ROWS), columns=range(0,num_cols))

    # df row order must be the same as the order in the database tables
    for i in range(0,num_rows):
        fieldname = df1.iloc[i][0].strip().lower()
        if fieldname == 'year end date' or fieldname == 'quarter end date':
            # quarter end date
            df.iloc[0] = df1.iloc[i]
        elif fieldname == 'date preliminary data loaded':
            df.iloc[1] = df1.iloc[i]
        elif fieldname == 'earnings period indicator':
            df.iloc[2] = df1.iloc[i]
        elif fieldname == 'quarterly indicator':
            df.iloc[3] = df1.iloc[i]
        elif fieldname == 'basic earnings indicator':
            df.iloc[4] = df1.iloc[i]
        elif fieldname == 'template indicator':
            df.iloc[5] = df1.iloc[i]
        elif 'preliminary full context ind' in fieldname:
            df.iloc[6] = df1.iloc[i]
        elif 'projected fiscal year' in fieldname:
            df.iloc[7] = df1.iloc[i]
        elif 'number of months last report' in fieldname:
            df.iloc[8] = df1.iloc[i]
        elif fieldname == 'operating revenue':
            df.iloc[9] = df1.iloc[i]
        elif fieldname == 'total revenue':
            df.iloc[10] = df1.iloc[i]
        elif fieldname == 'adjustments to revenue':
            df.iloc[11] = df1.iloc[i]
        elif fieldname == 'cost of sales':
            df.iloc[12] = df1.iloc[i]
        elif fieldname == 'cost of sales with depreciation':
            df.iloc[13] = df1.iloc[i]
        elif fieldname == 'gross margin':
            df.iloc[14] = df1.iloc[i]
        elif fieldname == 'gross operating profit':
            df.iloc[15] = df1.iloc[i]
        elif fieldname == 'Research & Development (R&D;) Expense'.lower():
            df.iloc[16] = df1.iloc[i]
        elif fieldname == 'Selling General & Administrative (SG&A;) Expense'.lower():
            df.iloc[17] = df1.iloc[i]
        elif fieldname == 'advertising':
            df.iloc[18] = df1.iloc[i]
        elif fieldname == 'operating income':
            df.iloc[19] = df1.iloc[i]
        elif fieldname == 'ebitda':
            df.iloc[20] = df1.iloc[i]
        elif fieldname == 'depreciation':
            df.iloc[21] = df1.iloc[i]
        elif fieldname == 'depreciation (unrecognized)':
            df.iloc[22] = df1.iloc[i]
        elif fieldname == 'amortization':
            df.iloc[23] = df1.iloc[i]
        elif fieldname == 'amortization of intangibles':
            df.iloc[24] = df1.iloc[i]
        elif fieldname == 'operating profit after depreciation':
            df.iloc[25] = df1.iloc[i]
        elif fieldname == 'interest income':
            df.iloc[26] = df1.iloc[i]
        elif fieldname == 'earnings from equity interest':
            df.iloc[27] = df1.iloc[i]
        elif fieldname == 'other income net':
            df.iloc[28] = df1.iloc[i]
        elif 'income acquired in process r&' in fieldname:
            df.iloc[29] = df1.iloc[i]
        elif fieldname == 'income restructuring and m&a;' or fieldname == 'incomerestructuring and m&a;':
            df.iloc[30] = df1.iloc[i]
        elif fieldname == 'other special charges':
            df.iloc[31] = df1.iloc[i]
        elif fieldname == 'special income charges' or fieldname == 'special income (charges)':
            df.iloc[32] = df1.iloc[i]
        elif fieldname == 'ebit':
            df.iloc[33] = df1.iloc[i]
        elif fieldname == 'interest expense':
            df.iloc[34] = df1.iloc[i]
        elif fieldname == 'pre-tax income ':
            df.iloc[35] = df1.iloc[i]
        elif fieldname == 'income taxes':
            df.iloc[36] = df1.iloc[i]
        elif fieldname == 'minority interest':
            df.iloc[37] = df1.iloc[i]
        elif fieldname == 'pref. securities of subsid. trust':
            df.iloc[38] = df1.iloc[i]
        elif fieldname == 'income before income taxes':
            df.iloc[39] = df1.iloc[i]
        elif fieldname == 'net income (continuing operations)' or fieldname == 'net income from continuing operations':
            df.iloc[40] = df1.iloc[i]
        elif fieldname == 'net income (discontinued operations)' or fieldname == 'net income from discontinued operations':
            df.iloc[41] = df1.iloc[i]
        elif fieldname == 'net income (total operations)' or fieldname == 'net income from total operations':
            df.iloc[42] = df1.iloc[i]
        elif fieldname == 'extraordinary income/losses' or fieldname == 'extraordinary income losses':
            df.iloc[43] = df1.iloc[i]
        elif fieldname == 'income from cum. effect of acct. change' or fieldname == 'income from cumumulative effect of accounting change':
            df.iloc[44] = df1.iloc[i]
        elif fieldname == 'income from tax loss carryforward':
            df.iloc[45] = df1.iloc[i]
        elif fieldname == 'other gains/losses' or fieldname == 'other gains (losses)':
            df.iloc[46] = df1.iloc[i]
        elif fieldname == 'total net income':
            df.iloc[47] = df1.iloc[i]
        elif fieldname == 'normalized income':
            df.iloc[48] = df1.iloc[i]
        elif fieldname == 'net income available for common':
            df.iloc[49] = df1.iloc[i]
        elif fieldname == 'preferred dividends':
            df.iloc[50] = df1.iloc[i]
        elif fieldname == 'excise taxes':
            df.iloc[51] = df1.iloc[i]
        elif fieldname == 'Basic EPS (Continuing)'.lower():
            df.iloc[52] = df1.iloc[i]
        elif fieldname == 'Basic EPS (Discontinued)'.lower():
            df.iloc[53] = df1.iloc[i]
        elif 'Basic EPS from Total Operations'.lower() in fieldname:
            df.iloc[54] = df1.iloc[i]
        elif 'Basic EPS (Extraordinary'.lower() in fieldname:
            df.iloc[55] = df1.iloc[i]
        elif fieldname == 'Basic EPS (Cum. Effect of Acct. Change)'.lower() or fieldname == 'Basic EPS (Cum. Effect of Acc. Change)'.lower():
            df.iloc[56] = df1.iloc[i]
        elif fieldname == 'Basic EPS (Tax Loss Carry Forward)'.lower():
            df.iloc[57] = df1.iloc[i]
        elif fieldname == 'Basic EPS (Other Gains/Losses)'.lower():
            df.iloc[58] = df1.iloc[i]
        elif fieldname == 'Basic EPS - Total'.lower():
            df.iloc[59] = df1.iloc[i]
        elif fieldname == 'Basic EPS - Normalized'.lower():
            df.iloc[60] = df1.iloc[i]
        elif fieldname == 'Diluted EPS (Continuing)'.lower():
            df.iloc[61] = df1.iloc[i]
        elif fieldname == 'Diluted EPS (Discontinued)'.lower():
            df.iloc[62] = df1.iloc[i]
        elif fieldname == 'Diluted EPS from Total Operations'.lower():
            df.iloc[63] = df1.iloc[i]
        elif fieldname == 'Diluted EPS (Extraordinary)'.lower():
            df.iloc[64] = df1.iloc[i]
        elif 'Diluted EPS (Cum. Effect of Acc'.lower() in fieldname:
            df.iloc[65] = df1.iloc[i]
        elif fieldname == 'Diluted EPS (Tax Loss Carry Forward)'.lower():
            df.iloc[66] = df1.iloc[i]
        elif fieldname == 'Diluted EPS (Other Gains/Losses)'.lower():
            df.iloc[67] = df1.iloc[i]
        elif fieldname == 'Diluted EPS - Total'.lower():
            df.iloc[68] = df1.iloc[i]
        elif fieldname == 'Diluted EPS - Normalized'.lower():
            df.iloc[69] = df1.iloc[i]
        elif 'dividends paid per share' in fieldname:
            df.iloc[70] = df1.iloc[i]
        elif fieldname == 'revenue (ytd)' or fieldname == 'revenues (ytd)':
            df.iloc[71] = df1.iloc[i]
        elif fieldname == 'Net Income from Total Operations (YTD)'.lower():
            df.iloc[72] = df1.iloc[i]
        elif fieldname == 'EPS from Total Operations (YTD)'.lower():
            df.iloc[73] = df1.iloc[i]
        elif fieldname == 'Dividends Paid Per Share (YTD)'.lower():
            df.iloc[74] = df1.iloc[i]
        elif fieldname == 'cash & equivalents':
            df.iloc[75] = df1.iloc[i]
        elif fieldname == 'restricted cash':
            df.iloc[76] = df1.iloc[i]
        elif fieldname == 'marketable securities':
            df.iloc[77] = df1.iloc[i]
        elif fieldname == 'accounts receivable':
            df.iloc[78] = df1.iloc[i]
        elif fieldname == 'loans receivable':
            df.iloc[79] = df1.iloc[i]
        elif 'other receivable' in fieldname:
            df.iloc[80] = df1.iloc[i]
        elif fieldname == 'receivables':
            df.iloc[81] = df1.iloc[i]
        elif fieldname == 'inventories raw materials':
            df.iloc[82] = df1.iloc[i]
        elif fieldname == 'inventories work in progress':
            df.iloc[83] = df1.iloc[i]
        elif fieldname == 'inventories purchased components':
            df.iloc[84] = df1.iloc[i]
        elif fieldname == 'inventories finished goods':
            df.iloc[85] = df1.iloc[i]
        elif fieldname == 'inventories other':
            df.iloc[86] = df1.iloc[i]
        elif fieldname == 'inventories adjustments & allowances':
            df.iloc[87] = df1.iloc[i]
        elif fieldname == 'inventories':
            df.iloc[88] = df1.iloc[i]
        elif fieldname == 'prepaid expenses':
            df.iloc[89] = df1.iloc[i]
        elif fieldname == 'current defered income taxes':
            df.iloc[90] = df1.iloc[i]
        elif fieldname == 'other current assets':
            df.iloc[91] = df1.iloc[i]
        elif fieldname == 'total current assets':
            df.iloc[92] = df1.iloc[i]
        elif fieldname == 'land and improvements':
            df.iloc[93] = df1.iloc[i]
        elif fieldname == 'building and improvements':
            df.iloc[94] = df1.iloc[i]
        elif fieldname == 'machinery furniture & equipment':
            df.iloc[95] = df1.iloc[i]
        elif fieldname == 'construction in progress':
            df.iloc[96] = df1.iloc[i]
        elif fieldname == 'other fixed assets':
            df.iloc[97] = df1.iloc[i]
        elif fieldname == 'total fixed assets':
            df.iloc[98] = df1.iloc[i]
        elif fieldname == 'gross fixed assets':
            df.iloc[99] = df1.iloc[i]
        elif fieldname == 'accumulated depreciation':
            df.iloc[100] = df1.iloc[i]
        elif fieldname == 'net fixed assets':
            df.iloc[101] = df1.iloc[i]
        elif fieldname == 'intangibles' or fieldname == 'intangible assets':
            df.iloc[102] = df1.iloc[i]
        elif fieldname == 'cost in excess':
            df.iloc[103] = df1.iloc[i]
        elif fieldname == 'non-current deferred income taxes':
            df.iloc[104] = df1.iloc[i]
        elif fieldname == 'other non-current assets':
            df.iloc[105] = df1.iloc[i]
        elif fieldname == 'total non-current assets':
            df.iloc[106] = df1.iloc[i]
        elif fieldname == 'total assets':
            df.iloc[107] = df1.iloc[i]
        elif fieldname == 'inventory valuation method':
            df.iloc[108] = df1.iloc[i]
        elif fieldname == 'accounts payable':
            df.iloc[109] = df1.iloc[i]
        elif fieldname == 'notes payable':
            df.iloc[110] = df1.iloc[i]
        elif fieldname == 'short-term debt' or fieldname == 'short -term debt':
            df.iloc[111] = df1.iloc[i]
        elif fieldname == 'accrued expenses':
            df.iloc[112] = df1.iloc[i]
        elif fieldname == 'accrued liabilities':
            df.iloc[113] = df1.iloc[i]
        elif fieldname == 'deferred revenues':
            df.iloc[114] = df1.iloc[i]
        elif fieldname == 'current deferred income taxes':
            df.iloc[115] = df1.iloc[i]
        elif fieldname == 'other current liabilities':
            df.iloc[116] = df1.iloc[i]
        elif fieldname == 'total current liabilities':
            df.iloc[117] = df1.iloc[i]
        elif fieldname == 'long-term debt':
            df.iloc[118] = df1.iloc[i]
        elif fieldname == 'capital lease obligations':
            df.iloc[119] = df1.iloc[i]
        elif fieldname == 'deferred income taxes':
            df.iloc[120] = df1.iloc[i]
        elif fieldname == 'other non-current liabilities':
            df.iloc[121] = df1.iloc[i]
        elif fieldname == 'minority interest liability':
            df.iloc[122] = df1.iloc[i]
        elif fieldname == 'preferred secur. of subsid. trust':
            df.iloc[123] = df1.iloc[i]
        elif fieldname == 'preferred equity outside stock equity':
            df.iloc[124] = df1.iloc[i]
        elif fieldname == 'total non-current liabilities':
            df.iloc[125] = df1.iloc[i]
        elif fieldname == 'total liabilities':
            df.iloc[126] = df1.iloc[i]
        elif fieldname == 'preferred stock equity':
            df.iloc[127] = df1.iloc[i]
        elif fieldname == 'common stock equity':
            df.iloc[128] = df1.iloc[i]
        elif fieldname == 'common par':
            df.iloc[129] = df1.iloc[i]
        elif fieldname == 'additional paid-in capital' or fieldname == 'additional paid in capital':
            df.iloc[130] = df1.iloc[i]
        elif 'cumulative translation adjustment' in fieldname :
            df.iloc[131] = df1.iloc[i]
        elif fieldname == 'retained earnings':
            df.iloc[132] = df1.iloc[i]
        elif fieldname == 'treasury stock':
            df.iloc[133] = df1.iloc[i]
        elif fieldname == 'other equity adjustments':
            df.iloc[134] = df1.iloc[i]
        elif fieldname == 'total capitalization':
            df.iloc[135] = df1.iloc[i]
        elif fieldname == 'total equity':
            df.iloc[136] = df1.iloc[i]
        elif fieldname == 'total liabilities & stock equity':
            df.iloc[137] = df1.iloc[i]
        elif fieldname == 'cash flow':
            df.iloc[138] = df1.iloc[i]
        elif fieldname == 'working capital':
            df.iloc[139] = df1.iloc[i]
        elif fieldname == 'free cash flow':
            df.iloc[140] = df1.iloc[i]
        elif fieldname == 'invested capital':
            df.iloc[141] = df1.iloc[i]
        elif fieldname == 'shares out (common class only)' or fieldname == 'shares outstanding common class only':
            df.iloc[142] = df1.iloc[i]
        elif fieldname == 'preferred shares':
            df.iloc[143] = df1.iloc[i]
        elif fieldname == 'total ordinary shares':
            df.iloc[144] = df1.iloc[i]
        elif 'total common shares out' in fieldname:
            df.iloc[145] = df1.iloc[i]
        elif fieldname == 'treasury shares':
            df.iloc[146] = df1.iloc[i]
        elif 'basic weighted shares' in fieldname:
            df.iloc[147] = df1.iloc[i]
        elif 'diluted weighted shares' in fieldname :
            df.iloc[148] = df1.iloc[i]
        elif fieldname == 'number of employees':
            df.iloc[149] = df1.iloc[i]
        elif fieldname == 'number of part-time employees':
            df.iloc[150] = df1.iloc[i]
        elif fieldname == 'net income/loss' or fieldname == 'net income earnings':
            df.iloc[151] = df1.iloc[i]
        elif fieldname == 'depreciation':
            df.iloc[152] = df1.iloc[i]
        elif fieldname == 'amortization':
            df.iloc[153] = df1.iloc[i]
        elif fieldname == 'amortization of intangibles':
            df.iloc[154] = df1.iloc[i]
        elif fieldname == 'deferred income taxes':
            df.iloc[155] = df1.iloc[i]
        elif fieldname == 'operating gains':
            df.iloc[156] = df1.iloc[i]
        elif fieldname == 'extraordinary gains':
            df.iloc[157] = df1.iloc[i]
        elif fieldname == '(increase) decrease in receivables' or fieldname == 'change in assets - receivables':
            df.iloc[158] = df1.iloc[i]
        elif fieldname == '(increase) decrease in inventories':
            df.iloc[159] = df1.iloc[i]
        elif fieldname == '(increase) decrease in prepaid expenses':
            df.iloc[160] = df1.iloc[i]
        elif fieldname == '(increase) decrease in other current assets':
            df.iloc[161] = df1.iloc[i]
        elif fieldname == 'decrease (increase) in payables' or fieldname == 'change in liabilities - payables':
            df.iloc[162] = df1.iloc[i]
        elif fieldname == 'decrease (increase) in other current liabilities':
            df.iloc[163] = df1.iloc[i]
        elif fieldname == 'decrease (increase) in other working capital':
            df.iloc[164] = df1.iloc[i]
        elif fieldname == 'other non-cash items':
            df.iloc[165] = df1.iloc[i]
        elif fieldname == 'net cash from continuing operations':
            df.iloc[166] = df1.iloc[i]
        elif fieldname == 'net cash from discontinued operations':
            df.iloc[167] = df1.iloc[i]
        elif fieldname == 'net cash from total operating activities' or fieldname == 'net cash from operating activities':
            df.iloc[168] = df1.iloc[i]
        elif fieldname == 'sale of property plant & equipment':
            df.iloc[169] = df1.iloc[i]
        elif fieldname == 'sale of long-term investments':
            df.iloc[170] = df1.iloc[i]
        elif fieldname == 'sale of short-term investments':
            df.iloc[171] = df1.iloc[i]
        elif 'purchase of property' in fieldname:
            df.iloc[172] = df1.iloc[i]
        elif fieldname == 'acquisitions':
            df.iloc[173] = df1.iloc[i]
        elif fieldname == 'purchase of long-term investments':
            df.iloc[174] = df1.iloc[i]
        elif fieldname == 'purchase of  short-term investments':
            df.iloc[175] = df1.iloc[i]
        elif fieldname == 'other investing changes net':
            df.iloc[176] = df1.iloc[i]
        elif fieldname == 'cash from discontinued investing activities':
            df.iloc[177] = df1.iloc[i]
        elif fieldname == 'net cash from investing activities':
            df.iloc[178] = df1.iloc[i]
        elif fieldname == 'issuance of debt':
            df.iloc[179] = df1.iloc[i]
        elif fieldname == 'issuance of capital stock':
            df.iloc[180] = df1.iloc[i]
        elif fieldname == 'repayment of long-term debt':
            df.iloc[181] = df1.iloc[i]
        elif fieldname == 'repurchase of capital stock':
            df.iloc[182] = df1.iloc[i]
        elif fieldname == 'payment of cash dividends':
            df.iloc[183] = df1.iloc[i]
        elif fieldname == 'other financing charges net':
            df.iloc[184] = df1.iloc[i]
        elif fieldname == 'cash from discontinued financing activities':
            df.iloc[185] = df1.iloc[i]
        elif fieldname == 'net cash from financing activities':
            df.iloc[186] = df1.iloc[i]
        elif fieldname == 'effect exchange rate changes':
            df.iloc[187] = df1.iloc[i]
        elif fieldname == 'net change in cash & equivalents':
            df.iloc[188] = df1.iloc[i]
        elif fieldname == 'cash at beginning of period':
            df.iloc[189] = df1.iloc[i]
        elif fieldname == 'cash end of period' or fieldname == 'cash at end of period':
            df.iloc[190] = df1.iloc[i]
        elif fieldname == 'foreign sales':
            df.iloc[191] = df1.iloc[i]
        elif fieldname == 'domestic sales':
            df.iloc[192] = df1.iloc[i]
        elif fieldname == 'auditor name' or fieldname == 'auditors name':
            df.iloc[193] = df1.iloc[i]
        elif fieldname == 'auditor report' or fieldname == 'auditors report':
            df.iloc[194] = df1.iloc[i]
        elif fieldname == 'Close PE Ratio'.lower():
            df.iloc[195] = df1.iloc[i]
        elif fieldname == 'High PE Ratio'.lower():
            df.iloc[196] = df1.iloc[i]
        elif fieldname == 'Low PE Ratio'.lower():
            df.iloc[197] = df1.iloc[i]
        elif fieldname == 'gross profit margin':
            df.iloc[198] = df1.iloc[i]
        elif fieldname == 'pre-tax profit margin':
            df.iloc[199] = df1.iloc[i]
        elif fieldname == 'post-tax profit margin':
            df.iloc[200] = df1.iloc[i]
        elif fieldname == 'net profit margin':
            df.iloc[201] = df1.iloc[i]
        elif fieldname == 'interest coverage (cont. operations)':
            df.iloc[202] = df1.iloc[i]
        elif fieldname == 'interest as % of invested capital':
            df.iloc[203] = df1.iloc[i]
        elif fieldname == 'effective tax rate':
            df.iloc[204] = df1.iloc[i]
        elif fieldname == 'income per employee':
            df.iloc[205] = df1.iloc[i]
        elif fieldname == 'Normalized Close PE Ratio'.lower():
            df.iloc[206] = df1.iloc[i]
        elif fieldname == 'Normalized High PE Ratio'.lower():
            df.iloc[207] = df1.iloc[i]
        elif fieldname == 'Normalized Low PE Ratio'.lower():
            df.iloc[208] = df1.iloc[i]
        elif fieldname == 'normalized net profit margin':
            df.iloc[209] = df1.iloc[i]
        elif fieldname == 'Normalized ROE'.lower():
            df.iloc[210] = df1.iloc[i]
        elif fieldname == 'Normalized ROA'.lower():
            df.iloc[211] = df1.iloc[i]
        elif fieldname == 'Normalized ROCI'.lower():
            df.iloc[212] = df1.iloc[i]
        elif fieldname == 'normalized income per employee':
            df.iloc[213] = df1.iloc[i]
        elif fieldname == 'quick ratio':
            df.iloc[214] = df1.iloc[i]
        elif fieldname == 'current ratio':
            df.iloc[215] = df1.iloc[i]
        elif fieldname == 'payout ratio':
            df.iloc[216] = df1.iloc[i]
        elif fieldname == 'total debt/equity ratio':
            df.iloc[217] = df1.iloc[i]
        elif fieldname == 'long-term debt/total capital ':
            df.iloc[218] = df1.iloc[i]
        elif fieldname == 'leverage ratio':
            df.iloc[219] = df1.iloc[i]
        elif fieldname == 'asset turnover':
            df.iloc[220] = df1.iloc[i]
        elif fieldname == 'cash as % of revenue':
            df.iloc[221] = df1.iloc[i]
        elif fieldname == 'receivables as % of revenue':
            df.iloc[222] = df1.iloc[i]
        elif fieldname == 'SG&A; as % of Revenue'.lower():
            df.iloc[223] = df1.iloc[i]
        elif fieldname == 'R&D; as % of Revenue'.lower():
            df.iloc[224] = df1.iloc[i]
        elif fieldname == 'revenue per $ cash':
            df.iloc[225] = df1.iloc[i]
        elif fieldname == 'revenue per $  plant (net)':
            df.iloc[226] = df1.iloc[i]
        elif fieldname == 'revenue per $ common equity':
            df.iloc[227] = df1.iloc[i]
        elif fieldname == 'revenue per $ invested capital':
            df.iloc[228] = df1.iloc[i]
        elif fieldname == 'receivables turnover':
            df.iloc[229] = df1.iloc[i]
        elif fieldname == 'inventory turnover':
            df.iloc[230] = df1.iloc[i]
        elif fieldname == 'receivables per day sales':
            df.iloc[231] = df1.iloc[i]
        elif fieldname == 'sales per $ receivables':
            df.iloc[232] = df1.iloc[i]
        elif fieldname == 'sales per $ inventory':
            df.iloc[233] = df1.iloc[i]
        elif fieldname == 'revenue/assets':
            df.iloc[234] = df1.iloc[i]
        elif fieldname == 'number of days cost of goods in inventory':
            df.iloc[235] = df1.iloc[i]
        elif fieldname == 'current assets per share':
            df.iloc[236] = df1.iloc[i]
        elif fieldname == 'total assets per share':
            df.iloc[237] = df1.iloc[i]
        elif fieldname == 'intangibles as % of book-value':
            df.iloc[238] = df1.iloc[i]
        elif fieldname == 'inventory as % of revenue':
            df.iloc[239] = df1.iloc[i]
        elif fieldname == 'long-term debt per share':
            df.iloc[240] = df1.iloc[i]
        elif fieldname == 'current liabilities per share':
            df.iloc[241] = df1.iloc[i]
        elif fieldname == 'cash per share':
            df.iloc[242] = df1.iloc[i]
        elif fieldname == 'LT-Debt to Equity Ratio'.lower():
            df.iloc[243] = df1.iloc[i]
        elif fieldname == 'LT-Debt as % of Invested Capital'.lower():
            df.iloc[244] = df1.iloc[i]
        elif fieldname == 'LT-Debt as % of Total Debt'.lower():
            df.iloc[245] = df1.iloc[i]
        elif fieldname == 'total debt as % total assets':
            df.iloc[246] = df1.iloc[i]
        elif fieldname == 'working captial as % of equity':
            df.iloc[247] = df1.iloc[i]
        elif fieldname == 'revenue per share':
            df.iloc[248] = df1.iloc[i]
        elif fieldname == 'book value per share':
            df.iloc[249] = df1.iloc[i]
        elif fieldname == 'tangible book value per share':
            df.iloc[250] = df1.iloc[i]
        elif fieldname == 'price/revenue ratio':
            df.iloc[251] = df1.iloc[i]
        elif fieldname == 'price/equity ratio':
            df.iloc[252] = df1.iloc[i]
        elif fieldname == 'price/tangible book ratio':
            df.iloc[253] = df1.iloc[i]
        elif fieldname == 'working capital as % of price':
            df.iloc[254] = df1.iloc[i]
        elif fieldname == 'working capital per share':
            df.iloc[255] = df1.iloc[i]
        elif fieldname == 'cash flow per share':
            df.iloc[256] = df1.iloc[i]
        elif fieldname == 'free cash flow per share':
            df.iloc[257] = df1.iloc[i]
        elif fieldname == 'Return on Stock Equity (ROE)'.lower():
            df.iloc[258] = df1.iloc[i]
        elif fieldname == 'Return on Capital Invested (ROCI)'.lower():
            df.iloc[259] = df1.iloc[i]
        elif fieldname == 'Return on Assets (ROA)'.lower():
            df.iloc[260] = df1.iloc[i]
        elif fieldname == 'price/cash flow ratio':
            df.iloc[261] = df1.iloc[i]
        elif fieldname == 'price/free cash flow ratio':
            df.iloc[262] = df1.iloc[i]
        elif fieldname == 'sales per employee':
            df.iloc[263] = df1.iloc[i]
        elif fieldname == '% of sales-to-industry':
            df.iloc[264] = df1.iloc[i]
        elif fieldname == '% of earnings-to-industry':
            df.iloc[265] = df1.iloc[i]
        elif fieldname == '% of EPS-to-Industry'.lower():
            df.iloc[266] = df1.iloc[i]
        elif fieldname == '% of price-to-industry':
            df.iloc[267] = df1.iloc[i]
        elif fieldname == '% of PE-to-Industry'.lower():
            df.iloc[268] = df1.iloc[i]
        elif fieldname == '% of price/book-to-industry':
            df.iloc[269] = df1.iloc[i]
        elif fieldname == '% of price/sales-to-industry':
            df.iloc[270] = df1.iloc[i]
        elif fieldname == '% of price/cashflow-to-industry':
            df.iloc[271] = df1.iloc[i]
        elif fieldname == '% of pric/free cashlow-to-industry':
            df.iloc[272] = df1.iloc[i]
        elif fieldname == '% of debt/equity-to-industry':
            df.iloc[273] = df1.iloc[i]
        elif fieldname == '% of current ratio-to-industry':
            df.iloc[274] = df1.iloc[i]
        elif fieldname == '% of gross profit margin-to-industry':
            df.iloc[275] = df1.iloc[i]
        elif fieldname == '% of pre-tax profit margin-to-industry':
            df.iloc[276] = df1.iloc[i]
        elif fieldname == '% of post-tax profit margin-to-industry':
            df.iloc[277] = df1.iloc[i]
        elif fieldname == '% of net profit margin-to-industry':
            df.iloc[278] = df1.iloc[i]
        elif fieldname == '% of ROE-to-Industry'.lower():
            df.iloc[279] = df1.iloc[i]
        elif fieldname == '% of leverage-to-industry':
            df.iloc[280] = df1.iloc[i]
        elif fieldname == 'format indicator':
            df.iloc[281] = df1.iloc[i]
        elif fieldname == 'loans':
            # check if 'Loans' is income statement item.
            # This usually comes before 'Loans' of Balance Sheet.
            if loans_is == False:
                df.iloc[282] = df1.iloc[i]
                loans_is = True
        elif fieldname == 'investment securities':
            df.iloc[283] = df1.iloc[i]
        elif fieldname == 'lease financing income':
            df.iloc[284] = df1.iloc[i]
        elif fieldname == 'other interest income':
            df.iloc[285] = df1.iloc[i]
        elif fieldname == 'federal funds sold (purchased)':
            df.iloc[286] = df1.iloc[i]
        elif fieldname == 'interest bearing deposits':
            df.iloc[287] = df1.iloc[i]
        elif fieldname == 'loans held for resale':
            df.iloc[288] = df1.iloc[i]
        elif fieldname == 'trading account securities':
            df.iloc[289] = df1.iloc[i]
        elif fieldname == 'time deposits placed':
            df.iloc[290] = df1.iloc[i]
        elif fieldname == 'other money market investments':
            df.iloc[291] = df1.iloc[i]
        elif fieldname == 'total money market investments':
            df.iloc[292] = df1.iloc[i]
        elif fieldname == 'total interest income':
            df.iloc[293] = df1.iloc[i]
        elif fieldname == 'deposits':
            df.iloc[294] = df1.iloc[i]
        elif fieldname == 'short-term deposits':
            df.iloc[295] = df1.iloc[i]
        elif fieldname == 'long-term deposits':
            df.iloc[296] = df1.iloc[i]
        elif fieldname == 'federal funds purchased (securities sold)':
            df.iloc[297] = df1.iloc[i]
        elif fieldname == 'capitalized lease obligations':
            df.iloc[298] = df1.iloc[i]
        elif fieldname == 'other interest expense':
            df.iloc[299] = df1.iloc[i]
        elif fieldname == 'total interest expense':
            df.iloc[300] = df1.iloc[i]
        elif fieldname == 'net interest income (expense)':
            df.iloc[301] = df1.iloc[i]
        elif fieldname == 'provision for loan loss':
            df.iloc[302] = df1.iloc[i]
        elif fieldname == 'trust fees by commissions':
            df.iloc[303] = df1.iloc[i]
        elif fieldname == 'service charge on deposit accounts':
            df.iloc[304] = df1.iloc[i]
        elif fieldname == 'other service charges':
            df.iloc[305] = df1.iloc[i]
        elif fieldname == 'security transactions':
            df.iloc[306] = df1.iloc[i]
        elif fieldname == 'premiums earned':
            df.iloc[307] = df1.iloc[i]
        elif fieldname == 'net realized capital gains':
            df.iloc[308] = df1.iloc[i]
        elif fieldname == 'investment banking profit':
            df.iloc[309] = df1.iloc[i]
        elif fieldname == 'other non-interest income':
            df.iloc[310] = df1.iloc[i]
        elif fieldname == 'total non-interest income':
            df.iloc[311] = df1.iloc[i]
        elif fieldname == 'salaries and employee benefits':
            df.iloc[312] = df1.iloc[i]
        elif fieldname == 'net occupancy expense':
            df.iloc[313] = df1.iloc[i]
        elif fieldname == 'promotions and advertising':
            df.iloc[314] = df1.iloc[i]
        elif fieldname == 'property liability insurance claims':
            df.iloc[315] = df1.iloc[i]
        elif fieldname == 'policy acquisition costs':
            df.iloc[316] = df1.iloc[i]
        elif fieldname == 'amortization deferred policy acquisition cost':
            df.iloc[317] = df1.iloc[i]
        elif fieldname == 'current and future benefits':
            df.iloc[318] = df1.iloc[i]
        elif fieldname == 'other non-interest expense':
            df.iloc[319] = df1.iloc[i]
        elif fieldname == 'total non-interest expense':
            df.iloc[320] = df1.iloc[i]
        elif fieldname == 'premium tax credit':
            df.iloc[321] = df1.iloc[i]
        elif fieldname == 'Diluted EPS from Total Operations (YTD)'.lower():
            df.iloc[322] = df1.iloc[i]
        elif fieldname == 'cash and due from banks':
            df.iloc[323] = df1.iloc[i]
        elif fieldname == 'federal funds sold (securities purchased)':
            df.iloc[324] = df1.iloc[i]
        elif fieldname == 'interest bearing deposits at other banks':
            df.iloc[325] = df1.iloc[i]
        elif fieldname == 'investment securities net':
            df.iloc[326] = df1.iloc[i]
        elif fieldname == 'loans':
            if loans_is == True:
                df.iloc[327] = df1.iloc[i]
        elif fieldname == 'unearnedp remiums':
            df.iloc[328] = df1.iloc[i]
        elif fieldname == 'allowance for loans and lease losses':
            df.iloc[329] = df1.iloc[i]
        elif fieldname == 'net loans':
            df.iloc[330] = df1.iloc[i]
        elif fieldname == 'premises & equipment':
            df.iloc[331] = df1.iloc[i]
        elif fieldname == 'due from customers acceptance':
            df.iloc[332] = df1.iloc[i]
        elif fieldname == 'trading account securities':
            df.iloc[333] = df1.iloc[i]
        elif fieldname == 'accrued interest':
            df.iloc[334] = df1.iloc[i]
        elif fieldname == 'deferred acquisition cost':
            df.iloc[335] = df1.iloc[i]
        elif fieldname == 'accrued investment income':
            df.iloc[336] = df1.iloc[i]
        elif fieldname == 'separate account business':
            df.iloc[337] = df1.iloc[i]
        elif fieldname == 'time deposits placed':
            df.iloc[338] = df1.iloc[i]
        elif fieldname == 'intangible assets':
            df.iloc[339] = df1.iloc[i]
        elif fieldname == 'other assets':
            df.iloc[340] = df1.iloc[i]
        elif fieldname == 'non-interest bearing deposits':
            df.iloc[341] = df1.iloc[i]
        elif fieldname == 'interest bearing deposits':
            df.iloc[342] = df1.iloc[i]
        elif fieldname == 'bankers acceptance outstanding':
            df.iloc[343] = df1.iloc[i]
        elif fieldname == 'federal funds purchased (securities sold)':
            df.iloc[344] = df1.iloc[i]
        elif fieldname == 'accrued taxes':
            df.iloc[345] = df1.iloc[i]
        elif fieldname == 'accrued interest payables':
            df.iloc[346] = df1.iloc[i]
        elif fieldname == 'other payables':
            df.iloc[347] = df1.iloc[i]
        elif fieldname == 'claims and claim expense':
            df.iloc[348] = df1.iloc[i]
        elif fieldname == 'future policy benefits':
            df.iloc[349] = df1.iloc[i]
        elif fieldname == 'unearned premiums':
            df.iloc[350] = df1.iloc[i]
        elif fieldname == 'policy holder funds':
            df.iloc[351] = df1.iloc[i]
        elif fieldname == 'participating policy holder equity':
            df.iloc[352] = df1.iloc[i]
        elif fieldname == 'separate accounts business':
            df.iloc[353] = df1.iloc[i]
        elif fieldname == 'foreign currency adjustments':
            df.iloc[354] = df1.iloc[i]
        elif fieldname == 'net unrealized loss (gain) on investments':
            df.iloc[355] = df1.iloc[i]
        elif fieldname == 'net unrealized loss (gain) on foreign currency':
            df.iloc[356] = df1.iloc[i]
        elif fieldname == 'net other unearned losses (gains)':
            df.iloc[357] = df1.iloc[i]
        elif fieldname == 'provision for loan losses':
            df.iloc[358] = df1.iloc[i]
        elif fieldname == 'depreciation and amortization':
            df.iloc[359] = df1.iloc[i]
        elif fieldname == 'investment securities gain':
            df.iloc[360] = df1.iloc[i]
        elif fieldname == 'net policy acquisition costs':
            df.iloc[361] = df1.iloc[i]
        elif fieldname == 'realized investment gains':
            df.iloc[362] = df1.iloc[i]
        elif fieldname == 'net premiums receivables':
            df.iloc[363] = df1.iloc[i]
        elif fieldname == 'change in income taxes':
            df.iloc[364] = df1.iloc[i]
        elif fieldname == 'proceeds from sale - material investment':
            df.iloc[365] = df1.iloc[i]
        elif fieldname == 'purchase of investment securities':
            df.iloc[366] = df1.iloc[i]
        elif fieldname == 'net increase federal funds sold':
            df.iloc[367] = df1.iloc[i]
        elif fieldname == 'net change in deposits':
            df.iloc[368] = df1.iloc[i]
        elif fieldname == 'cash dividends paid':
            df.iloc[369] = df1.iloc[i]
        elif fieldname == 'change of short-term debt':
            df.iloc[370] = df1.iloc[i]
        elif fieldname == 'issuance of long-term debt':
            df.iloc[371] = df1.iloc[i]
        elif fieldname == 'issuance of preferred stock':
            df.iloc[372] = df1.iloc[i]
        elif fieldname == 'issuance of common stock':
            df.iloc[373] = df1.iloc[i]
        elif fieldname == 'purchase of treasury stock':
            df.iloc[374] = df1.iloc[i]
        elif fieldname == 'other financing activities':
            df.iloc[375] = df1.iloc[i]
        elif fieldname == 'effect of exchange rate changes':
            df.iloc[376] = df1.iloc[i]
        elif fieldname == 'total risk-based capital ratio':
            df.iloc[377] = df1.iloc[i]

    # replace NaN with empty string else SQL will fail
    df = df.replace(np.nan, '', regex=True)

    return df


#
# Parse CSV and extract data we want
#
# Complete CSV has 281 lines.  Some tickers have less.  Need to fill in empty values.
#
def parse_csv(csv_file, ticker, conn, table):

    print('Processing: ' + csv_file + ' - ', end='')
    try:
        df1 = pd.read_csv(csv_file, header=None)
    except:
        print('ERROR: Unable to open file: ' + csv_file)
        return

    num_cols = len(df1.columns)

    df = create_df(df1)

    for i in range(1, num_cols):
        c = [x.replace(',', '') for x in df[i]]  # remove comma from numbers
        if c[0] == '' or not c[0]:
            continue
        [year, month] = c[0].split('/')

        # if record exist, skip 

        sql1 = "select * from %s where ticker='%s' and year=%s and month=%s" % (table, ticker[0] , year, month)
        data = query_db_all(conn, sql1, None)
        if not(data is None) and len(data) > 0:
            print('x', end='')
            continue

        print('+', end='')
        sql = ('insert into %s values ('
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?) ') % (table)
        values = tuple([ticker[0] , year, month]) + tuple(c)
        update_db(conn, sql, values)
    print('')
    return

#
# Fetch financial data from websites
# arguments:
#        tickers - list of tickers in tuple format [TICKER, EXCHANGE, COMPANY NAME]
#        type    - 
#        conn    - database connection
#
def import_findata(ticker, type, conn):
    if len(ticker) < 3:
        return

    print('Ticker: ' + ticker[0])

    if type == 1:
        table = 'fin_annual'
    else:
        table = 'fin_quarter'

    file_str = ticker[0] + '_' + ticker[1] + '_'
    files = os.listdir('.')   # get list of files in current directory
    for file in files:
        if file.startswith(file_str) == True:
            parse_csv(file, ticker, conn, table)

    file_str = ticker[0] + '_' + ticker[1] + '.csv'
    files = os.listdir('.')   # get list of files in current directory
    for file in files:
        if file == file_str:
            parse_csv(file, ticker, conn, table)
    
    return

#
# Annualized by summing the latest 4 quaterly data.
#
def calc_annualized(ticker, conn):
    if len(ticker) < 3:
        return

    print("Calculating annualized data:", ticker[0])
    #
    # annualized income statement items
    #
    print("Calculating income statement items...")
    sql = ('select '
        'sum(is001), sum(is002), sum(is003), sum(is004), sum(is005), sum(is006), sum(is007), sum(is008), sum(is009), sum(is010), '
        'sum(is011), sum(is012), sum(is013), sum(is014), sum(is015), sum(is016), sum(is017), sum(is018), sum(is019), sum(is020), '
        'sum(is021), sum(is022), sum(is023), sum(is024), sum(is025), sum(is026), sum(is027), sum(is028), sum(is029), sum(is030), '
        'sum(is031), sum(is032), sum(is033), sum(is034), sum(is035), sum(is036), sum(is037), sum(is038), sum(is039), sum(is040), '
        'sum(is041), sum(is042), sum(is043), sum(is044), sum(is045), sum(is046), sum(is047), sum(is048), sum(is049), sum(is050), '
        'sum(is051), sum(is052), sum(is053), sum(is054), sum(is055), sum(is056), sum(is057), sum(is058), sum(is059), sum(is060), '
        'sum(is061), sum(is062), sum(is063) '
        'from (select * from fin_quarter where ticker = ? order by year desc, month desc limit 4)')
    is_sum = query_db_one(conn, sql, tuple([ticker[0]]))  
    if (is_sum is None):
        return

    #
    # annualized cash flow statement items
    #
    print("Calculating cash flow statement items...")
    sql = ('select '
        'sum(cf001), sum(cf002), sum(cf003), sum(cf004), sum(cf005), sum(cf006), sum(cf007), sum(cf008), sum(cf009), sum(cf010), '
        'sum(cf011), sum(cf012), sum(cf013), sum(cf014), sum(cf015), sum(cf016), sum(cf017), sum(cf018), sum(cf019), sum(cf020), '
        'sum(cf021), sum(cf022), sum(cf023), sum(cf024), sum(cf025), sum(cf026), sum(cf027), sum(cf028), sum(cf029), sum(cf030), '
        'sum(cf031), sum(cf032), sum(cf033), sum(cf034), sum(cf035), sum(cf036) '
        'from (select * from fin_quarter where ticker = ? order by year desc, month desc limit 4)')
    cf_sum = query_db_one(conn, sql, tuple([ticker[0]])) 
    if cf_sum is None:
        return

    #
    # annualized ROx items
    #
    print("Calculating return on x items...")
    sql = ('select sum(r016), sum(r017), sum(r018), sum(r064), sum(r065), sum(r066) '
        'from (select * from fin_quarter where ticker = ? order by year desc, month desc limit 4)')
    r_sum = query_db_one(conn, sql, tuple([ticker[0]]))  
    if r_sum is None:
        return

    print("Calculating annualized data for financials' items:", ticker[0])
    #
    # annualized extra items for financial
    #
    print("Calculating income statement items...")
    sql = ('select '
        'sum(f002), sum(f003), sum(f004), sum(f005), sum(f006), sum(f007), sum(f008), sum(f009), sum(f010), '
        'sum(f011), sum(f012), sum(f013), sum(f014), sum(f015), sum(f016), sum(f017), sum(f018), sum(f019), sum(f020), '
        'sum(f021), sum(f022), sum(f023), sum(f024), sum(f025), sum(f026), sum(f027), sum(f028), sum(f029), sum(f030), '
        'sum(f031), sum(f032), sum(f033), sum(f034), sum(f035), sum(f036), sum(f037), sum(f038), sum(f039), sum(f040), '
        'sum(f041), sum(f042), '
        'sum(f078), sum(f079), sum(f080), sum(f081), sum(f082), sum(f083), sum(f084), sum(f085), '
        'sum(f086), sum(f087), sum(f088), sum(f089), sum(f090), sum(f091), sum(f092), sum(f093), sum(f094), sum(f095), '
        'sum(f096), sum(f097) '
        'from (select * from fin_quarter where ticker = ? order by year desc, month desc limit 4)')
    f_sum = query_db_one(conn, sql, tuple([ticker[0]]))  
    if (f_sum is None):
        return

    print("Getting latest quarterly report...")
    sql = 'select * from fin_quarter where ticker = ? order by year desc, month desc limit 1'
    row = query_db_one(conn, sql, tuple([ticker[0]]))  # all items in the latest qaurterly statement
    if row is None:
        return
    year = row[1]   # latest quarterly report year
    month = row[2]  # latest quarterly report month

    print("Getting annualized report...")
    sql = 'select * from fin_annualized where ticker=? and year=? and month=? order by year desc, month desc limit 1'
    values = tuple([ticker[0], year, month])
    row2 = query_db_one(conn, sql, values)  # all items in the latest annualized statement
    if row2 is None:
        # record not found in annualized table, create it
        print("Create annualized report")
        sql = ('insert into fin_annualized values ('
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '
                '?, ?, ?, ?) ') 
        if update_db(conn, sql, tuple(row)) == 0:
            return
    
    # Update income statement
    print("Updating income statement items...")
    sql = ('update fin_annualized set '
            'is001=?, is002=?, is003=?, is004=?, is005=?, is006=?, is007=?, is008=?, is009=?, is010=?, '
            'is011=?, is012=?, is013=?, is014=?, is015=?, is016=?, is017=?, is018=?, is019=?, is020=?, '
            'is021=?, is022=?, is023=?, is024=?, is025=?, is026=?, is027=?, is028=?, is029=?, is030=?, '
            'is031=?, is032=?, is033=?, is034=?, is035=?, is036=?, is037=?, is038=?, is039=?, is040=?, '
            'is041=?, is042=?, is043=?, is044=?, is045=?, is046=?, is047=?, is048=?, is049=?, is050=?, '
            'is051=?, is052=?, is053=?, is054=?, is055=?, is056=?, is057=?, is058=?, is059=?, is060=?, '
            'is061=?, is062=?, is063=? '
            'where ticker=? and year=? and month=?') 
    values = tuple(is_sum) + tuple([ticker[0], year, month])
    if update_db(conn, sql, values) == 0:
        return

    # Update cash flow statement
    print("Updating cash flow items...")
    sql = ('update fin_annualized set '
            'cf001=?, cf002=?, cf003=?, cf004=?, cf005=?, cf006=?, cf007=?, cf008=?, cf009=?, cf010=?, '
            'cf011=?, cf012=?, cf013=?, cf014=?, cf015=?, cf016=?, cf017=?, cf018=?, cf019=?, cf020=?, '
            'cf021=?, cf022=?, cf023=?, cf024=?, cf025=?, cf026=?, cf027=?, cf028=?, cf029=?, cf030=?, '
            'cf031=?, cf032=?, cf033=?, cf034=?, cf035=?, cf036=? '
            'where ticker=? and year=? and month=?') 
    values = tuple(cf_sum) + tuple([ticker[0], year, month])
    if update_db(conn, sql, values) == 0:
        return

    # Update return on x ratios 
    print("Updating return on x items...")
    sql = ('update fin_annualized set '
            'r016=?, r017=?, r018=?, r064=?, r065=?, r066=? '
            'where ticker=? and year=? and month=?') 
    values = tuple(r_sum) + tuple([ticker[0], year, month])
    if update_db(conn, sql, values) == 0:
        return

    # Update extra items for financials 
    print("Updating items for financials...")
    sql = ('update fin_annualized set '
            'f002=?, f003=?, f004=?, f005=?, f006=?, f007=?, f008=?, f009=?, f010=?, '
            'f011=?, f012=?, f013=?, f014=?, f015=?, f016=?, f017=?, f018=?, f019=?, f020=?, '
            'f021=?, f022=?, f023=?, f024=?, f025=?, f026=?, f027=?, f028=?, f029=?, f030=?, '
            'f031=?, f032=?, f033=?, f034=?, f035=?, f036=?, f037=?, f038=?, f039=?, f040=?, '
            'f041=?, f042=?, '
            'f078=?, f079=?, f080=?, f081=?, f082=?, f083=?, f084=?, f085=?, '
            'f086=?, f087=?, f088=?, f089=?, f090=?, f091=?, f092=?, f093=?, f094=?, f095=?, '
            'f096=?, f097=? '
            'where ticker=? and year=? and month=?') 
    values = tuple(f_sum) + tuple([ticker[0], year, month])
    if update_db(conn, sql, values) == 0:
        return

    # Net profit margin
    if is_sum[0] == 0 or is_sum[0] == '':
        npm = 0
    else:
        npm = is_sum[39]/is_sum[0]

    # current rat''
    if row[120] == 0 or row[120] == '':
        curr_ratio = 0
    else:
        curr_ratio = row[95] / row[120]

    # debt equity ratio
    if row[139] == 0 or row[139] == '':
        der = 0
    else:
        der = (row[114] + row[121]) / row[139]

    # Update the calculated ratios
    sql = ('update fin_annualized set '
            'r007=?, r021=?, r023=? '
            'where ticker=? and year=? and month=?') 
    values = tuple([npm, curr_ratio, der]) + tuple([ticker[0], year, month])
    if update_db(conn, sql, values) == 0:
        return
    return

#
# Import tickers list data into companies table
#
def import_comp(tickers, conn):
    for ticker in tickers:
        if len(ticker) < 3:
            continue
        print("Create company info", ticker)
        sql = 'insert into companies (ticker, exchange, comp_name, status) values (?, ?, ?, 1)'
        values = tuple(ticker)
        if update_db(conn, sql, values) == 0:
            print("Update company info", ticker)
            sql = 'update companies set exchange=?, comp_name=?, status=1 where ticker=?'
            values = tuple([ticker[1], ticker[2]]) + tuple([ticker[0]])
            update_db(conn, sql, values)

    try:
        conn.commit()
    except Exception, e:
        print(str(e))

    return

#
# print(usage info
#
def print_usage(name):
    print('Synopsis:')
    print('Import data from CSV to database.')
    print('Usage:')
    print(name, '<filename>')
    return

#
# Args:
#       filename: file contains list of exchange and tickers
#
def main():
    if len(sys.argv) < 2:
        print_usage(sys.argv[0])
        return

    file_in = sys.argv[1]

    try:
        f = open(file_in, "rU")
    except:
        sys.exit('ERROR: Unable to open ' + file_in)

    tickers = f.readlines()
    tickers = [x.strip().split(',') for x in tickers]
    f.close()

    try:
        conn = sqlite3.connect(file_db)
    except:
        print('ERROR: unable to open database')

    import_comp(tickers, conn)

    os.chdir("data")
    for t in tickers:
        if len(t) < 3:
            continue
        import_findata(t, 1, conn)
    try:
        conn.commit()
    except Exception, e:
        print(str(e))

    os.chdir("../data_quarter")
    for t in tickers:
        if len(t) < 3:
            continue
        import_findata(t, 2, conn)
    try:
        conn.commit()
    except Exception, e:
        print(str(e))

    for t in tickers:
        if len(t) < 3:
            continue
        calc_annualized(t, conn)
    try:
        conn.commit()
    except Exception, e:
        print(str(e))
    conn.close()

    return

if __name__ == '__main__':
    main()
