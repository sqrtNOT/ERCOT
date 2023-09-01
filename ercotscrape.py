#!/usr/bin/python3.9
from requests import get
import re

url = "http://mis.ercot.com/misapp/GetReports.do?reportTypeId=203"
filepath = "/ERCOT/ercotfiles"
# 2 step regex find table rows then extract text
# single rule works but if a dl link is missing it will break the associations
breaker = re.compile(r"""<tr>.*?</tr>""")
matcher = re.compile(r"""(ext\.[\w.]+?zip).+?(misdownload.*?)\'""")
r = get(url)
rows = breaker.findall(r.text)
results = []
# rows of the html table
for row in rows:
    _ = matcher.findall(row)
    if _:
        results.append([_[0][0], _[0][1]])
# local list of used files
downloaded = []
for line in open(filepath, "r"):
    downloaded.append(line.strip("\n"))
# new files
needtodl = []
for result in results:
    if result[0] not in downloaded:
        result[1] = "http://mis.ercot.com/" + result[1]
        needtodl.append(result)
if not needtodl:
    raise SystemExit
    # nothing new so no need to continue


# past here only gets run if we have new files to download
for tup in needtodl:
    print(tup)

utilities = {
    "AEP_CENTRAL": 39,
    "AEP_NORTH": 39,
    "CENTERPOINT": 40,
    "NUECES": 41,
    "ONCOR": 42,
    "SWEPCO": 63,
    "TNMP": 43,
}
utilrule = re.compile("(AEP_CENTRAL|AEP_NORTH|CENTERPOINT|NUECES|ONCOR|SWEPCO|TNMP)")
import zipfile
import io
import mysql.connector
import pandas as pd
password = ""

URI = "mysql+mysqlconnector://{user}:{password}@{server}/{database}".format(
    user="ercotloader", password=password, server="localhost", database="ercot"
)
conn = mysql.connector.connect(
    user="ercotloader", password=password, host="localhost", database="ercot"
)
cursor = conn.cursor(prepared=True)
insertrow = "REPLACE INTO ercot_esiid values(" + "%s," * 20 + "%s)"
column_names = [
    "esiid",
    "address",
    "address_overflow",
    "city",
    "state",
    "zipcode",
    "duns",
    "read_cycle",
    "status",
    "premise_type",
    "power_region",
    "stationcode",
    "stationname",
    "metered",
    "service_orders",
    "polr_class",
    "settlement_ams",
    "tdsp_ams",
    "switch_hold",
]
for file in needtodl:
    url = file[1]
    filereq = get(url.strip("'"))
    if not filereq.ok:
        # problem with request aborting
        raise SystemExit
    zip = zipfile.ZipFile(io.BytesIO(filereq.content))
    try:
        utility = utilities[utilrule.search(file[0])[0]]
    except:
        # utility not found in filename
        utility = None
    # write the original zip file to disk here if needed
    if len(zip.namelist()) != 1:
        # more than one file in the archive -  unsupported
        continue
    data = pd.read_csv(
        zip.open(zip.namelist()[0]), keep_default_na=False, names=column_names
    )
    # handle whitespace fixes here
    # add utility id column
    data["utility_id"] = [utility] * len(data)
    # add market column
    data["market_id"] = [None] * len(data)
    print(len(data))
    # potentially branch on FUL vs DAILY if pre-delete is deemed faster. Don't truncate
    # also potential for scheduling FUL as low priority if it's meant to be run during business hours
    # fairly slow for FUL files, consider writing to server then using LOAD DATA INFILE instead
    progress = 0
    print(file)
    for row in data.itertuples(index=False, name=None):
        cursor.execute(insertrow, row)
        progress = progress + 1
        if progress % 1000 == 5:
            print(progress)
    conn.commit()
    with open(filepath, "a") as outfile:
        for filename, _ in needtodl:
            outfile.write(filename + "\n")
conn.close()
