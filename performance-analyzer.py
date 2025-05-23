"""
    App Name: Performance Analyzer CCM 2.0
    version: v24.2
"""

# python Performance_Analyzer.py "https://myfastretailpreprod.o9solutions.com" "2024-06-25" "2024-06-27" "FastRetail" "FRSSO2-Pre Prod" "FR-Pre Prod" "5205" "etluser@FRSSO2.com" "Welcome@123"

import json
import pandas as pd
from pandas import read_csv, DataFrame, to_datetime, options, concat
from collections import defaultdict
from re import sub
from dateutil import tz
from time import strftime, gmtime, time
from requests import post, get, exceptions, packages
from json import load
from sys import argv
from datetime import datetime
import re
import csv
import os


initTime = time()

options.mode.chained_assignment = None
# HEADERS
MESSAGE = "Message"
SERVER = "Server"
LOGGER = "Logger"
RID = "RId"
SID = "SId"
USER_ID = "LUId"
TIMESTAMP = "Timestamp"
LEVEL = "Level"
THREAD = "Thread"
DAY = "Day"

SCS_PLAN_CONFIG = "o9.GraphCube.Plugins.SupplyChainSolver.plan.PlanConfig"
SCS_PLAN_CACHE = "o9.GraphCube.Plugins.SupplyChainSolver.plan.PlanCache"
SCS_UTIL = "o9.GraphCube.util.Utilities"
PLUG = "o9.GraphCube.Plugins.AbstractPlugin"
SCS_PLAN = "o9.GraphCube.Plugins.SupplyChainSolver.plan.Plan"
_LOGGER = "o9_logger"

READ_TIME = "Read Time"
PROCESSING_TIME = "Processing time"
WRITE_TIME = "Write Time"
PLUGIN_NAME = "Plugin Name"
QUERY = "Query"
START_TIME = "StartTime"
END_TIME = "EndTime"
VERSION_SAVE = "VersionToSave"
NUM_EXECUTORS = "NumberOfSaveExecutedForDay"
DUR_IN_MINUTES = "DurationInMinutes"
DUR_MM_SS = "DurationMMSS"
INVOCATIONS = "Invocations"
EXECUTIONS = "Executions"
NUM_OPS = "Number Of Non Null Ops"
PLUGIN_RUN_DIVISION = "Plugin Run Division"
PLUGIN_RUN_DURATION = "Plugin Run Duration"

Records = "Records"
Value = "Value"
AGG_VAL = False
Category = "Category"
Solver_Parameter = False
Format = "%Y-%m-%d"
max_characters = 4000

consolidatedOutputLog = pd.DataFrame()


def processLogFile(df):
    """
    Processes a log file to extract unique plugin-related messages.

    Parameters:
        inputFile (str): Path to the input CSV file containing logs.
        outputFile (str): Path to save the processed CSV file.

    Returns:
        None: Saves the processed log DataFrame to a CSV file.
    """
    # Clean up the "Message" column
    df["Message"] = df["Message"].str.replace(r"\s+", " ", regex=True).str.strip()

    queryReceivedPattern = (
        r"(?i)Query Received:\s*\{[^}]*\}:\s*(Exec plugin instance.*?;)"
    )
    ibplPattern = re.compile(r"IBPL body generated for parameterized", re.IGNORECASE)
    execPluginPattern = re.compile(
        r"(Exec plugin instance.*?;)", re.IGNORECASE | re.DOTALL
    )
    createProcedurePattern = re.compile(r"Create Procedure", re.IGNORECASE)
    execPluginInstancePattern = re.compile(
        r"(Exec plugin instance.*?;)", re.IGNORECASE | re.DOTALL
    )

    df["extractedMessage"] = df["Message"].str.extract(
        queryReceivedPattern, flags=re.IGNORECASE, expand=False
    )
    filteredDf1 = df[df["extractedMessage"].notnull()]
    filteredMessages2 = df[df["Message"].str.contains(ibplPattern, na=False)]
    filteredMessages3 = df[df["Message"].str.contains(createProcedurePattern, na=False)]

    filteredDf2 = pd.DataFrame(
        {
            "RId": filteredMessages2["RId"].values,
            "Message": filteredMessages2["Message"].values,
        }
    )
    filteredDf3 = pd.DataFrame(
        {
            "RId": filteredMessages3["RId"].values,
            "Message": filteredMessages3["Message"].values,
        }
    )

    uniqueMessagesDf = filteredDf1.drop_duplicates(subset=["extractedMessage", "RId"])
    pluginNamePattern = re.compile(
        r"Exec plugin instance(?: \[(.*?)\]| (\S+))", re.IGNORECASE
    )
    uniqueMessagesDf["Plugin Name"] = uniqueMessagesDf["extractedMessage"].apply(
        lambda x: (
            pluginNamePattern.search(x).group(1)
            if pluginNamePattern.search(x) is not None
            else "Unknown"
        )
    )
    uniqueMessagesDf["Message"] = uniqueMessagesDf["extractedMessage"]
    uniqueMessagesDf = uniqueMessagesDf[["RId", "Plugin Name", "Message"]]
    # uniqueMessagesDf.to_csv("Neeraj1.csv", index=False)

    def extractPluginMessages(filteredDf, pattern):
        extractedMessages = []
        extractedRIds = []
        extractedPluginNames = []

        for rid, message in zip(filteredDf["RId"], filteredDf["Message"]):
            matches = pattern.findall(message)
            for match in matches:
                extractedMessages.append(match)
                pluginNameMatch = re.search(
                    r"Exec plugin instance (\S+)", match, re.IGNORECASE
                )
                pluginName = pluginNameMatch.group(1) if pluginNameMatch else "Unknown"
                if pluginName.startswith("[") and pluginName.endswith("]"):
                    pluginName = pluginName[1:-1]
                extractedPluginNames.append(pluginName)
                extractedRIds.append(rid)

        return pd.DataFrame(
            {
                "RId": extractedRIds,
                "Plugin Name": extractedPluginNames,
                "Message": extractedMessages,
            }
        )

    filteredDf2 = extractPluginMessages(filteredDf2, execPluginPattern)
    filteredDf3 = extractPluginMessages(filteredDf3, execPluginInstancePattern)

    combinedDf = pd.concat(
        [uniqueMessagesDf, filteredDf2, filteredDf3], ignore_index=True
    ).drop_duplicates()
    combinedDf.reset_index(drop=True, inplace=True)
    combinedDf["index"] = combinedDf.index + 1

    combinedDf = combinedDf[["index", "RId", "Plugin Name", "Message"]]

    return combinedDf


def getQueriesApply(_row, _queryFinished, _outputDict):
    unique_id = _row[MESSAGE].split(":")[1]
    tmp = _queryFinished.loc[_queryFinished[MESSAGE].str.contains(unique_id)]

    if len(tmp) > 0:
        tmp = tmp.iloc[0]
    else:
        return
    query = sub(r"Query Received: {.*}:", "", _row[MESSAGE])
    durationInMin = int(tmp[MESSAGE].split(":")[2].strip().replace("ms", "")) / 60000
    durationInMin = round(durationInMin, 4)
    durationInSec = durationInMin * 60
    durInMinSec = f"{int(durationInSec // 60):02}:{int(round(durationInSec % 60)):02}"
    endTime = tmp[TIMESTAMP].astimezone(tz.tzlocal())
    _outputDict[RID].append(_row[RID])
    _outputDict[THREAD].append(_row[THREAD])
    _outputDict[USER_ID].append(_row[USER_ID])
    _outputDict[QUERY].append(query.strip().replace("^", "")[0:max_characters])
    _outputDict[START_TIME].append(_row[TIMESTAMP].astimezone(tz.tzlocal()))
    _outputDict[END_TIME].append(endTime)
    _outputDict[DUR_IN_MINUTES].append(durationInMin)
    _outputDict[DUR_MM_SS].append(durInMinSec)
    _outputDict[DAY].append(_row[TIMESTAMP].strftime(Format))


def getQueries(_data):
    print("Getting Queries...")
    queryReceived = "Query Received: {"
    queryFinished = "CPU TIME: {"
    outputDict = defaultdict(list)
    queryStart = _data[_data[MESSAGE].str.contains(queryReceived)]
    queryFinished = _data[_data[MESSAGE].str.contains(queryFinished)]
    queryStart.apply(lambda _x: getQueriesApply(_x, queryFinished, outputDict), axis=1)
    return DataFrame.from_dict(outputDict)


def getSCSSolverLogsComp(_row, _outputDict):
    tmp = _row[MESSAGE].split("time")
    durationInMin = round(int(tmp[1].replace("ms", "").strip()) / 60000, 4)
    query = f"Plugin instance SupplyChainSolver {tmp[0].strip()}"
    _outputDict[RID].append(_row[RID])
    _outputDict[THREAD].append(_row[THREAD])
    _outputDict[USER_ID].append(_row[USER_ID])
    _outputDict[QUERY].append(query[0:max_characters])
    _outputDict[DUR_IN_MINUTES].append(durationInMin)
    _outputDict[DUR_MM_SS].append(strftime("%M:%S", gmtime(durationInMin * 60)))
    _outputDict[INVOCATIONS].append(None)
    _outputDict[EXECUTIONS].append(None)
    _outputDict[NUM_OPS].append(None)
    _outputDict[START_TIME].append(_row[TIMESTAMP].astimezone(tz.tzlocal()))
    _outputDict[DAY].append(_row[TIMESTAMP].strftime(Format))


def getSolverPlanCache(Plandata):
    plancache = Plandata[Plandata[LOGGER] == SCS_PLAN_CACHE]
    outputDict = defaultdict(list)

    for ind, d in plancache.iterrows():
        tmp = d["Message"].split(".")[-1]
        tmp = tmp.split(":")
        if len(tmp) < 2:
            continue
        query = tmp[0]
        value = tmp[1]
        outputDict[RID].append(d[RID])
        outputDict[THREAD].append(d[THREAD])
        outputDict[USER_ID].append(d[USER_ID])
        outputDict[DAY].append(d[TIMESTAMP].strftime(Format))

        outputDict[QUERY].append(
            query.replace(" ", "").replace("^", "")[0:max_characters]
        )
        outputDict[Value].append(value)
        outputDict[Category].append("Static Parameter")

    # memory utilization
    scsutil = Plandata[Plandata[LOGGER] == SCS_UTIL]
    s = ""
    val = 0
    m_flag = False
    rid = 0  # Initialize RID
    thread = 0  # Initialize THREAD
    user_id = 0  # Initialize USER_ID
    query = ""  # Initialize query

    for ind, d in scsutil.iterrows():
        m_flag = True
        if d["Message"].startswith("Memory"):
            tmp = d["Message"].split(":")
            query = tmp[0]
            ls_value = re.findall(r"\d+\.\d+", tmp[1])
            if len(ls_value) != 0:
                value = float(ls_value[0])
            else:
                value = float(re.search(r"\d+", tmp[1]).group())
            rid = d[RID]
            thread = d[THREAD]
            user_id = d[USER_ID]
            if s == "":
                s = query

            if s in query and value > val:
                val = value

            if s not in query:
                outputDict[RID].append(d[RID])
                outputDict[THREAD].append(d[THREAD])
                outputDict[USER_ID].append(d[USER_ID])
                outputDict[DAY].append(d[TIMESTAMP].strftime(Format))
                outputDict[QUERY].append(s.replace("^", "")[0:max_characters])
                outputDict[Value].append(str(val) + "GB")
                outputDict[Category].append("Memory Utilization")
                s = query
                val = value
    if m_flag:
        outputDict[RID].append(rid)
        outputDict[THREAD].append(thread)
        outputDict[USER_ID].append(user_id)
        outputDict[DAY].append(d[TIMESTAMP].strftime(Format))
        outputDict[QUERY].append(query.replace("^", "")[0:max_characters])
        outputDict[Value].append(str(val) + "GB")
        outputDict[Category].append("Memory Utilization")

    # solver parameter
    if Solver_Parameter:
        plan = Plandata[Plandata[LOGGER] == SCS_PLAN]
        flag = False
        for idx, row in plan.iterrows():
            if "Activity Marker Exclusion List" in row[MESSAGE]:
                flag = True

            if flag:
                tmp = row["Message"].split(":")
                if tmp < 2:
                    continue
                query = tmp[0]
                value = tmp[1]
                outputDict[RID].append(row[RID])
                outputDict[THREAD].append(row[THREAD])
                outputDict[USER_ID].append(row[USER_ID])
                outputDict[DAY].append(row[TIMESTAMP].strftime(Format))
                outputDict[QUERY].append(query.replace("^", "")[0:max_characters])
                outputDict[Value].append(value)
                outputDict[Category].append("Solver Parameter")

            if "Enable Capacity Zero Qty Per" in row[MESSAGE]:
                break

    # Output Stats
    output_stats = Plandata[Plandata[LOGGER] == SCS_PLAN]
    output_stats = output_stats[output_stats[MESSAGE].str.startswith("Instance")]
    flag = False
    for idx, row in output_stats.iterrows():
        tmp = row["Message"].split(".")[2]

        if flag:
            tmp = tmp.split("=")
            if len(tmp) < 2:
                continue
            query = tmp[0]
            value = tmp[1]
            outputDict[RID].append(row[RID])
            outputDict[THREAD].append(row[THREAD])
            outputDict[USER_ID].append(row[USER_ID])
            outputDict[DAY].append(row[TIMESTAMP].strftime(Format))
            outputDict[QUERY].append(query.replace("^", "")[0:max_characters])
            outputDict[Value].append(value)
            outputDict[Category].append("Output Stats")

        if "Finished Zero qty RCA export" in tmp:
            flag = True

        if " Number of Build Ahead buckets saved by Adaptive SS " in tmp:
            flag = False

    lp_logs = Plandata[Plandata[LOGGER] == _LOGGER]
    lp_logs = lp_logs[lp_logs[MESSAGE].str.startswith("No")]

    for idx, row in lp_logs.iterrows():
        tmp = row["Message"].split(":")
        if tmp < 2:
            continue
        query = tmp[0]
        value = tmp[1]

        outputDict[RID].append(row[RID])
        outputDict[THREAD].append(row[THREAD])
        outputDict[USER_ID].append(row[USER_ID])
        outputDict[DAY].append(row[TIMESTAMP].strftime(Format))
        outputDict[QUERY].append(query.replace("^", "")[0:max_characters])
        outputDict[Value].append(value)
        outputDict[Category].append("LP Solver logs")

    return DataFrame.from_dict(outputDict)


def getActivePluginRunTimes(_data, _inputDict):
    print("Getting Computation on Active Plugin runtime.....")
    abstract_data = _data[_data[LOGGER] == PLUG]
    abstract_data = abstract_data[
        abstract_data[MESSAGE].str.contains("Elapsed Compute Time")
    ]

    for idx, _row in abstract_data.iterrows():
        tmp = _row[MESSAGE].split("=")
        query = tmp[0]
        durationInMin = round(float(tmp[1].replace("s", "").strip()) / 60, 4)

        _inputDict[RID].append(_row[RID])
        _inputDict[THREAD].append(_row[THREAD])
        _inputDict[USER_ID].append(_row[USER_ID])
        _inputDict[QUERY].append(query.replace("^", "")[0:max_characters])
        _inputDict[DUR_IN_MINUTES].append(durationInMin)
        _inputDict[DUR_MM_SS].append(strftime("%M:%S", gmtime(durationInMin * 60)))
        _inputDict[INVOCATIONS].append("")
        _inputDict[EXECUTIONS].append("")
        _inputDict[NUM_OPS].append("")
        _inputDict[START_TIME].append(_row[TIMESTAMP].astimezone(tz.tzlocal()))
        _inputDict[DAY].append(_row[TIMESTAMP].strftime(Format))

    return DataFrame.from_dict(_inputDict)


def getComputationsApply(_row, _outputDict, _data, _index):
    computationStr = "Computation execution time"
    query = _row[MESSAGE].split("Query :")
    durationInMin = None
    invocations = None
    executions = None
    numOps = None

    if len(query) > 1:
        tmp = query[1].split(f"{computationStr}:")
        query = tmp[0].strip()
        # print('--', tmp[1])
        durationInMin = round(
            float(tmp[1].split("s")[0].replace(",", "").replace(" s", "").strip()) / 60,
            4,
        )
        try:
            prevData = _data.loc[_index - 1, MESSAGE]
            prevData = prevData.split(";")
            invocations = int(prevData[0].split("invocations:")[1].strip())
            executions = int(prevData[1].split("executions:")[1].strip())
            numOps = int(prevData[2].split("non-null no ops:")[1].strip())
        except IndexError:
            numOps = None
        except Exception as ex:  # Catch any unanticipated errors
            print(f"An unexpected error occurred: {ex}")
            invocations = None
            executions = None
            numOps = None
    elif len(query) == 1:
        query = query[0]
        if "Finished executing plug-in instance" in query:
            tmp = query.split("Finished executing plug-in instance")[1].split(
                f"{computationStr}:"
            )
            query = f"Plugin instance {tmp[0].strip()}"
            durationInMin = round(
                float(tmp[1].replace("s.", "").replace(",", "").strip()) / 60, 4
            )

    else:
        print(_row)

    durationInSec = durationInMin * 60

    durInMinSec = f"{int(durationInSec // 60):02}:{int(round(durationInSec % 60)):02}"

    _outputDict[RID].append(_row[RID])
    _outputDict[THREAD].append(_row[THREAD])
    _outputDict[USER_ID].append(_row[USER_ID])
    _outputDict[QUERY].append(query.replace("^", "")[0:max_characters])
    _outputDict[DUR_IN_MINUTES].append(durationInMin)
    _outputDict[DUR_MM_SS].append(durInMinSec)
    _outputDict[INVOCATIONS].append(invocations)
    _outputDict[EXECUTIONS].append(executions)
    _outputDict[NUM_OPS].append(numOps)
    _outputDict[START_TIME].append(_row[TIMESTAMP].astimezone(tz.tzlocal()))
    _outputDict[DAY].append(_row[TIMESTAMP].strftime(Format))


def getComputations(_data):
    print("Getting Computations...")
    computationStr = "Computation execution time"
    outputDict = defaultdict(list)
    computationLogs = _data[(_data[MESSAGE].str.contains(computationStr))]
    SCSolverLogs = _data[
        (
            _data[LOGGER].values
            == "o9.GraphCube.Plugins.SupplyChainSolver.plan.PlanConfig"
        )
        & (_data[MESSAGE].str.endswith("ms"))
    ]
    SCSolverLogs.apply(lambda _row: getSCSSolverLogsComp(_row, outputDict), axis=1)
    computationLogs.apply(
        lambda _row: getComputationsApply(_row, outputDict, _data, _row.name), axis=1
    )

    return outputDict


def getSegmentApply(_row, _outputDict, segmentData, depthData):
    segmentStr = r"\[Segment :"
    curTimeStamp = _row[TIMESTAMP]
    segmentID = str(_row[MESSAGE].split("Started planning segment")[1].strip())
    curSegment = segmentData[
        segmentData[MESSAGE].str.contains(f"{segmentStr} {segmentID}]")
    ]
    allMsg = list(
        curSegment.loc[(curSegment[MESSAGE].str.contains("Number of")), MESSAGE]
    )
    curThreadDepth = depthData[
        ((depthData[THREAD] == _row[THREAD]) & (depthData[RID] == _row[RID]))
    ]
    finalSegmentDF = curSegment[
        curSegment[MESSAGE].str.contains("Finished planning segment")
    ]
    if len(finalSegmentDF) == 0:
        durationInMin = None
    else:
        finalTime = finalSegmentDF.iloc[0][TIMESTAMP]
        durationInMin = (finalTime - curTimeStamp).seconds / 60
        allMsg += list(
            curThreadDepth.loc[
                (curThreadDepth[TIMESTAMP] >= curTimeStamp)
                & (curThreadDepth[TIMESTAMP] <= finalTime),
                MESSAGE,
            ]
        )

    query = "\n".join(allMsg)
    _outputDict[RID].append(_row[RID])
    _outputDict[THREAD].append(_row[THREAD])
    _outputDict[USER_ID].append(_row[USER_ID])
    _outputDict[QUERY].append(query.replace("^", "")[0:max_characters])
    _outputDict[DUR_IN_MINUTES].append(
        round(durationInMin, 4) if durationInMin is not None else None
    )
    _outputDict[DUR_MM_SS].append(
        strftime("%M:%S", gmtime(durationInMin * 60))
        if durationInMin is not None
        else None
    )
    _outputDict[START_TIME].append(_row[TIMESTAMP].astimezone(tz.tzlocal()))
    _outputDict[DAY].append(_row[TIMESTAMP].strftime(Format))


def getSolverSegmentsDetail(_data):
    print("Getting Solver Data...")
    segmentStr = r"\[Segment :"
    outputDict = defaultdict(list)

    segmentData = _data[(_data[MESSAGE].str.contains(segmentStr))]
    startSegmentData = segmentData[
        segmentData[MESSAGE].str.contains("Started planning segment")
    ]
    depthData = _data[_data[MESSAGE].str.contains("Depth of Supply Chain")]

    startSegmentData.apply(
        lambda _row: getSegmentApply(_row, outputDict, segmentData, depthData), axis=1
    )
    return DataFrame.from_dict(outputDict)


def getPluginRunTimes(_data):
    print("Getting Plugin Data...")
    query_received = {}

    consolidatedOutputLog.apply(
        lambda row: query_received.setdefault(
            (row[RID], row[PLUGIN_NAME]), row[MESSAGE]
        ),
        axis=1,
    )

    outputDict = defaultdict(list)

    lp_time = _data[
        _data[MESSAGE].str.startswith("Finished executing plug-in instance")
    ].reset_index(drop=True)
    batch_job_submit_time = _data[
        _data[MESSAGE].str.startswith("Got batch-job submit response")
    ].reset_index(drop=True)
    exec_plugin_instance_time = _data[
        _data[MESSAGE].str.startswith("Started executing plug-in instance")
    ].reset_index(drop=True)
    finished_script_time = _data[
        _data[MESSAGE].str.startswith("Finished script execution on")
    ].reset_index(drop=True)
    write_to_ls_time = _data[
        _data[MESSAGE].str.startswith("Finished executing plug-in instance")
    ].reset_index(drop=True)

    lp_time = lp_time.drop_duplicates(subset="RId").reset_index(drop=True)
    batch_job_submit_time = batch_job_submit_time.drop_duplicates(
        subset="RId"
    ).reset_index(drop=True)
    exec_plugin_instance_time = exec_plugin_instance_time.drop_duplicates(
        subset="RId"
    ).reset_index(drop=True)
    finished_script_time = finished_script_time.drop_duplicates(
        subset="RId"
    ).reset_index(drop=True)
    write_to_ls_time = write_to_ls_time.drop_duplicates(subset="RId").reset_index(
        drop=True
    )

    common_rids = (
        set(lp_time["RId"])
        & set(batch_job_submit_time["RId"])
        & set(exec_plugin_instance_time["RId"])
        & set(finished_script_time["RId"])
        & set(write_to_ls_time["RId"])
    )

    lp_time = (
        lp_time[lp_time["RId"].isin(common_rids)]
        .sort_values("RId")
        .reset_index(drop=True)
    )
    batch_job_submit_time = (
        batch_job_submit_time[batch_job_submit_time["RId"].isin(common_rids)]
        .sort_values("RId")
        .reset_index(drop=True)
    )
    exec_plugin_instance_time = (
        exec_plugin_instance_time[exec_plugin_instance_time["RId"].isin(common_rids)]
        .sort_values("RId")
        .reset_index(drop=True)
    )
    finished_script_time = (
        finished_script_time[finished_script_time["RId"].isin(common_rids)]
        .sort_values("RId")
        .reset_index(drop=True)
    )
    write_to_ls_time = (
        write_to_ls_time[write_to_ls_time["RId"].isin(common_rids)]
        .sort_values("RId")
        .reset_index(drop=True)
    )

    for idx, row in lp_time.iterrows():
        text = row["Message"]
        computation = re.split(r"\[.*?]", text)

        computation_time = computation[1].split(":")
        bjs_time = datetime.fromisoformat(str(batch_job_submit_time[TIMESTAMP][idx]))
        epi_time = datetime.fromisoformat(
            str(exec_plugin_instance_time[TIMESTAMP][idx])
        )
        fst_time = datetime.fromisoformat(str(finished_script_time[TIMESTAMP][idx]))
        wtl_time = datetime.fromisoformat(str(write_to_ls_time[TIMESTAMP][idx]))

        read_time = abs(bjs_time - epi_time)
        plan_time = abs(fst_time - bjs_time)
        write_time = abs(wtl_time - fst_time)

        query = computation_time[0].split("Computation")[0]
        _time = computation_time[1]

        durationInSec = float(_time.replace(",", "").replace("s.", "").strip())
        durationInMin = round(durationInSec / 60, 3)
        durInMinSec = (
            f"{int(durationInSec // 60):02}:{int(round(durationInSec % 60)):02}"
        )

        outputDict[RID].append(row[RID])
        outputDict[THREAD].append(row[THREAD])
        outputDict[USER_ID].append(row[USER_ID])

        KEY = (row[RID], query.replace(" ", "").replace("^", "")[0:max_characters])
        if KEY in query_received:
            outputDict[QUERY].append(query_received[KEY])
        else:
            print(f"Key {KEY} not found in query_received")
            outputDict[QUERY].append(None)

        outputDict[DUR_IN_MINUTES].append(durationInMin)
        outputDict[DUR_MM_SS].append(durInMinSec)
        outputDict[Records].append(None)
        outputDict[START_TIME].append(row[TIMESTAMP].astimezone(tz.tzlocal()))
        outputDict[DAY].append(row[TIMESTAMP].strftime(Format))
        outputDict[PLUGIN_NAME].append(
            query.replace(" ", "").replace("^", "")[0:max_characters]
        )
        outputDict[PLUGIN_RUN_DIVISION].append("Total Plugin Run Time")
        outputDict[PLUGIN_RUN_DURATION].append(durationInMin)

        # Plugin run divisions
        for div, dur in zip(
            ["Total Read Time", "Total Processing Time", "Total Write Time"],
            [
                read_time.total_seconds() / 60,
                plan_time.total_seconds() / 60,
                write_time.total_seconds() / 60,
            ],
        ):
            durationInSec = dur * 60
            outputDict[RID].append(row[RID])
            outputDict[THREAD].append(row[THREAD])
            outputDict[USER_ID].append(row[USER_ID])

            if KEY in query_received:
                outputDict[QUERY].append(query_received[KEY])
            else:
                outputDict[QUERY].append(None)

            outputDict[DUR_IN_MINUTES].append(dur)
            outputDict[DUR_MM_SS].append(durInMinSec)
            outputDict[Records].append(None)
            outputDict[START_TIME].append(row[TIMESTAMP].astimezone(tz.tzlocal()))
            outputDict[DAY].append(row[TIMESTAMP].strftime(Format))

            outputDict[PLUGIN_NAME].append(
                query.replace(" ", "").replace("^", "")[0:max_characters]
            )
            durInMinSec = (
                f"{int(durationInSec // 60):02}:{int(round(durationInSec % 60)):02}"
            )
            dur = round(dur, 3)

            outputDict[PLUGIN_RUN_DIVISION].append(div)
            outputDict[PLUGIN_RUN_DURATION].append(dur)

    return pd.DataFrame.from_dict(outputDict)


def getSolverRunTimes(_data):
    print("Getting Solver Run Time...")
    planConfig = _data[_data[LOGGER] == SCS_PLAN_CONFIG]
    outputDict = defaultdict(list)
    
    recordInserted = planConfig[
        planConfig[MESSAGE].str.startswith("Total number of records inserted")
    ]
    for i, d in recordInserted.iterrows():
        tmp = d[MESSAGE].split("=")
        outputDict[RID].append(d[RID])
        outputDict[THREAD].append(d[THREAD])
        outputDict[USER_ID].append(d[USER_ID])
        outputDict[QUERY].append(tmp[0].replace("^", "")[0:max_characters])
        outputDict[DUR_IN_MINUTES].append(None)
        outputDict[DUR_MM_SS].append(None)
        outputDict[Records].append(int(tmp[1].strip()))
        outputDict[START_TIME].append(d[TIMESTAMP].astimezone(tz.tzlocal()))
        outputDict[DAY].append(d[TIMESTAMP].strftime(Format))
        outputDict[PLUGIN_NAME].append(None)
        outputDict[PLUGIN_RUN_DIVISION].append(None)
        outputDict[PLUGIN_RUN_DURATION].append(None)

    return DataFrame.from_dict(outputDict)


def getCombinedRunTimes(_data):
    pluginRunTimes = solverRunTimes = pd.DataFrame()

    pluginRunTimes = getPluginRunTimes(_data)
    solverRunTimes = getSolverRunTimes(_data)

    combined_output = pd.concat([pluginRunTimes, solverRunTimes], ignore_index=True)

    return combined_output


def processStat(message):
    match = re.search(r'"Data":{[^}]+}', message)
    return json.loads(f"{{{match.group(0)}}}") if match else None


def extract_fields(dicT):
    data = dicT.get("Data", {})
    return (
        data.get("SaveEndTime"),
        data.get("SaveStartTime"),
        data.get("VersionsToSave"),
    )


def getSaveStat(_data):
    print("Getting Save Stats ...")
    _data[DAY] = _data[TIMESTAMP].dt.strftime(Format)
    _data_saveStat = _data[_data[MESSAGE].str.contains("Save Stats")]
    if not _data_saveStat.empty:
        _data_saveStat = _data_saveStat.groupby([DAY])[MESSAGE].last().reset_index()
        _data_saveStat[MESSAGE] = _data_saveStat[MESSAGE].apply(processStat)

        _data_saveStat[[END_TIME, START_TIME, VERSION_SAVE]] = _data_saveStat[
            MESSAGE
        ].apply(lambda x: pd.Series(extract_fields(x)))
    else:
        _data_saveStat = pd.DataFrame()
    _data_save = _data[
        _data[MESSAGE].str.contains("Query Received:", case=False)
        & _data[MESSAGE].str.contains(r"Save\(\);", case=False)
    ]
    if not _data_save.empty:
        _data_save = _data_save.groupby([DAY]).size().reset_index(name=NUM_EXECUTORS)
    else:
        _data_save = pd.DataFrame()
    if not _data_saveStat.empty and not _data_save.empty:
        _output = pd.merge(
            _data_saveStat,
            _data_save,
            on=[DAY],
            how="left",
        )
        _output.drop(columns=MESSAGE, inplace=True)
        return _output

    elif not _data_saveStat.empty and _data_save.empty:
        _data_saveStat = _data_saveStat.assign(NumberOfSaveExecutedForDay="")
        return _data_saveStat

    elif _data_saveStat.empty and not _data_save.empty:
        empty_columns = ["EndTime", " StartTime", "VersionToSave"]
        for col in empty_columns:
            _data_save.insert(1, col, "")
        return _data_save

    else:
        return None


def getLogsData(_url, _apiKey, _tenant_id, _start, _end):
    print(f"Get Logs data from URL: {_url}")
    headers = {
        "Authorization": str(_apiKey),
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    params = {
        "tenantId": str(_tenant_id),
        "server": "LiveServer",
        "messageSearchByRegex": "false",
        "timestamp": _start,
        "endTime": _end,
        "pageSize": 100000,
    }
    res = get(_url, params=params, headers=headers, verify=False)
    print(res)

    if res.status_code == 200:
        try:
            json_response = res.json()  # Parse JSON response

            # with open("output.json", "w", encoding="utf-8") as json_file:
            #     json.dump(json_response, json_file, ensure_ascii=False, indent=4)

            new_df = pd.json_normalize(json_response)  # Get DataFrame from Json
            # new_df.to_csv("output.csv", index=False, encoding="utf-8")

            return True, new_df

        except json.JSONDecodeError:
            print("Error decoding JSON. Response content:", res.text)
            return False, DataFrame()

    else:
        return False, DataFrame()


def get_basic_key(userEmail, submittedPassword, api_endpoint):
    try:
        headers = {"Content-Type": "application/json"}
        payload = {
            "userEmail": userEmail,
            "submittedPassword": submittedPassword,
            "expirySeconds": 36000,
        }
        token_url = api_endpoint + "/api/framework/auth/login"
        response = post(
            token_url, data=json.dumps(payload), headers=headers, verify=False
        )
        if response.status_code == 200:
            # Assuming the API returns the API key in JSON format
            api_key = response.json().get("AuthToken")
            if api_key:
                return f"Basic {api_key}"
            else:
                print("API key not found in the response.")
        else:
            print("Failed to authenticate. Status code:", response.status_code)
    except exceptions.RequestException as e1:
        print("Error occurred during API request:", e1)

    return None


def format_filename_suffix(list_of_names):
    formatted_names = [
        sub(r"[^\w\s]", "_", str(name)).strip() for name in list_of_names
    ]
    return "_".join(formatted_names)


def adding_extra_column_attributes(extraDf: DataFrame, dict_of_headers):
    if any(header in extraDf.columns for header in dict_of_headers.keys()):
        return extraDf
    length_of_df = len(extraDf)
    new_cols_df = DataFrame(
        {header: [value] * length_of_df for header, value in dict_of_headers.items()}
    )
    return concat([new_cols_df, extraDf], axis=1)


# READ CONFIG FILE
isLogin = False
configData = None
logData = DataFrame()
login_url = ""
file_name_suffix = ""
url = ""
start_time = ""
end_time = ""
apiKey = ""
TenantID = ""
tenant_details: dict = {}
packages.urllib3.disable_warnings(packages.urllib3.exceptions.InsecureRequestWarning)
if len(argv) == 6:
    if ".com" in argv[1]:
        isLogin = True
        url = argv[1]
        apiKey = argv[2]
        TenantID = argv[3]
        start_time = argv[4]
        end_time = argv[5]

    elif ".json" in argv[1]:
        configData = load(open(str(argv[1])))
        isLogin = True
        url = configData["url"]
        apiKey = configData["apiKey"]
        tenantId = configData["tenantId"]
        start_time = configData["start_time"]
        end_time = configData["end_time"]

    elif ".csv" in argv[1]:
        CustomerName = argv[2]
        TenantName = argv[3]
        EnviName = argv[4]
        TenantID = argv[5]
        tenant_details = {
            "CustomerName": CustomerName,
            "TenantID": TenantID,
            "TenantName": TenantName,
            "EnviName": EnviName,
        }
        file_name_suffix = format_filename_suffix(tenant_details.values())
        inputPath = str(argv[1])
        inputPath_DF = pd.read_csv(inputPath, low_memory=False)
        print(f"Reading Input File: {inputPath}")
        try:
            logData = read_csv(inputPath, low_memory=False)
        except pd.errors.EmptyDataError as e:
            print("EmptyDataError:", e)
        except Exception as e:
            print("An error occurred:", e)
        logData.reset_index(inplace=True)
    else:
        raise Exception(f"Unable To Process file. Filename: {argv[1]}")

elif len(argv) == 9:
    isLogin = True
    url = argv[1]
    start_time = argv[2]
    end_time = argv[3]
    CustomerName = argv[4]
    TenantName = argv[5]
    EnviName = argv[6]
    TenantID = argv[7]
    apiKey = argv[8]
    tenant_details = {
        "CustomerName": CustomerName,
        "TenantID": TenantID,
        "TenantName": TenantName,
        "EnviName": EnviName,
    }
    file_name_suffix = format_filename_suffix(tenant_details.values())

elif len(argv) == 10:
    isLogin = True
    url = argv[1]
    start_time = argv[2]
    end_time = argv[3]
    CustomerName = argv[4]
    TenantName = argv[5]
    EnviName = argv[6]
    TenantID = argv[7]
    username = argv[8]
    password = argv[9]
    apiKey = get_basic_key(username, password, url)
    tenant_details = {
        "CustomerName": CustomerName,
        "TenantID": TenantID,
        "TenantName": TenantName,
        "EnviName": EnviName,
    }
    file_name_suffix = format_filename_suffix(tenant_details.values())
else:
    print(f"Invalid Arguments: {argv}")
    print(
        'Example: python/exe {name of the script} "url" "apikey" "tenantId" "startDate" "endDate"',
        'Example: python/exe {name of the script} "url" "start time" "endtime" \
        "CustomerName" "TenantName" "EnviName" "TenantID" "ApiKey"',
        'Example: python/exe {name of the script} "url" "start time" "endtime"\
        "CustomerName" "TenantName" "EnviName" "TenantID" "ETLuserName" "ETLPassword"',
    )
    raise Exception(f"Invalid Arguments: {argv}")

if isLogin:
    logURL = f"{url}/api/logs"
    # TIMEZONE CONVERSION
    utc_zone = tz.tzutc()
    local_zone = tz.tzlocal()

    start = (
        datetime.strptime(start_time, "%Y-%m-%d")
        .replace(tzinfo=local_zone)
        .astimezone(utc_zone)
        .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        + "Z"
    )
    end = (
        datetime.strptime(end_time, "%Y-%m-%d")
        .replace(tzinfo=local_zone)
        .astimezone(utc_zone)
        .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        + "Z"
    )
    print(f"Start Time: {start} and End Time: {end}")
    isPassed, logData = getLogsData(logURL, apiKey, TenantID, start, end)
    logData.to_csv(f"input_log.csv", index=False)
    if not isPassed:
        raise Exception("Unable to get Logs from the Server.")

if len(logData) > 0:
    consolidatedOutputLog = processLogFile(logData)
    # consolidatedOutputLog.to_csv("consolidatedOutputLog.csv", index=False)
    logData.reset_index(inplace=True)
    logData[TIMESTAMP] = to_datetime(logData[TIMESTAMP], format="mixed", dayfirst=True)

    # GET QUERIES DATA.

    queriesDF = getQueries(logData)
    if queriesDF is not None and len(queriesDF) > 0:
        queriesDF.sort_values(by=DUR_IN_MINUTES, inplace=True, ascending=False)
        queriesDF = queriesDF.replace("\r+|\n+|\t+", "", regex=True)
        queriesDF = adding_extra_column_attributes(queriesDF, tenant_details)
        queriesDF.to_csv(
            f"Queries_{file_name_suffix}.csv",
            index=False,
            sep="^",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
        )
    else:
        columns = [
            RID,
            THREAD,
            USER_ID,
            QUERY,
            START_TIME,
            END_TIME,
            DUR_IN_MINUTES,
            DUR_MM_SS,
            DAY,
        ]
        queriesDF = pd.DataFrame(columns=columns)
        queriesDF = adding_extra_column_attributes(queriesDF, tenant_details)
        queriesDF.to_csv(
            f"Queries_{file_name_suffix}.csv",
            index=False,
            sep="^",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
        )

    # GET COMPUTATION DATA.
    computationDict = getComputations(logData)
    # print_in_file(computationDict[QUERY])   #this one has the plugin instance
    computationDF = getActivePluginRunTimes(logData, computationDict)
    # print(computationDF)
    if computationDF is not None and len(computationDF) > 0:
        computationDF.sort_values(by=DUR_IN_MINUTES, inplace=True, ascending=False)
        computationDF = computationDF.replace("\r+|\n+|\t+", "", regex=True)
        computationDF = adding_extra_column_attributes(computationDF, tenant_details)
        # print_in_file(computationDF)
        computationDF.to_csv(
            f"Computation_{file_name_suffix}.csv",
            index=False,
            sep="^",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
        )
    else:
        columns = [
            RID,
            THREAD,
            USER_ID,
            QUERY,
            DUR_IN_MINUTES,
            DUR_MM_SS,
            INVOCATIONS,
            EXECUTIONS,
            NUM_OPS,
            START_TIME,
            DAY,
        ]
        computationDF = pd.DataFrame(columns=columns)
        computationDF = adding_extra_column_attributes(computationDF, tenant_details)
        computationDF.to_csv(
            f"Computation_{file_name_suffix}.csv",
            index=False,
            sep="^",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
        )

    # GET SOLVER DATA.
    segmentedSolverDF = getSolverSegmentsDetail(logData)
    if segmentedSolverDF is not None and len(segmentedSolverDF) > 0:
        segmentedSolverDF.sort_values(by=DUR_IN_MINUTES, inplace=True, ascending=False)
        segmentedSolverDF = adding_extra_column_attributes(
            segmentedSolverDF, tenant_details
        )
        segmentedSolverDF.to_csv(
            f"SegmentedSolverDetails_{file_name_suffix}.csv",
            index=False,
            sep="^",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
        )
    else:
        columns = [
            RID,
            THREAD,
            USER_ID,
            QUERY,
            DUR_IN_MINUTES,
            DUR_MM_SS,
            START_TIME,
            DAY,
        ]
        segmentedSolverDF = pd.DataFrame(columns=columns)
        segmentedSolverDF = adding_extra_column_attributes(
            segmentedSolverDF, tenant_details
        )
        segmentedSolverDF.to_csv(
            f"SegmentedSolverDetails_{file_name_suffix}.csv",
            index=False,
            sep="^",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
        )

    # GET PLUGIN RUNTIME
    solverRunTimesDF = getCombinedRunTimes(logData)
    # print(solverRunTimesDF)
    if solverRunTimesDF is not None and len(solverRunTimesDF) > 0:
        solverRunTimesDF = adding_extra_column_attributes(
            solverRunTimesDF, tenant_details
        )
        solverRunTimesDF.to_csv(
            f"SolverRunTimes_{file_name_suffix}.csv",
            index=False,
            sep="^",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
        )
    else:
        columns = [
            RID,
            THREAD,
            USER_ID,
            QUERY,
            DUR_IN_MINUTES,
            DUR_MM_SS,
            Records,
            START_TIME,
            DAY,
            PLUGIN_NAME,
            PLUGIN_RUN_DIVISION,
            PLUGIN_RUN_DURATION,
        ]
        solverRunTimesDF = pd.DataFrame(columns=columns)
        solverRunTimesDF = adding_extra_column_attributes(
            solverRunTimesDF, tenant_details
        )
        solverRunTimesDF.to_csv(
            f"SolverRunTimes_{file_name_suffix}.csv",
            index=False,
            sep="^",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
        )

    # GET SOLVE PLAN CACHE
    SolverPlanCacheDF = getSolverPlanCache(logData)
    # print(SolverPlanCacheDF)
    if SolverPlanCacheDF is not None and len(SolverPlanCacheDF) > 0:
        SolverPlanCacheDF = adding_extra_column_attributes(
            SolverPlanCacheDF, tenant_details
        )
        SolverPlanCacheDF.to_csv(
            f"SolverStats_{file_name_suffix}.csv",
            index=False,
            sep="^",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
        )
    else:
        columns = [RID, THREAD, USER_ID, DAY, QUERY, Value, Category]
        SolverPlanCacheDF = pd.DataFrame(columns=columns)
        SolverPlanCacheDF = adding_extra_column_attributes(
            SolverPlanCacheDF, tenant_details
        )
        SolverPlanCacheDF.to_csv(
            f"SolverStats_{file_name_suffix}.csv",
            index=False,
            sep="^",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
        )

    # GET SAVE RECORDS
    SaveStat = getSaveStat(logData)
    if SaveStat is not None and len(SaveStat) > 0:
        SaveStat = adding_extra_column_attributes(SaveStat, tenant_details)
        SaveStat.to_csv(
            f"SaveStats_{file_name_suffix}.csv",
            index=False,
            sep="^",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
        )
    else:
        columns = [DAY, START_TIME, END_TIME, VERSION_SAVE, NUM_EXECUTORS]
        SaveStat = pd.DataFrame(columns=columns)
        SaveStat = adding_extra_column_attributes(SaveStat, tenant_details)
        SaveStat.to_csv(
            f"SaveStats_{file_name_suffix}.csv",
            index=False,
            sep="^",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
        )

    print(f"Time Taken: {time() - initTime}")

else:
    print(f"No Data found for the time specified.")
