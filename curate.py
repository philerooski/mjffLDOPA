import synapseclient as sc
import synapseutils as su
import pandas as pd
import dateutil
import re


PROJECT = "syn17103739"
GENE_ACTIVE_PARENT = "syn17103740"
METADATA_PARENT = "syn17108135"
PEBBLE_PARENT = "syn17103741"
PHONE_PARENT = "syn17103742"
TASKS_AND_SCORES = "syn17103743"


def curate_raw_data(syn):
    raw_data_folders = [GENE_ACTIVE_PARENT, PEBBLE_PARENT, PHONE_PARENT]
    raw_data_devices = ["GENEActiv", "Pebble", "Phone"]
    data_cols = ["subjectId", "device", "participantDay", "timestampStart",
                 "timestampEnd", "sourceFile", "dataFileHandleId"]
    raw_data = pd.DataFrame(columns = data_cols)
    for folder, device in zip(raw_data_folders, raw_data_devices):
        w = su.walk(syn, folder)
        parent, folders, _ = next(w)
        records = []
        for folder_name, folder_id in folders:
            subject, _, subject_files = next(w)
            subject_num = int(re.search("\d+", folder_name).group())
            subject_loc = "NY" if "NY" in folder_name else "BOS"
            subject_id = "{}_{}".format(subject_num, subject_loc)
            for file_name, file_id in subject_files:
                file_day = int(re.search("\d+", file_name).group())
                syn_file = syn.get(file_id)
                df = pd.read_table(syn_file.path)
                timestamp_start = min(df.timestamp)
                timestamp_end = max(df.timestamp)
                fhid = syn_file['dataFileHandleId']
                records.append([subject_id, device, file_day, timestamp_start,
                                timestamp_end, file_id, fhid])
        raw_data_table = pd.DataFrame(records, columns = data_cols)
        fhids_to_copy = raw_data_table['dataFileHandleId'].tolist()
        source_files = raw_data_table["sourceFile"].tolist()
        new_fhids = []
        for i in range(0, len(fhids_to_copy), 100):
            fhids_subset = fhids_to_copy[i:i+100]
            source_files_subset = source_files[i:i+100]
            new_fhids_subset = su.copyFileHandles(
                    syn = syn,
                    fileHandles = fhids_subset,
                    associateObjectTypes = ["FileEntity"] * len(fhids_subset),
                    associateObjectIds = source_files_subset,
                    contentTypes = ["text/tab-separated-values"] * len(fhids_subset),
                    fileNames = [None] * len(fhids_subset))
            new_fhids_subset = [int(i['newFileHandle']['id'])
                                for i in new_fhids_subset['copyResults']]
            new_fhids = new_fhids + new_fhids_subset
        fhid_mapping = {k: v for k, v in zip(fhids_to_copy, new_fhids)}
        raw_data_table["dataFileHandleId"] = \
                raw_data_table["dataFileHandleId"].map(fhid_mapping)
        raw_data = raw_data.append(raw_data_table, ignore_index = True, sort = False)
    return(raw_data)


def translate_subject_id(sid):
    if sid < 100:
        return "{}_BOS".format(sid)
    else:
        return "{}_NYC".format(sid % 100)


def curate_scores(syn):
    scores_f = syn.get(TASKS_AND_SCORES)
    scores = pd.read_table(scores_f.path)
    scores_long = pd.melt(
            scores,
            id_vars = ['subject_id', 'visit', 'session', 'task_id',
                       'task_code', 'time_start', 'time_end'],
            value_vars = ['tremor_RightUpperLimb', 'tremor_LeftUpperLimb',
                          'tremor_LowerLimbs', 'dyskinesia_RightUpperLimb',
                          'dyskinesia_LeftUpperLimb', 'dyskinesia_LowerLimbs',
                          'bradykinesia_RightUpperLimb',
                          'bradykinesia_LeftUpperLimb', 'bradykinesia_LowerLimbs'])
    scores_long['phenotype'], scores_long['device_position'] = \
            scores_long['variable'].str.split("_", 1).str
    scores_long = scores_long.drop("variable", axis = 1)
    scores_long = scores_long.rename(columns = {"value": "score"})
    scores_curated = scores_long[
        ['subject_id', 'visit', 'session', 'task_id', 'task_code', 'time_start',
         'time_end', 'phenotype', 'device_position', 'score']]
    scores_curated = scores_curated.sort_values(
            by = ['subject_id', 'visit', 'session', 'time_start'])
    scores_curated['subject_id'] = scores_curated['subject_id'].apply(translate_subject_id)
    return(scores_curated)


def translate_metadata_subject_id(file_name):
    subject_num = int(re.search("\d+", file_name).group())
    subject_loc = "BOS" if "ldhp" in file_name else "NYC"
    return "{}_{}".format(subject_num, subject_loc)


def translate_metadata_time(year, month, day, timestamp):
    dt = dateutil.parser.parse("{}-{}-{} {}".format(year, month, day, timestamp))
    dt = dt.replace(tzinfo = dateutil.tz.gettz("America/New_York"))
    timestamp = int(dt.timestamp())
    return(timestamp)


def curate_metadata(syn):
    w = su.walk(syn, METADATA_PARENT)
    _, _, metadata_files = next(w)
    meds_curated = pd.DataFrame(
            columns = ["subject_id", "timestamp",
                       "pd_related_medications", "other_medications"])
    sleep_curated = pd.DataFrame(
            columns = ["subject_id", "timestamp", "event"])
    for metadata_name, metadata_id in metadata_files:
        subject_id = translate_metadata_subject_id(metadata_name)
        f = syn.get(metadata_id)
        meds = pd.read_excel(
                f.path, sheet_name = "Home Diary - Meds", skiprows=3)
        sleep = pd.read_excel(
                f.path, sheet_name = "Home Diary - Sleep", skiprows=3)
        feedback = pd.read_excel(
                f.path, sheet_name = "Feedback_Questionnaire", skiprows=3)
        # meds
        meds = meds[["Day (DD)", "Month (MM)", "Year (YYY)", "Time (hh:mm - 24 hour format)",
                     "PD-related medications taken", "Other medications taken"]]
        meds.columns = ["day", "month", "year", "time",
                        "pd_related_medications", "other_medications"]
        meds_curated_records = []
        for i, r in meds.iterrows():
            timestamp = translate_metadata_time(
                    r['year'], r['month'], r['day'], r['time'])
            meds_curated_records.append([subject_id, timestamp, r['pd_related_medications'],
                                         r['other_medications'])


def main():
    syn = sc.login()
    raw_data = curate_raw_data(syn)
    scores = curate_scores(syn)


if __name__ == "__main__":
    main()
