import synapseclient as sc
import synapseutils as su
import pandas as pd
import dateutil
import copy
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
    data_cols = ["subject_id", "device", "participant_day", "timestamp_start",
                 "timestamp_end", "source_file", "data_file_handle_id"]
    raw_data = pd.DataFrame(columns = data_cols)
    for folder, device in zip(raw_data_folders, raw_data_devices):
        w = su.walk(syn, folder)
        parent, folders, _ = next(w)
        records = []
        for folder_name, folder_id in folders:
            subject, _, subject_files = next(w)
            subject_num = int(re.search("\d+", folder_name).group())
            subject_loc = "NYC" if "NY" in folder_name else "BOS"
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
        fhids_to_copy = raw_data_table['data_file_handle_id'].tolist()
        source_files = raw_data_table["source_file"].tolist()
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
        raw_data_table["data_file_handle_id"] = \
                raw_data_table["data_file_handle_id"].map(fhid_mapping)
        raw_data = raw_data.append(raw_data_table, ignore_index = True, sort = False)
    return(raw_data)


def mutate_device_side(syn, raw_data_curated):
    train_q = syn.tableQuery("select patient, device, deviceSide from syn10495809")
    train = train_q.asDataFrame()
    test_q = syn.tableQuery("select patient, device, deviceSide from syn10701954")
    test = test_q.asDataFrame()
    device_sides = train.append(test, ignore_index = False, sort = False)
    device_sides = pd.DataFrame(list(set([tuple(r) for r in device_sides.values])),
                                columns = ["subject_id", "device", "device_position"])
    device_sides["device_position"] = device_sides["device_position"].apply(
            lambda s : "RightUpperLimb" if s == "Right" else "LeftUpperLimb")
    raw_data_curated["device_position"] = raw_data_curated.merge(
            device_sides, how = "left", by = ["subject_id", "device"])
    raw_data_curated["device_position"] = raw_data_curated["device_position"].apply(
            lambda s : "LowerLimbs" if pd.isnull(s) else s)
    return(device_sides)


def translate_subject_id(sid):
    if sid < 100:
        return "{}_BOS".format(sid)
    else:
        return "{}_NYC".format(sid % 100)


def curate_scores(syn):
    scores_f = syn.get(TASKS_AND_SCORES)
    scores = pd.read_table(scores_f.path)
    scores = scores.rename(columns = {"time_start": "timestamp_start",
                                      "time_end": "timestamp_end"})
    scores_long = pd.melt(
            scores,
            id_vars = ['subject_id', 'visit', 'session', 'task_id',
                       'task_code', 'timestamp_start', 'timestamp_end'],
            value_vars = ['tremor_RightUpperLimb', 'tremor_LeftUpperLimb',
                          'tremor_LowerLimbs', 'dyskinesia_RightUpperLimb',
                          'dyskinesia_LeftUpperLimb', 'dyskinesia_LowerLimbs',
                          'bradykinesia_RightUpperLimb',
                          'bradykinesia_LeftUpperLimb', 'bradykinesia_LowerLimbs'])
    scores_long['phenotype'], scores_long['body_region'] = \
            scores_long['variable'].str.split("_", 1).str
    scores_long = scores_long.drop("variable", axis = 1)
    scores_long = scores_long.rename(columns = {"value": "score"})
    scores_curated = scores_long[
        ['subject_id', 'visit', 'session', 'task_id', 'task_code', 'timestamp_start',
         'timestamp_end', 'phenotype', 'body_region', 'score']]
    scores_curated = scores_curated.sort_values(
            by = ['subject_id', 'visit', 'session', 'timestamp_start'])
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

def iso_format(df):
    return "{}-{}-{}".format(df.iloc[2], df.iloc[1], df.iloc[0])

def curate_subject_questionnaire(path, subject_q_cols,
                                 null_str = ["<Select from list>", "NA"]):
    record = []
    subject_q = pd.read_excel(
            path, sheet_name="Subject_Questionnaire", usecols="B", squeeze=True,
            skiprows=[0,1,2,3,17,21,22,36,37,43,44,53,54,61,62])
    visit_date_indices = [12, 13, 14]
    diagnosis_date_indices = [15, 16, 17]
    last_levodopa_dose_date_indices = [28, 29, 30, 31]
    for i in range(visit_date_indices[0]):
        record.append(iso_format(subject_q.iloc[visit_date_indices]))
    # TODO


def curate_metadata(syn):
    w = su.walk(syn, METADATA_PARENT)
    _, _, metadata_files = next(w)
    meds_cols = ["subject_id", "timestamp", "pd_related_medications",
                 "other_medications"]
    meds_curated = pd.DataFrame(columns = meds_cols)
    sleep_cols = ["subject_id", "sleep", "wake"]
    sleep_curated = pd.DataFrame(columns = sleep_cols)
    feedback_cols = ["subject_id", "charge_smartphone", "charge_pebble",
                     "experience_watches", "experience_devices", "clearness_diary",
                     "accuracy_diary", "additional_feedback_device_phone",
                     "additional_feedback_diary", "additional_feedback_experiment"]
    feedback_curated = pd.DataFrame(columns = feedback_cols)
    subject_q_cols = ["subject_id", "cohort", "gender", "birth_year",
        "dominant_hand", "upper_limb_length", "upper_arm_length", "lower_arm_length",
        "lower_limb_length", "thigh_length", "shank_length", "height", "weight",
        "visit_date", "date_of_diagnosis", "pd_most_affected_side",
        "gait_impediments", "posture_instability", "tremor", "bradykinesia",
        "disrupted_sleep", "freeze_of_gait", "dyskinesia", "rigidity",
        "other_symptoms", "last_levodopa_dose", "regular_medication", "geneActive_num",
        "pebble_num", "geneActive_hand", "pebble_hand", "smartphone_location",
        "recording_start", "recording_end", "recording_time_zone", "updrs_time",
        "updrs_score_p1", "updrs_score_p2", "updrs_score_p3", "updrs_score_p4",
        "h_and_y_score", "updrs_second_visit_time", "updrs_second_visit_score_p3"]
    subject_q_curated = pd.DataFrame(columns = subject_q_cols)
    null_str = "<Select from list>"
    for metadata_name, metadata_id in metadata_files:
        subject_id = translate_metadata_subject_id(metadata_name)
        f = syn.get(metadata_id)
        meds = pd.read_excel(
                f.path, sheet_name = "Home Diary - Meds", skiprows=3)
        sleep = pd.read_excel(
                f.path, sheet_name = "Home Diary - Sleep", skiprows=3)
        feedback = pd.read_excel(f.path, sheet_name = "Feedback_Questionnaire",
                                 skiprows=2, usecols = "A:B")
        # meds
        meds = meds[["Day (DD)", "Month (MM)", "Year (YYYY)", "Time (hh:mm - 24 hour format)",
                     "PD-related medications taken", "Other medications taken"]]
        meds.columns = ["day", "month", "year", "time",
                        "pd_related_medications", "other_medications"]
        meds_curated_records = []
        for i, r in meds.iterrows():
            if (pd.notnull(r['day']) and pd.notnull(r['month']) and
                pd.notnull(r['year']) and re.match("\d\d:\d\d:\d\d", str(r['time']))):
                timestamp = translate_metadata_time(
                        int(r['year']), r['month'], r['day'], r['time'])
                meds_curated_records.append([subject_id, timestamp, r['pd_related_medications'],
                                             r['other_medications']])
        meds_curated_records = pd.DataFrame(meds_curated_records, columns = meds_cols)
        meds_curated = meds_curated.append(meds_curated_records, ignore_index = True)
        # sleep
        sleep = sleep[["Day (DD)", "Month (MM)", "Year (YYYY)",
                       "Time fallen asleep (hh:mm - 24 hour format)",
                       "Time woke up (hh:mm - 24 hour format)"]]
        sleep.columns = ["day", "month", "year", "sleep", "wake"]
        sleep_curated_records = []
        sleep_curated_row = [subject_id]
        for i, r in sleep.iterrows():
            if r['day'] == null_str and r['month'] == null_str: # end of metadata
                if len(sleep_curated_row) == 2: # wake recorded but no sleep
                    sleep_curated_row.append(None)
                    sleep_curated_records.append(sleep_curated_row)
                    break
            if (pd.notnull(r['day']) and pd.notnull(r['month']) and
               pd.notnull(r['year'])):
                if re.match("\d\d:\d\d:\d\d", str(r['sleep'])):
                    timestamp = translate_metadata_time(
                        int(r['year']), r['month'], r['day'], r['sleep'])
                    if len(sleep_curated_row) == 1: # only contains subject_id
                        sleep_curated_row.append(timestamp)
                    else: # last record was a sleep record
                        sleep_curated_row.append(None) # no wake recorded for this record
                        sleep_curated_records.append(sleep_curated_row)
                        sleep_curated_row = [subject_id, timestamp]
                if re.match("\d\d:\d\d:\d\d", str(r['wake'])):
                    timestamp = translate_metadata_time(
                        int(r['year']), r['month'], r['day'], r['wake'])
                    if len(sleep_curated_row) == 2: # contains both subject_id and sleep
                        sleep_curated_row.append(timestamp)
                        sleep_curated_records.append(sleep_curated_row)
                        sleep_curated_row = [subject_id]
                    else: # no previous sleep timestamp recorded
                        sleep_curated_row.append(None)
                        sleep_curated_row.append(timestamp)
                        sleep_curated_records.append(sleep_curated_row)
                        sleep_curated_row = [subject_id]
        sleep_curated_records = pd.DataFrame(sleep_curated_records, columns = sleep_cols)
        sleep_curated = sleep_curated.append(sleep_curated_records, ignore_index = True)
        # feedback
        feedback.columns = ["question", "answer"]
        feedback_curated_records = []
        feedback_curated_row = [subject_id]
        for i, r in feedback.iterrows():
            if pd.notnull(r["question"]):
                if r["question"][0] in list(map(str, range(1, 7))):
                    if isinstance(r["answer"], str) and r["answer"] != null_str:
                        answer = int(re.search("\d+", r["answer"]).group())
                    else:
                        answer = ""
                    feedback_curated_row.append(answer)
                elif r["question"][0].isdigit() and int(r["question"][0]) > 6:
                    if r["answer"] == "--" or r["answer"] == null_str:
                        answer = ""
                    else:
                        answer = r["answer"]
                    feedback_curated_row.append(answer)
        feedback_curated_records = pd.DataFrame([feedback_curated_row],
                                                columns = feedback_cols)
        feedback_curated = feedback_curated.append(feedback_curated_records)
    return meds_curated, sleep_curated, feedback_curated


def parse_float_to_int(i):
    str_i = str(i)
    if "nan" == str_i:
        str_i = ""
    elif str_i.endswith(".0"):
        str_i = str_i[:-2]
    return(str_i)

def clean_numeric_cols(df, cols):
    df = copy.deepcopy(df)
    for c in cols:
        df[c] = df[c].apply(parse_float_to_int)
    return(df)

def store_tables(syn, raw_data_curated, scores_curated, meds_curated,
                 sleep_curated, feedback_curated):
    # sensor measurements
    raw_data_cols = [
            sc.Column(name = "subject_id", columnType = "STRING", maximumSize = 6),
            sc.Column(name = "device", columnType = "STRING", maximumSize = 10),
            sc.Column(name = "participant_day", columnType = "INTEGER"),
            sc.Column(name = "timestamp_start", columnType = "DOUBLE"),
            sc.Column(name = "timestamp_end", columnType = "DOUBLE"),
            sc.Column(name = "source_file", columnType = "ENTITYID"),
            sc.Column(name = "data_file_handle_id", columnType = "FILEHANDLEID")]
    raw_data_schema = sc.Schema(name = "Sensor Measurements", columns = raw_data_cols,
                                parent = PROJECT)
    raw_data_table = sc.Table(raw_data_schema, raw_data_curated)
    syn.store(raw_data_table)
    # task scores
    scores_curated_table = sc.table.build_table("Task Scores", PROJECT, scores_curated)
    syn.store(scores_curated_table)
    # medication diary
    meds_cols = [
            sc.Column(name = "subject_id", columnType = "STRING", maximumSize = 6),
            sc.Column(name = "timestamp", columnType = "INTEGER"),
            sc.Column(name = "pd_related_medications", columnType = "STRING",
                      maximumSize = 120),
            sc.Column(name = "other_medications", columnType = "STRING",
                      maximumSize = 120)]
    meds_curated_clean = clean_numeric_cols(meds_curated, ["timestamp"])
    meds_schema = sc.Schema(name = "Medication Diary", columns = meds_cols,
                            parent = PROJECT)
    meds_table = sc.Table(meds_schema, meds_curated_clean)
    syn.store(meds_table)
    # sleep diary
    sleep_cols = [
            sc.Column(name = "subject_id", columnType = "STRING", maximumSize = 6),
            sc.Column(name = "sleep", columnType = "INTEGER"),
            sc.Column(name = "wake", columnType = "INTEGER")]
    sleep_curated_clean = clean_numeric_cols(sleep_curated, ["sleep", "wake"])
    sleep_schema = sc.Schema(name = "Sleep Diary", columns = sleep_cols,
                       parent = PROJECT)
    sleep_table = sc.Table(sleep_schema, sleep_curated_clean)
    syn.store(sleep_table)
    # feedback survey
    feedback_cols = [
            sc.Column(name = "subject_id", columnType = "STRING", maximumSize = 6),
            sc.Column(name = "charge_smartphone", columnType = "INTEGER"),
            sc.Column(name = "charge_pebble", columnType = "INTEGER"),
            sc.Column(name = "experience_watches", columnType = "INTEGER"),
            sc.Column(name = "experience_devices", columnType = "INTEGER"),
            sc.Column(name = "clearness_diary", columnType = "INTEGER"),
            sc.Column(name = "accuracy_diary", columnType = "INTEGER"),
            sc.Column(name = "additional_feedback_device_phone", columnType = "LARGETEXT"),
            sc.Column(name = "additional_feedback_diary", columnType = "LARGETEXT"),
            sc.Column(name = "additional_feedback_experiment", columnType = "LARGETEXT")]
    feedback_curated_clean = clean_numeric_cols(
            feedback_curated,
            ["charge_smartphone", "charge_pebble", "charge_pebble",
             "experience_watches", "experience_devices", "clearness_diary",
             "accuracy_diary"])
    feedback_schema = sc.Schema(name = "Feedback Survey", columns = feedback_cols,
                       parent = PROJECT)
    feedback_table = sc.Table(feedback_schema, feedback_curated_clean)
    syn.store(feedback_table)


def main():
    syn = sc.login()
    raw_data_curated = curate_raw_data(syn)
    scores_curated = curate_scores(syn)
    meds_curated, sleep_curated, feedback_curated = curate_metadata(syn)
    store_tables(syn, raw_data_curated, scores_curated, meds_curated,
                 sleep_curated, feedback_curated)

if __name__ == "__main__":
    main()
