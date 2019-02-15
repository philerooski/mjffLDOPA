import synapseclient as sc
import synapseutils as su
import pandas as pd
import dateutil
import copy
import re


PROJECT = "syn17103739"
METADATA_PARENT = "syn17108135"


def translate_metadata_subject_id(file_name):
    subject_num = int(re.search("\d+", file_name).group())
    subject_loc = "BOS" if "ldhp" in file_name else "NYC"
    return "{}_{}".format(subject_num, subject_loc)


def translate_metadata_time(year, month, day, timestamp):
    try:
        dt = dateutil.parser.parse("{}-{}-{} {}".format(year, month, day, timestamp))
        dt = dt.replace(tzinfo = dateutil.tz.gettz("America/New_York"))
        timestamp = int(dt.timestamp())
    except ValueError:
        timestamp = float("nan")
    return(timestamp)


def iso_format(df):
    if all(pd.notnull(df.values)):
        return "{}-{}-{}".format(df.iloc[2], df.iloc[1], df.iloc[0])
    else:
        return None


def curate_subject_questionnaire(path, subject_id, subject_q_cols,
                                 null_str = ["Unknown", "<Select from list>"]):
    record = [subject_id]
    subject_q = pd.read_excel(
            path, sheet_name="Subject_Questionnaire", usecols="B", squeeze=True,
            skiprows=[0,1,2,3,17,21,22,36,37,43,44,53,54,61,62], na_values = null_str)
    visit_date_indices = [12, 13, 14]
    diagnosis_date_indices = [15, 16, 17]
    last_levodopa_dose_date_indices = [28, 29, 30, 31]
    for item in subject_q.iteritems():
        index, value = item
        if index == visit_date_indices[0]:
            record.append(iso_format(subject_q.iloc[visit_date_indices]))
        elif index == last_levodopa_dose_date_indices[0]:
            last_time = subject_q.iloc[last_levodopa_dose_date_indices]
            record.append(translate_metadata_time(*last_time.iloc[::-1]))
        elif index not in (visit_date_indices + last_levodopa_dose_date_indices):
            record.append(value)
    subject_q_row = pd.DataFrame([record], columns = subject_q_cols)
    return(subject_q_row)


def parse_controlled_sessions_values(controlled_session, subject_id, session_num):
    record = [subject_id, session_num]
    clinical_assessment_date_indices = [0, 1, 2]
    time_indices = [3, 4, 8, 9, 10, 11]
    clinical_assessment_date_vals = None
    for item in controlled_session.iteritems():
        index, value = item
        if index in clinical_assessment_date_indices:
            if clinical_assessment_date_vals is None:
                clinical_assessment_date_vals = controlled_session.iloc[
                        clinical_assessment_date_indices]
            else:
                pass
        elif index in time_indices:
            record.append(translate_metadata_time(
                *clinical_assessment_date_vals[::-1],
                value))
        else:
            record.append(value)
    return record


def curate_controlled_sessions(path, subject_id, controlled_session_cols,
                               null_str = ["Unknown", "<Select from list>"]):
    first_session_t1 = pd.read_excel(path, sheet_name = "1st Controlled_Session",
                                     skiprows=3, usecols="C", skipfooter=1,
                                     squeeze = True, na_values = null_str)
    second_session_t1 = pd.read_excel(path, sheet_name = "2nd Controlled_Session",
                                      skiprows=3, usecols="C", skipfooter=1,
                                      squeeze = True, na_values = null_str)
    first_session_second_meds = pd.read_excel(
            path, sheet_name = "1st Controlled_Session", skiprows=8, usecols="D",
            skipfooter=3, squeeze = True, na_values = null_str, header = None)
    second_session_second_meds = pd.read_excel(
            path, sheet_name = "2nd Controlled_Session", skiprows=8, usecols="D",
            skipfooter=3, squeeze = True, na_values = null_str, header = None)
    first_session_t2 = pd.read_excel(path, sheet_name = "1st Controlled_Session",
                                     skiprows=4, skipfooter = 5, usecols="H",
                                     squeeze = True, na_values = null_str)
    second_session_t2 = pd.read_excel(path, sheet_name = "2nd Controlled_Session",
                                     skiprows=4, skipfooter = 5, usecols="H",
                                     squeeze = True, na_values = null_str)
    first_session_comments = pd.read_excel(path, sheet_name = "1st Controlled_Session",
                                           skiprows=4, usecols="M", skipfooter=7,
                                           squeeze = True)
    second_session_comments = pd.read_excel(path, sheet_name = "2nd Controlled_Session",
                                            skiprows=4, usecols="M", skipfooter=7,
                                            squeeze = True)
    first_session = pd.concat([first_session_t1, first_session_second_meds[:1],
                               first_session_t2, first_session_comments],
                              ignore_index=True)
    second_session = pd.concat([second_session_t1, second_session_second_meds[:1],
                                second_session_t2, second_session_comments],
                               ignore_index=True)
    record_one = parse_controlled_sessions_values(
            first_session, subject_id, 1)
    record_two = parse_controlled_sessions_values(
            second_session, subject_id, 2)
    controlled_sessions_curated = pd.DataFrame([record_one, record_two],
                                               columns=controlled_session_cols)
    return controlled_sessions_curated

def parse_subject_diary(r):
    results = []
    non_dyskinesia_vals = r[[0,4,5,6]]
    dyskinesia_vals = r[[1,2,3]]
    dyskinesia_level = dyskinesia_vals.index("Y")
    results.append(dyskinesia_level)
    for v in non_dyskinesia_vals:
        if v == "Y":
            results.append(True)
        else:
            results.append(False)
    results.append(r[7]) # comments
    return results


def curate_subject_diary(path, subject_id, subject_diary_cols):
    record_one = []
    record_two = []
    first_diary_date = pd.read_excel(path, sheet_name="1st In Clinic Subject Diary",
                                     skiprows=4, usecols="C", squeeze=True,
                                     header=None).iloc[:4]
    second_diary_date = pd.read_excel(path, sheet_name="2nd In Clinic Subject Diary",
                                     skiprows=4, usecols="C", squeeze=True,
                                     header=None).iloc[:4]
    first_diary = pd.read_excel(path, sheet_name="1st In Clinic Subject Diary",
                                skiprows=12, usecols="B:I", header=None)
    second_diary = pd.read_excel(path, sheet_name = "2nd In Clinic Subject Diary",
                                 skiprows=12, usecols="B:I", header=None)
    first_date = iso_format(first_diary_date.iloc[:3])
    second_date = iso_format(second_diary_date.iloc[:3])
    first_tz = first_diary_date.iloc[4]
    second_tz = second_diary_date.iloc[4]
    for i, r in first_diary.iterrows():
        results = [subject_id, 1, first_date, first_tz, i*30]
        results.append(parse_subject_diary(r))
        record_one.append(results)
    for i, r in second_diary.iterrows():
        results = [subject_id, 2, second_date, second_tz, i*30]
        results.append(parse_subject_diary(r))
        record_two.append(results)
    subject_diary_curated = pd.DataFrame(record_one, columns=subject_diary_cols)
    subject_diary_curated = subject_diary_curated.append(record_two, ignore_index=True)
    return(subject_diary_curated)

def curate_metadata(syn):
    w = su.walk(syn, METADATA_PARENT)
    _, _, metadata_files = next(w)
    subject_q_cols = ["subject_id", "cohort", "gender", "birth_year",
        "dominant_hand", "upper_limb_length", "upper_arm_length", "lower_arm_length",
        "lower_limb_length", "thigh_length", "shank_length", "height", "weight",
        "visit_date", "diagnosis_day", "diagnosis_month", "diagnosis_year",
        "pd_most_affected_side", "gait_impediments", "posture_instability",
        "tremor", "bradykinesia", "disrupted_sleep", "freeze_of_gait", "dyskinesia",
        "rigidity", "other_symptoms", "last_levodopa_dose_timestamp", "regular_medication",
        "geneActive_num", "pebble_num", "geneActive_hand", "pebble_hand", "smartphone_location",
        "recording_start", "recording_end", "timezone", "updrs_time",
        "updrs_score_p1", "updrs_score_p2", "updrs_score_p3", "updrs_score_p4",
        "h_and_y_score", "updrs_second_visit_time", "updrs_second_visit_score_p3"]
    subject_q_curated = pd.DataFrame(columns = subject_q_cols)
    controlled_session_cols = ["subject_id", "session",
        "clinical_assessment_timestamp", "medication_intake_timestamp",
        "medication_name", "medication_dosage", "timezone",
        "second_medication_intake_timestamp", "stopwatch_start_timestamp",
        "fox_insight_app_start_timestamp", "geneActiv_start_timestamp",
        "general_comments"]
    controlled_session_curated = pd.DataFrame(columns = controlled_session_cols)
    subject_diary_cols = ["subject_id", "diary_num", "date", "timezone", "time_lag",
        "dyskinesia", "off", "tremor", "freeze_of_gait", "slowness_of_movement",
        "comments"]
    subject_diary_curated = pd.DataFrame(columns = subject_diary_cols)
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
        subject_q_curated = subject_q_curated.append(
                curate_subject_questionnaire(f.path, subject_id, subject_q_cols),
                ignore_index = True)
        controlled_session_curated = controlled_session_curated.append(
                curate_controlled_sessions(f.path, subject_id, controlled_session_cols),
                ignore_index = True)
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


def main():
    syn = sc.login()


if __name__ == "__main__":
    main()
