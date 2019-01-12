import synapseclient as sc
import synapseutils as su
import pandas as pd
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
    raw_data = pd.DataFrame(
            columns = ["subjectId", "device", "participantDay",
                       "sourceFile", "dataFileHandleId"])
    for folder, device in zip(raw_data_folders, raw_data_devices):
        w = su.walk(syn, folder)
        parent, folders, _ = next(w)
        records = []
        for folder_name, folder_id in folders:
            patient, _, patient_files = next(w)
            patient_num = int(re.search("\d+", folder_name).group())
            patient_loc = "NY" if "NY" in folder_name else "BOS"
            subject_id = "{}_{}".format(patient_num, patient_loc)
            for file_name, file_id in patient_files:
                file_day = int(re.search("\d+", file_name).group())
                syn_file = syn.get(file_id, downloadFile = False)
                fhid = syn_file['dataFileHandleId']
                records.append([subject_id, device, file_day, file_id, fhid])
        raw_data_table = pd.DataFrame(
                records, columns = ["subjectId", "device", "participantDay",
                                    "sourceFile", "dataFileHandleId"])
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


def main():
    syn = sc.login()
    raw_data = curate_raw_data(syn)


if __name__ == "__main__":
    main()
