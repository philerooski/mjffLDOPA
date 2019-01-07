import synapseclient as sc
import synapseutils as su
import re


PROJECT = "syn17103739"
GENE_ACTIVE_PARENT = "syn17103740"
METADATA_PARENT = "syn17108135"
PEBBLE_PARENT = "syn17103741"
PHONE_PARENT = "syn17103742"
TASKS_AND_SCORES = "syn17103743"


def walk_folder(syn, folder, device):
    w = su.walk(syn, folder)
    parent, folders, _ = next(w)
    records = []
    for f in folders:
        patient, patient_parent = next(w)
        patient_num = int(re.search("\d+", f[0]).group())
        patient_loc = "NY" if "NY" in f[0] else "BOS"
        _, _, files = next(w)
        for fi in files:
            file_name, file_id = fi
            file_session = int(re.search("\d+", file_name).group())
            syn_file = syn.get(file_id, downloadFile = False)
            fhid = syn_file['dataFileHandleId']
            entity_type = syn_file['entityType']
            #records.append([device, patient_loc, patient_num, file_session,


def main():
    args = read_args()
    syn = sc.login()
    walk_folder(syn, GENE_ACTIVE_PARENT, "GENEActiv")
    walk_folder(syn, PEBBLE_PARENT, "Pebble")
    walk_folder(syn, PHONE_PARENT, "Phone")


if __name__ == "__main__":
    main()
