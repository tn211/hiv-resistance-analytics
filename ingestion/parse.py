from pathlib import Path
import xml.etree.ElementTree as ET

def parse_all(xml_path):
    root = ET.parse(xml_path).getroot()

    filename = Path(xml_path).name
    patient = root.findtext('patient/patientAlias')
    baseline_year = root.findtext('baselineYear')
    cd4_nadir = root.findtext('patient/CD4NadirBeforeTCE')
    date_unit = root.findtext('dateUnit')
    schema_version = root.findtext('schemaVersion')

    case_rows = [{
        "xml_filename": filename,
        "patient_alias": patient,
        "baseline_year": baseline_year,
        "cd4_nadir_before_tce": cd4_nadir,
        "date_unit": date_unit,
        "schema_version": schema_version
    }]

    measurements = []
    isolates = []
    mutations = []
    treatments = []

    for tag, mtype, val_fields in [
        ("baselineRNA", "RNA", ["logLoad", "rawLoad"]),
        ("pastRNA", "RNA", ["logLoad", "rawLoad"]),
        ("followupRNA", "RNA", ["logLoad", "rawLoad"]),
        ("baselineCD4", "CD4", ["count"]),
        ("pastCD4", "CD4", ["count"]),
        ("followupCD4", "CD4", ["count"]),
    ]:
        for node in root.findall(f".//{tag}"):
            for vf in val_fields:
                val = node.findtext(vf)
                if val:
                    measurements.append({
                        "xml_filename": filename,
                        "patient_alias": patient,
                        "relative_date": node.findtext("relativeDate"),
                        "measurement_type": mtype,
                        "timepoint_tag": tag,
                        "timepoint_type": tag.replace(mtype, "").lower(),
                        "value": val,
                        "value_type": vf
                    })

    for tag, itype in [("baselineIsolate", "baseline"), ("pastIsolate", "past")]:
        for i, iso in enumerate(root.findall(f".//{tag}")):
            iso_id = f"{filename}_{itype}_{i}"

            isolates.append({
                "xml_filename": filename,
                "patient_alias": patient,
                "isolate_id": iso_id,
                "isolate_type": itype,
                "gene": iso.findtext("gene"),
                "subtype": iso.findtext("subtype"),
                "relative_date": iso.findtext("relativeDate"),
                "aa_start": iso.findtext("aaStart"),
                "aa_stop": iso.findtext("aaStop"),
                "aa_sequence": iso.findtext("aaSequence"),
                "nucleotide_sequence": iso.findtext("nucleotideSequence"),
            })

            for mut in iso.findall(".//aaMutation"):
                mutations.append({
                    "xml_filename": filename,
                    "patient_alias": patient,
                    "isolate_id": iso_id,
                    "isolate_type": itype,
                    "gene": iso.findtext("gene"),
                    "relative_date": iso.findtext("relativeDate"),
                    "position": mut.findtext("position"),
                    "amino_acid": mut.findtext("aminoAcid"),
                    "mixtures": mut.findtext("mixtures"),
                })

    for tnode in root.findall(".//baselineNewTreatment"):
        start = tnode.findtext("relativeStartDate")
        stop = tnode.findtext("relativeStopDate")
        duration = tnode.findtext("duration")
        for drug in tnode.findall("drug"):
            treatments.append({
                "xml_filename": filename,
                "patient_alias": patient,
                "relative_start_date": start,
                "relative_stop_date": stop,
                "duration": duration,
                "drug_code": drug.findtext("drugCode"),
                "drug_class": drug.findtext("drugClass"),
                "treatment_type": "new"
            })

    for tnode in root.findall(".//pastRegimenTreatments"):
        start = tnode.findtext("relativeStartDate")
        stop = tnode.findtext("relativeStopDate")
        duration = tnode.findtext("duration")
        for drug in tnode.findall("drug"):
            treatments.append({
                "xml_filename": filename,
                "patient_alias": patient,
                "relative_start_date": start,
                "relative_stop_date": stop,
                "duration": duration,
                "drug_code": drug.findtext("drugCode"),
                "drug_class": drug.findtext("drugClass"),
                "treatment_type": "past"
            })

    return case_rows, measurements, isolates, mutations, treatments