import boto3, os, tarfile, gdown
import polars as pl
from parse import parse_all
from pathlib import Path
import xml.etree.ElementTree as ET


def upload_to_s3(local_dir, bucket, prefix, s3_client):
    """Upload partitioned Parquet to S3."""
    local = Path(local_dir)
    for file in local.rglob("*.parquet"):
        key = f"{prefix}{file.relative_to(local)}"
        s3_client.upload_file(str(file), bucket, key)
        print(f"Uploaded: s3://{bucket}/{key}")


def main():
    os.makedirs("data", exist_ok=True)
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/xmls", exist_ok=True)
    os.makedirs("data/parquet", exist_ok=True)

    session = boto3.Session(profile_name="hiv-project")
    s3 = session.client("s3")
    bucket_name = "hiv-data-022784797781"

    url = "https://drive.google.com/file/d/19PRBBOEFZGalIXE8am64L1e6dyBlF5K_/view?usp=sharing"
    tar_path = "data/raw/tce_xmls.tar.gz"

    gdown.download(url, tar_path, quiet=False)

    with tarfile.open("data/raw/tce_xmls.tar.gz", "r:") as tar:
        tar.extractall(path="data/xmls", filter="tar")

    xml_files = list(Path("data/xmls/xmls_db").glob("*.xml"))

    all_cases = []
    all_measurements = []
    all_isolates = []
    all_mutations = []
    all_treatments = []

    for f in xml_files:
        try:
            case_rows, measurements, isolates, mutations, treatments = parse_all(
                f)
            all_cases.extend(case_rows)
            all_measurements.extend(measurements)
            all_isolates.extend(isolates)
            all_mutations.extend(mutations)
            all_treatments.extend(treatments)
        except Exception as e:
            print(f"Failed {f.name}: {e}")

    df_case = pl.DataFrame(all_cases)
    df_measurements = pl.DataFrame(all_measurements)
    df_isolates = pl.DataFrame(all_isolates)
    df_mutations = pl.DataFrame(all_mutations)
    df_treatments = pl.DataFrame(all_treatments)

    for key, subdf in df_measurements.partition_by("measurement_type",
                                                   as_dict=True).items():
        measurement_val = key[0] if isinstance(key, tuple) else key
        subdf = subdf.drop("measurement_type")
        subdf.write_parquet(
            f"data/parquet/tce_measurements/measurement_type={measurement_val}/data.parquet",
            mkdir=True,
        )

    df_case.write_parquet("data/parquet/tce_case/data.parquet", mkdir=True)
    df_isolates.write_parquet("data/parquet/tce_isolates/data.parquet",
                              mkdir=True)
    df_mutations.write_parquet("data/parquet/tce_mutations/data.parquet",
                               mkdir=True)
    df_treatments.write_parquet("data/parquet/tce_treatments/data.parquet",
                                mkdir=True)

    upload_to_s3("data/parquet/tce_measurements/", bucket_name,
                 "hivdb-tce/tce_measurements/", s3)
    upload_to_s3("data/parquet/tce_case/", bucket_name, "hivdb-tce/tce_case/",
                 s3)
    upload_to_s3("data/parquet/tce_isolates/", bucket_name,
                 "hivdb-tce/tce_isolates/", s3)
    upload_to_s3("data/parquet/tce_mutations/", bucket_name,
                 "hivdb-tce/tce_mutations/", s3)
    upload_to_s3("data/parquet/tce_treatments/", bucket_name,
                 "hivdb-tce/tce_treatments/", s3)


if __name__ == "__main__":
    main()
