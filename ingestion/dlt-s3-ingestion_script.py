import duckdb, boto3, os, requests, tarfile, gdown, gzip, time
import polars as pl
from pathlib import Path
import xml.etree.ElementTree as ET


@dlt.resource(name="tce_xmls", write_disposition="replace")
def ingest_tce_xmls(
    gdrive_url: str = "https://drive.google.com/file/d/19PRBBOEFZGalIXE8am64L1e6dyBlF5K_/view?usp=sharing",
    tar_path: str = "ingestion/data/raw/tce_xmls.tar.gz",
    extract_path: str = "ingestion/data/xmls",
):
    os.makedirs(os.path.dirname(tar_path), exist_ok=True)
    gdown.download(gdrive_url, tar_path, quiet=False)

    with tarfile.open(tar_path, "r:*") as tar:
        tar.extractall(path=extract_path, filter="tar")

    for root, _, files in os.walk(extract_path):
        for fname in files:
            yield {"path": os.path.join(root, fname), "filename": fname}


# ---


# Parse the XML files into Parquet files.


@dlt.resource(name="tce_cases", write_disposition="replace")
def tce_cases(xml_dir="ingestion/data/xmls/xmls_db"):
    for f in Path(xml_dir).glob("*.xml"):
        try:
            case_rows, _, _, _, _ = parse_all(f)
            yield from case_rows
        except Exception as e:
            print(f"Failed {f.name}: {e}")

@dlt.resource(name="raw_measurements", write_disposition="replace")
def tce_measurements(xml_dir="ingestion/data/xmls/xmls_db"):
    for f in Path(xml_dir).glob("*.xml"):
        try:
            _, measurements, _, _, _ = parse_all(f)
            yield from measurements
        except Exception as e:
            print(f"Failed {f.name}: {e}")

@dlt.resource(name="tce_isolates", write_disposition="replace")
def tce_isolates(xml_dir="ingestion/data/xmls/xmls_db"):
    for f in Path(xml_dir).glob("*.xml"):
        try:
            _, _, isolates, _, _ = parse_all(f)
            yield from isolates
        except Exception as e:
            print(f"Failed {f.name}: {e}")

@dlt.resource(name="tce_mutations", write_disposition="replace")
def tce_mutations(xml_dir="ingestion/data/xmls/xmls_db"):
    for f in Path(xml_dir).glob("*.xml"):
        try:
            _, _, _, mutations, _ = parse_all(f)
            yield from mutations
        except Exception as e:
            print(f"Failed {f.name}: {e}")

@dlt.resource(name="tce_treatments", write_disposition="replace")
def tce_treatments(xml_dir="ingestion/data/xmls/xmls_db"):
    for f in Path(xml_dir).glob("*.xml"):
        try:
            _, _, _, _, treatments = parse_all(f)
            yield from treatments
        except Exception as e:
            print(f"Failed {f.name}: {e}")


@dlt.source
def tce_source():
    return [tce_cases(), tce_measurements(), tce_isolates(), tce_mutations(), tce_treatments()]


def main():
    os.makedirs('ingestion/data', exist_ok=True)
    os.makedirs("ingestion/data/raw", exist_ok=True)
    os.makedirs("ingestion/data/xmls", exist_ok=True)
    os.makedirs("ingestion/data/parquet", exist_ok=True)

    bucket_name = "hiv-data-022784797781"

    pipeline = dlt.pipeline(
        pipeline_name="tce_ingestion",
        destination=filesystem(bucket_url=f"s3://{bucket_name}"),
        dataset_name="tce_uploaded",
    )

    load_info = pipeline.run(ingest_tce_xmls())
    print(load_info)

    filesystem_dest = dlt.destinations.filesystem(
        bucket_url=f"s3://{bucket_name}",
        enable_dataset_name_normalization=False,
    )

    pipeline = dlt.pipeline(
        pipeline_name="tce_ingestion",
        destination=filesystem_dest,
        dataset_name="hivdb-tce",
    )

    load_info = pipeline.run(tce_source(), loader_file_format="parquet")
    print(load_info)


if __name__ == "__main__":
    main()
