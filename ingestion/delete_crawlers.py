import boto3
from botocore.exceptions import ClientError

def main():
    glue = boto3.client("glue")

    crawler_names = [
        "tce_measurements_crawler",
        "tce_case_crawler",
        "tce_isolates_crawler",
        "tce_mutations_crawler",
        "tce_treatments_crawler",
    ]

    for name in crawler_names:
        try:
            glue.delete_crawler(Name=name)
            print(f"Deleted: {name}")
        except glue.exceptions.EntityNotFoundException:
            print(f"Not found: {name}")
        except ClientError as e:
            print(f"Failed: {name} -> {e.response['Error']['Code']}: {e.response['Error']['Message']}")

if __name__ == "__main__":
    main()