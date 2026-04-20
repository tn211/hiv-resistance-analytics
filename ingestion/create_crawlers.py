import boto3

def create_crawler(session, name, s3_path, database="hivdb_tce"):
    glue = session.client("glue")
    sts = session.client("sts")

    account_id = sts.get_caller_identity()["Account"]
    role_arn = f"arn:aws:iam::{account_id}:role/glue_crawler_role"

    glue.create_crawler(
        Name=name,
        Role=role_arn,
        DatabaseName=database,
        Targets={"S3Targets": [{"Path": s3_path}]},
        SchemaChangePolicy={
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "DEPRECATE_IN_DATABASE",
        },
    )
    print(f"Created crawler: {name}")

def main():
    session = boto3.Session()

    bucket_name = "hiv-data-022784797781"

    create_crawler(session, "tce_measurements_crawler", f"s3://{bucket_name}/hivdb-tce/tce_measurements/")
    create_crawler(session, "tce_case_crawler", f"s3://{bucket_name}/hivdb-tce/tce_case/")
    create_crawler(session, "tce_isolates_crawler", f"s3://{bucket_name}/hivdb-tce/tce_isolates/")
    create_crawler(session, "tce_mutations_crawler", f"s3://{bucket_name}/hivdb-tce/tce_mutations/")
    create_crawler(session, "tce_treatments_crawler", f"s3://{bucket_name}/hivdb-tce/tce_treatments/")

if __name__ == "__main__":
    main()