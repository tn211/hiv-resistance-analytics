import time
import boto3

CRAWLERS = [
    "tce_measurements_crawler",
    "tce_case_crawler",
    "tce_isolates_crawler",
    "tce_mutations_crawler",
    "tce_treatments_crawler",
]

def run_all_crawlers_and_wait():
    session = boto3.Session(profile_name="hiv-project")
    glue = session.client("glue", region_name="us-east-1")

    # start all
    for name in CRAWLERS:
        print(f"Starting {name}")
        glue.start_crawler(Name=name)

    finished = set()

    while len(finished) < len(CRAWLERS):
        for name in CRAWLERS:
            if name in finished:
                continue

            crawler = glue.get_crawler(Name=name)
            state = crawler["Crawler"]["State"]

            if state == "READY":
                last = crawler["Crawler"].get("LastCrawl", {})
                status = last.get("Status")

                if status == "SUCCEEDED":
                    print(f"{name} succeeded")
                    finished.add(name)
                else:
                    raise RuntimeError(f"{name} failed with status: {status}")

            else:
                print(f"{name} still running (state={state})")

        time.sleep(5)

    print("All crawlers finished")


def main():
    run_all_crawlers_and_wait()


if __name__ == "__main__":
    main()