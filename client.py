import tarfile
import os
import time
import re
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage


def preprocess():
    try:
        job_client = dataproc.JobControllerClient.from_service_account_json(
            'service_account.json', client_options={
                "api_endpoint": "us-central1-dataproc.googleapis.com:443"})
        job = {
            "placement": {"cluster_name": "project-14848"},
            "pig_job": {
                "query_file_uri": "gs://dataproc-staging-us-central1-615783235778-n1pp6rvq/process.sh",
            },
        }
        job_client.submit_job_as_operation(
            request={"project_id": "kevinlii148484",
                     "region": "us-central1", "job": job}
        )
    except Exception as e:
        print("{}\n".format(e))
    return None


def postprocess(matches):
    try:
        job_client = dataproc.JobControllerClient.from_service_account_json(
            'service_account.json', client_options={
                "api_endpoint": "us-central1-dataproc.googleapis.com:443"})
        job = {
            "placement": {"cluster_name": "project-14848"},
            "pig_job": {
                "query_file_uri": "gs://dataproc-staging-us-central1-615783235778-n1pp6rvq/postprocess.sh",
            },
        }
        job_client.submit_job_as_operation(
            request={"project_id": "kevinlii148484",
                     "region": "us-central1", "job": job}
        )
        client = storage.Client.from_service_account_json(
            'service_account.json')

        bucket = client.get_bucket(matches.group(1))
        output = bucket.blob(
            f"{matches.group(2)}.000000000").download_as_string()
        return output
    except Exception as e:
        print("{}\n".format(e))


def submit_term_job(word):
    # Create the job client.
    WORDCOUNT_JAR = ('file:///usr/lib/hadoop/hadoop-streaming.jar')
    job_client = dataproc.JobControllerClient.from_service_account_json(
        'service_account.json', client_options={
            "api_endpoint": "us-central1-dataproc.googleapis.com:443"})
    job = {
        "placement": {"cluster_name": "project-14848"},
        "hadoop_job": {
            "main_jar_file_uris": WORDCOUNT_JAR,
            "args": [
                "-files",
                "gs://dataproc-staging-us-central1-615783235778-n1pp6rvq/server/term_mapper.py,gs://dataproc-staging-us-central1-615783235778-n1pp6rvq/server/term_reducer.py",
                "-mapper"
                "'python term_mapper.py'",
                "-reducer"
                "'python term_reducer.py {}'".format(word),
                "-combiner",
                "'python term_reducer.py {}".format(word),
                "-input",
                "/project/*",
                "-output",
                "/OutputFolder"
            ],
        },
    }
    operation = job_client.submit_job_as_operation(
        request={"project_id": "kevinlii148484",
                 "region": "us-central1", "job": job}
    )
    response = operation.result()
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)
    output = postprocess(matches)
    print(f"Job finished successfully: {output}")
    return output


def submit_top_job(n):
    WORDCOUNT_JAR = ('file:///usr/lib/hadoop/hadoop-streaming.jar')
    job_client = dataproc.JobControllerClient.from_service_account_json(
        'service_account.json', client_options={
            "api_endpoint": "us-central1-dataproc.googleapis.com:443"})
    # hadoop jar /usr/lib/hadoop/hadoop-streaming.jar -file term_mapper.py -mapper 'python term_mapper.py' -file term_reducer.py -reducer 'python term_reducer.py king' -combiner 'python term_reducer.py king' -input /project/* -output /OutputFolder
    job = {
        "placement": {"cluster_name": "project-14848"},
        "hadoop_job": {
            "main_jar_file_uris": WORDCOUNT_JAR,
            "args": [
                "-files",
                "gs://dataproc-staging-us-central1-615783235778-n1pp6rvq/server/top_mapper.py,gs://dataproc-staging-us-central1-615783235778-n1pp6rvq/server/top_reducer.py",
                "-mapper"
                "'python top_mapper.py'",
                "-reducer"
                "'python top_reducer.py {}'".format(n),
                "-combiner",
                "'python top_reducer.py {}".format(n),
                "-input",
                "/project/*",
                "-output",
                "/OutputFolder"
            ],
        },
    }
    operation = job_client.submit_job_as_operation(
        request={"project_id": "kevinlii148484",
                 "region": "us-central1", "job": job}
    )
    response = operation.result()
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)
    output = postprocess(matches)
    print(f"Job finished successfully: {output}")
    return output


def load_file(filenames):
    # Sends the file contents to the gcp server application
    for filename in filenames:
        file = os.path.abspath(filename)
        file_details = filename.split(".")
        try:
            tar = tarfile.open(file, "r:gz")
            if (tar == None):
                raise FileNotFoundError(
                    "Aborting: No files selected. Please select a valid file.")
            content = ""
            for member in tar.getmembers():
                f = tar.extractfile(member)
                if f is not None:
                    content += f.read().decode("utf-8").strip()
            f = open(file_details[0], "w")
            f.write(content)
            f.close()
            storage_client = storage.Client.from_service_account_json(
                'service_account.json')
            bucket = storage_client.bucket(
                "dataproc-staging-us-central1-615783235778-n1pp6rvq")
            blob = bucket.blob("project/{}".format(file_details[0]))
            blob.upload_from_filename(file_details[0])
        except FileNotFoundError:
            print("Aborting: File '{}' not found. Make sure that you are choosing a valid file.".format(
                filename))
            exit(1)


def top_n(val):
    # get the top n terms from GCP using GET request
    try:
        preprocess()
        dicts = submit_top_job(val)
    except Exception as e:
        print("{}\n".format(e))
        # filler data while API does not work
        dicts = {"King": 5000, "Henry": 4500, "the": 4000,
                 "fourth": 3500, "sir": 3000, "walter": 2500}
    finally:
        row_format = "{:>6}{:>15}\n"
        table = row_format.format("Word", "Frequency")
        for word, frequency in dicts.items():
            table += row_format.format(word, frequency)
        return table, len(dicts)


def search_for_term(word):
    # search the terms from files by using GET request to GCP server application
    try:
        preprocess()
        dicts = submit_term_job(word)
    except Exception as e:
        print("{}\n".format(e))
        # filler data while API does not work
        dicts = {"1": ["histories", "1kinghenryiv", "169"], "2": [
            "histories", "1kinghenryiv", "160"], "3": ["histories", "2kinghenryiv", "179"]}
    finally:
        row_format = "{:>6}{:>15}\n"
        table = row_format.format(
            "Doc Name", "Frequencies")
        for word, item in dicts.items():
            folder, name, frequency = item
            table += row_format.format(name, frequency)
        return table


if __name__ == "__main__":
    print("=========================")
    print("Welcome to Load My Engine")
    print("=========================\n")
    files = []
    while (True):
        val = input("Please select a tar.gz file you would like to process: ")
        files.append(val)
        exit_loop = False
        while(True):
            cont = input(
                "Would you like to select another file? [y/N]: ").strip().lower()
            if cont == "y" or cont == "yes":
                break
            elif cont == "n" or cont == "no":
                exit_loop = True
                break
            else:
                print("Your choice was invalid. Please select either [y/N]")
        if exit_loop:
            break
    print("You have selected the follow files:")
    print(files)
    print("Loading your engine...")
    content = load_file(files)
    print("\n=========================")
    print("Engine was loaded!")
    print("=========================\n")
    while(True):
        print("You can now either 'Search for Term' or view 'Top-N' terms")
        print("To exit the program, type 'e' or 'exit'")
        val = input("Please select an action...[s/tn/e]: ")
        val = val.strip().lower()
        if val == 's' or val == "search" or val == "search for term":
            val = input("Enter Your Search Term: ").strip()
            start_time = time.time()
            table = search_for_term(val)
            print("\nYou searched for the term: {}".format(val))
            timeElapsed = time.time() - start_time
            print("Your search was executed in {} seconds".format(timeElapsed))
            print(table)
        elif val == 't' or val == "tn" or val == "topn" or val == "top-n":
            val = input("Enter Your N Value (must be an int): ").strip()
            while not val.isnumeric():
                print(
                    "Error: The value you just entered was not an integer. Please try again")
                val = input(
                    "\nEnter Your N Value (must be an int): ").strip()
            table, tableLen = top_n(val)
            print("Top-{} Frequent Term(s)".format(min(int(val), tableLen)))
            print(table)
        elif val == 'e' or val == 'exit':
            print("\n=========================")
            print("Goodbye!")
            print("=========================\n")
            break
        else:
            print("You entered an invalid input. Please try again\n")


# -file
# gs://dataproc-staging-us-central1-615783235778-n1pp6rvq/server/term_mapper.py
# -mapper
# 'python term_mapper.py'
# -file
# gs://dataproc-staging-us-central1-615783235778-n1pp6rvq/server/term_reducer.py
# -reducer
# 'python term_reducer.py king'
# -input
# hdfs://project
# -output
# hdfs://OutputFolder
