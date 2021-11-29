import tarfile
import os
import time
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage


def preprocess():
    try:
        file = os.path.abspath('service_account.json')
        job_client = dataproc.JobControllerClient.from_service_account_json(
            file, client_options={
                "api_endpoint": "us-central1-dataproc.googleapis.com:443"})
        job = {
            "placement": {"cluster_name": "project-14848"},
            "pig_job": {
                "query_file_uri": "gs://dataproc-staging-us-central1-615783235778-n1pp6rvq/process.sh",
            },
        }
        job_response = job_client.submit_job(
            request={"project_id": "kevinlii148484",
                     "region": "us-central1", "job": job})
        while (not job_response.done):
            job_response = job_client.get_job(
                request={"project_id": "kevinlii148484",
                         "region": "us-central1", "job_id": job_response.job_uuid})
        job_client.delete_job(
            request={"project_id": "kevinlii148484",
                     "region": "us-central1", "job_id": job_response.job_uuid})
    except Exception as e:
        print("{}\n".format(e))
    return None


def postprocess():
    try:
        file = os.path.abspath('service_account.json')
        job_client = dataproc.JobControllerClient.from_service_account_json(
            file, client_options={
                "api_endpoint": "us-central1-dataproc.googleapis.com:443"})
        job = {
            "placement": {"cluster_name": "project-14848"},
            "pig_job": {
                "query_file_uri": "gs://dataproc-staging-us-central1-615783235778-n1pp6rvq/postprocess.sh",
            },
        }
        job_response = job_client.submit_job(
            request={"project_id": "kevinlii148484",
                     "region": "us-central1", "job": job})
        while (not job_response.done):
            job_response = job_client.get_job(
                request={"project_id": "kevinlii148484",
                         "region": "us-central1", "job_id": job_response.job_uuid})
        job_client.delete_job(
            request={"project_id": "kevinlii148484",
                     "region": "us-central1", "job_id": job_response.job_uuid})
        client = storage.Client.from_service_account_json(file)
        bucket = client.bucket(
            "dataproc-staging-us-central1-615783235778-n1pp6rvq")
        output = bucket.blob("resultFile").download_as_string()
        return output
    except Exception as e:
        print("{}\n".format(e))


def submit_term_job(word):
    # Create the job client.
    JAR = ('file:///usr/lib/hadoop/hadoop-streaming.jar')
    file = os.path.abspath('service_account.json')
    job_client = dataproc.JobControllerClient.from_service_account_json(
        file, client_options={
            "api_endpoint": "us-central1-dataproc.googleapis.com:443"})
    job = {
        "placement": {"cluster_name": "project-14848"},
        "hadoop_job": {
            "jar_file_uris": [JAR],
            "main_class": "org.apache.hadoop.streaming.HadoopStreaming",
            "args": [
                "-files",
                "gs://dataproc-staging-us-central1-615783235778-n1pp6rvq/server/term_mapper.py,gs://dataproc-staging-us-central1-615783235778-n1pp6rvq/server/term_reducer.py",
                "-mapper",
                "python term_mapper.py",
                "-reducer",
                "python term_reducer.py {}".format(word),
                "-combiner",
                "python term_reducer.py {}".format(word),
                "-input",
                "/project/*",
                "-output",
                "/OutputFolder"
            ],
        },
    }
    operation = job_client.submit_job_as_operation(
        request={"project_id": "kevinlii148484",
                 "region": "us-central1", "job": job})
    operation.result()
    output = postprocess()
    return output


def submit_top_job(n):
    JAR = ('file:///usr/lib/hadoop/hadoop-streaming.jar')
    file = os.path.abspath('service_account.json')
    job_client = dataproc.JobControllerClient.from_service_account_json(
        file, client_options={
            "api_endpoint": "us-central1-dataproc.googleapis.com:443"})
    # hadoop jar /usr/lib/hadoop/hadoop-streaming.jar -file term_mapper.py -mapper 'python term_mapper.py' -file term_reducer.py -reducer 'python term_reducer.py king' -combiner 'python term_reducer.py king' -input /project/* -output /OutputFolder
    job = {
        "placement": {"cluster_name": "project-14848"},
        "hadoop_job": {
            "jar_file_uris": [JAR],
            "main_class": "org.apache.hadoop.streaming.HadoopStreaming",
            "args": [
                "-files",
                "gs://dataproc-staging-us-central1-615783235778-n1pp6rvq/server/top_mapper.py,gs://dataproc-staging-us-central1-615783235778-n1pp6rvq/server/top_reducer.py",
                "-mapper",
                "python top_mapper.py",
                "-reducer",
                "python top_reducer.py {}".format(n),
                "-combiner",
                "python top_reducer.py {}".format(n),
                "-input",
                "/project/*",
                "-output",
                "/OutputFolder"
            ],
        },
    }
    operation = job_client.submit_job_as_operation(
        request={"project_id": "kevinlii148484",
                 "region": "us-central1", "job": job})
    operation.result()
    output = postprocess()
    return output


def load_file(filenames):
    # Sends the file contents to the gcp server application
    f = os.path.abspath('service_account.json')
    storage_client = storage.Client.from_service_account_json(f)
    bucket = storage_client.bucket(
        "dataproc-staging-us-central1-615783235778-n1pp6rvq")
    try:
        blobs = bucket.list_blobs(prefix='project')
        for blob in blobs:
            blob.delete()
    except Exception as e:
        deleted = None
    for filename in filenames:
        file = os.path.abspath(filename)
        file_details = filename.split(".")
        try:
            tar = tarfile.open(filename, "r:gz")
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
            blob = bucket.blob("project/{}".format(file_details[0]))
            blob.upload_from_filename(file_details[0])
        except FileNotFoundError as e:
            print("Aborting: File '{}' not found. Make sure that you are choosing a valid file.".format(
                filename))
            print(e)
            exit(1)


def top_n(val):
    try:
        preprocess()
        response = submit_top_job(val).decode("utf-8")
        response = response.split("\n")
        dicts = {}
        for i in range(min(len(response), int(val))):
            res = response[i].split("\t")
            dicts[res[0]] = res[1]
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


def search_for_term(word, files):
    try:
        preprocess()
        response = submit_term_job(word).decode("utf-8")
        dicts = []
        res = response.split("\t")
        if response != None and len(res) >= 2:
            dicts.append(res[1])
        else:
            dicts.append(0)
    except Exception as e:
        print("{}\n".format(e))
        # filler data while API does not work
        dicts = ["500"]
    finally:
        row_format = "{:>3}{:>50}\n"
        table = row_format.format(
            "Documents", "Frequency")
        title = ""
        for file in files:
            res = file.split(".")
            title += (res[0] + " ")
        for frequency in dicts:
            table += row_format.format(title, frequency)
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
            table = search_for_term(val, files)
            print("\nYou searched for the term: {}".format(val))
            timeElapsed = time.time() - start_time
            print("Your search was executed in {} seconds\n".format(timeElapsed))
            print(table)
        elif val == 't' or val == "tn" or val == "topn" or val == "top-n":
            val = input("Enter Your N Value (must be an int): ").strip()
            while not val.isnumeric() and int(val) > 0:
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
