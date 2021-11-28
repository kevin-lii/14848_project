sh gsutil cp -r gs://dataproc-staging-us-central1-615783235778-n1pp6rvq/project/ .
sh gsutil cp -r gs://dataproc-staging-us-central1-615783235778-n1pp6rvq/server/ .
sh gsutil cp gs://dataproc-staging-us-central1-615783235778-n1pp6rvq/hadoop-streaming-2.7.3.jar .
fs -rm -r /project
fs -put project/ /
fs -rm -r /OutputFolder


