sh gsutil cp -r gs://dataproc-staging-us-central1-615783235778-n1pp6rvq/project/ .
sh gsutil cp -r gs://dataproc-staging-us-central1-615783235778-n1pp6rvq/server/ .
fs -rm -r /project
fs -put project/ /
fs -rm -r /OutputFolder


