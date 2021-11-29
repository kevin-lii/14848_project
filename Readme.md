# 14848 Project (Option 2)
### Client Application:
My client application is located in the file called `client.py`. To run the application first pull the image using `docker pull kevinlii/projectclient` and just simply run the command `docker run -it kevinlii/projectclient`. To call the server, the client will use the google-cloud-dataproc and google-cloud-storage libraries to communicate with GCP and process tbe request.

### Server Application:
My server files are located in the `server` directory and is uploaded to GCP. However, in order for the client application to communicate with the server, you need to create a file called `service_account.json` in the root directory. In terms of the content of the `service_account.json` file, you will fill the file using the comment from my Canvas submission.