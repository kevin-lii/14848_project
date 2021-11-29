# 14848 Project (Option 2)
### Client Application:
My client application is located in the file called `client.py`. To run the application first pull the image using `docker pull kevinlii/projectclient` and just simply run the command `docker run -it kevinlii/projectclient`. Currently, the docker container has three tar.gz files: `Hugo.tar.gz`, `shakespeare.tar.gz`, and `Tolstoy.tar.gz` so you can only use those three files to run the program with if you are using the Docker image. If you are the client using the Docker image, there is no extra steps needed to run this program; however, if you are either running the client locally or trying to update the Docker image, you need to create a file called `service_account.json` in the root directory. In terms of the content of the `service_account.json` file, you will fill the file using the comment from my Canvas submission. Also, if you are trying to run the client locally, you will need to run the command `pip install google-cloud-dataproc` and `pip install google-cloud-storage` since the client communicates with the server using the google-cloud-dataproc and google-cloud-storage libraries. 

### Server Application:
My server files are located in the `server` directory and is uploaded to GCP. 

### Walkthrough Video
https://drive.google.com/drive/folders/1Ep1NvMj1gtI8xIRWItyct27PSXjONBQn?usp=sharing