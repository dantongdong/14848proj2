# 14848proj2 - Mini Search Engine

### Video code walk through
here

### Client Side
client side application (all source code available under client folder): https://hub.docker.com/repository/docker/dantongdong/minisearch
*Note* For security purposes, the credential.json doesn't contain any credential information. Please inquire for more detail.

### Server side
All source code available under server folder. All of the script are also uploaded to GCP bucket

### Run Application
1. Pull from the client side image:
    ```
    docker pull dantongdong/minisearch:latest
    ```
2. In order to run the application and upload data, we need to first bind mount the data to the docker, and run the docker 
using interactive mode. Please run the following command:
    ```
    docker run -v YOUR_PATH_TO_DATA:/Data -it <image>
    ```
   For example, it can be something like:
    ```
    docker run -v ///Users/dantongdong/Desktop/Cloud_Infra/14848proj2/Data:/Data -it d5a1becbb6ac
    ```

3. After you launch the application, it will ask you to log in to gcp account and authenticate.
THE APPLICATION WILL NOT WORK IF IT CANNOT BE AUTHENTICATED FROM GCP

4. You will then be asked to upload files, after type in one file, press enter to type in another file. When
finished, type UPLOAD to upload and build RDD on GCP. For example, when prompted "Add your files name here > ",
you can type in
    ```
    Data/shakespeare
    Data/Hugo
    Data/Tolstoy
    ```
5. You can end select from search for a specific term or search for top N frequent words.
Follow the prompt and you will be good to go!