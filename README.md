## Steps to deploy the DAG on Airflow server

* Copy the `bike_api_dag.py` file into the `dags` forlder under `airflow` directory on the airflow server.

* Create `shh` connection. Follow the steps below to setup shh connection in the Airflow web UI.
  
  * On the top nav-bar of the UI goto to `Admin` and click on `Connections` as shown in the image below
  
    ![ssh connection 1](https://github.com/enigma0503/bikewise/blob/main/img/ssh1.png)
    
  * Click on the `plus` button to add a new connection of type `ssh` with the name `SSH_CONNECTION` and enter the 
    hostname along with the username as shown in the image below
    
     ![ssh connection 2](https://github.com/enigma0503/bikewise/blob/main/img/ssh2.png)
     
* Set the variables. Follow the steps below to set the variables in the Airflow web UI.
  
  * On the top nav-bar of the UI goto to `Admin` and click on `Variables` as shown in the image below
    
    ![var 1](https://github.com/enigma0503/bikewise/blob/main/img/var1.png)
  
  * Add a new variable with `Key` as `PYTHON_LOC` and enter the path of your `Python3` in `Val` field as shown below
    
    ![var 1](https://github.com/enigma0503/bikewise/blob/main/img/var2.png)
    
  * Add a new variable with `Key` as `SCRIPTS_DIR` and in `Val` field enter the path of your directory where all the python scripts to be executed are present as shown below
    
    ![var 1](https://github.com/enigma0503/bikewise/blob/main/img/var3.png)
    
    
## Steps to deploy the source code on the server.

* Copy the `bikewise_scripts` folder to the server where you want to run all your scripts (same server that is used to create the `ssh connection` on the airflow server)

* Create a `bookmark.json` file. The file will have `date as key`, `2 timestamps` and `3 flags` in the order `occurred_before timestamp` , `occurred_after timestamp` , `data downloaded flag`, `file copied flag`, `table created flag`. Keep the file empty if you don't need any previous data else add the required timestamps and flags to the file.
 
* Create a `config.yaml` file with the following parameters (make sure to change the values as per your server)
  
```shell
  PROD:
    URL: https://bikewise.org:443/api/v2/incidents
    BOOKMARK_FILE: /home/itv000579/bookmark.json
    LOCAL_DATA_DIR: /home/itv000579/shubham/bike_data
    LOCAL_REPORTS_DIR: /home/itv000579/shubham/bike_data/reports
    HDFS_DIR: /user
    HDFS_USERNAME: itv000579
```
* Now we need to create 2 environment variables. Use the terminal to open your `.bashrc` file and make the required changes as shown below

```shell
 vi ~/.bashrc
```
  * Move to the bottom of the file and type the following (make sure to make the changes as per your server):

```shell
 export ENVIRON=PROD
 export CONFIG_LOC=/path/to/your/config.yaml
```
  * Save and exit the editor. In order to make sure that the changes are reflected, we need to rerun the `.bashrc` file. To do so run the following command

```shell
 . ~/.bashrc
```

* Now you can test your DAG using the Airflow webserver UI.
