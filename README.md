### Steps to deploy the DAG on Airflow server
* Copy the `bike_api_dag.py` file into the `dags` forlder under `airflow` directory on the airflow server
* Create `shh` connection
  Follow the steps below to setup shh connection in the Airflow web UI
  
  * On the top nav-bar of the UI goto to `Admin` and click on `Connections` as shown in the image below
  
    ![ssh connection 1](https://github.com/enigma0503/bikewise/blob/main/img/ssh1.png)
    
  * Click on the `plus` button to add a new connection of type `ssh` with the name `SSH_CONNECTION` and enter the 
    hostname along with the username as shown in the image below
    
     ![ssh connection 2](https://github.com/enigma0503/bikewise/blob/main/img/ssh2.png)
* Set the variables
