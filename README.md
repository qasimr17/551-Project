# DSCI-551

This project consists of two main folders, Controller and User_Interface.

## Controller

This folder contains all the relevant code to run the flask backend, including the files to run search and analytics on the two databases: MySQL and MongoDB as well.

Further, it also contains the relevant csv files in the data subfolder that are we have used in our project implementation.


## User_Interface 

This folder contains all the code to run the front-end developed in Angular. 


## Running the code 

To run the application, we need to serve both the back-end and the front-end.
To serve the flask back-end, the relevant command to run in the Controller folder is:

`python app.py` 

This will start the backend server on port 5000.

Moreover, to start the frontend server, we run two commands in the /User_Interface/edfs-ui folder:

1. To install all the relevant dependencies:
`npm install` 

2. To run the application itself:
`ng serve`

The front-end server will then run on port 4200.