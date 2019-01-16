Instructions to Run the code:
Please find these attachements:
•	Hive DDL file
•	Spark wiki_etl.scala file
•	ISO data
•	SBT file

1.	Please run the hive DLL commands 
2.	Download the ISO data on your Desktop
3.	Create and source directory on your Desktop and download the source .gz file in this directory
4.	Create the destination directory on your desktop to store the end query results
5.	Before running the code, please end the path for the source directory, ISO data file path and destination folder path.
6.	Once the path is edited you can run the code.
7.	The code will prompt you to enter the year, month, day and hour info. Please enter in yyyymmdd-hhmmss format (20120101-000000). Make sure the file exists in the source directory on your desktop. If file doesn’t exist, then the code will again prompt you to enter yyyymmdd-hhmmss. 
8.	Once this is entered the code execution continues and the data is stored in the final partition table.
9.	The result of the first query is stored in a single csv file in the destination directory you created under result1 folder.
10.	The result of the second query is stored in a single csv file in the destination directory you created under result2 folder.

