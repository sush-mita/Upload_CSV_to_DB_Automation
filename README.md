# Upload_CSV_to_DB_Automation
This Project is about uploading the CSV files into a Cloud Database using automation. 
- Made an automated code where dumping all the files in a folder and running the code Cleans the CSV files and then transforms them into the Database
- Used the IMDB CSV Files to Clean and upload to the AWS Cloud Database.
- Identified all the CSV files in the current directory and moved them to a new directory.
- Cleaning Steps Applied to the CSV files  :
- Cleaned Table Names and File Names.
- Replaced all empty values with NA
- Removed data which has special Characters.
- Made all the data into lower case letters and formated the data into clean visible values.
- Used Pandas Library to clean the Data
- Created a Database and table in AWS Cloud Database using Postgre SQL.
- Imported the cleaned data into the datbase 
- Applied Error handling Functions.
