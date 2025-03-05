# **CEZ\_FTP\_DATA**

A small tool for PE Control that handles CSV data from Huawei dataloggers,\
converts them to JSON, and uploads them to the HU CEZ trader FTP server.

This tool automatically reads data from all directories on **source\_ftp**\
(configured in */data/config/sftp.json*), selects the appropriate file based on the current time,\
transforms it, and writes it to **target\_ftp**. The name of each directory must match the POD ID of the corresponding power plant.

The data transformation process runs every 5 minutes and is designed for Hungary (Europe/Budapest timezone).

More information about the required JSON format can be found here:\
[https://megujulo-cez.hu/knowledge](https://megujulo-cez.hu/knowledge)

---

## **Setting up data sending for a new project**

To enable data sending for a new project:

1. Create a new directory on **source\_ftp**. The directory name must be the POD id of the power plant.
2. Update the FTP push configuration on the Huawei datalogger of the given power plant.
3. Configuration of datalogger:
   1. Remote directory: POD id
   2. Data reporting: enable
   3. File format: Format 2
   4. File name: minYYYYMMDD.csv
   5. Time format: YYYY-MM-DD HH
   6. Reporting mode: Cyclic
   7. Reporting interval: 5min (based on CEZ requirement)
   8. File mode: Accumulated data

⚠ **Important:** The project must be traded with CEZ, and the photovoltaic plant (PVP) must be located in Hungary.

⚠ **MORE INFO** can be found here:\
[https://photonenergyco.sharepoint.com/sites/files/PECZ/2100_PE_Control/755_Monitoring/CEZ_FTP_DATA.docx](https://photonenergyco.sharepoint.com/sites/files/PECZ/2100_PE_Control/755_Monitoring/CEZ_FTP_DATA.docx)


---

## **How to run**

The easiest way to run the project is using Docker. If you have Docker installed, clone this repository:

```sh
git clone https://github.com/Mikker575/CEZ_FTP_DATA.git
```

Then, update the configuration files (such as *sftp.json* with correct FTP details) and run:

```
start_app.bat
```

### **Running without Docker**

If you prefer to run the project manually, ensure you have **Python 3.10** installed and follow these steps:

```sh
cd CEZ_FTP_DATA/app
venv\Scripts\activate  # Activate virtual environment
pip install -r requirements.txt  # Install dependencies
python main.py  # Start the application
```

---

## **How to run tests**

To run tests using **pytest**, execute the following:

```sh
cd CEZ_FTP_DATA/app
pytest
```