import boto3
import configparser
import os


config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ["AWS_ACCESS_KEY_ID"] = config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("AWS", "AWS_SECRET_ACCESS_KEY")

s3_client = boto3.client('s3')

S3_BUCKET = 'shwes3udacapstone'
KEY_PATH = "data"


def uploadToS3(f_name,s3_f_name):
    try:
        s3_client.upload_file(f_name, S3_BUCKET, KEY_PATH + s3_f_name)
    except Exception as e:
        print(e)


def uploadImmigrationDataS3():
    root = "../../data/18-83510-I94-Data-2016/"
    files = [root + f for f in os.listdir(root)]
    for f in files:
        uploadToS3(f,"/raw/i94_immigration_data/18-83510-I94-Data-2016/" + f.split("/")[-1])


def uploadDemographics():
    uploadToS3("us-cities-demographics.csv","/raw/demographics/us-cities-demographics.csv")


def uploadGlobalTemperatures():
    root = "../../data2/"
    files = [root + f for f in os.listdir(root)]
    for f in files:
        uploadToS3(f,"/raw/globaltemperatures/" + f.split("/")[-1])


def uploadAirportCode():
    uploadToS3("airport-codes_csv.csv", "/raw/airportcode/airport-codes_csv.csv")

def uploadCodes():
    uploadToS3("i94addrl.txt", "/raw/codes/i94addrl.txt")
    uploadToS3("i94cntyl.txt", "/raw/codes/i94cntyl.txt")
    uploadToS3("i94prtl.txt", "/raw/codes/i94prtl.txt")

uploadImmigrationDataS3()
uploadDemographics()
uploadGlobalTemperatures()
uploadAirportCode()
uploadCodes()