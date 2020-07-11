from glob import glob
import os

with open('./data/thingspeak/combined-data.csv', 'a') as singleFile:
    # singleFile.write('thingspeak_primary_id,created_at,entry_id,PM1.0_CF_ATM_ug/m3,PM2.5_CF_ATM_ug/m3,'
    #                  'PM10.0_CF_ATM_ug/m3,UptimeMinutes,ADC,Temperature_F,Humidity_%,PM2.5_CF_1_ug/m3\n')
    for csvFile in glob('./data/thingspeak/thingspeak-*.csv'):
        daily_file = os.stat(csvFile)

        if daily_file.st_size < 100:
            os.remove(csvFile)
            print ("file size {}".format(daily_file.st_size))
        else :
            with open (csvFile, 'r') as sourcefile:
                next(sourcefile)
                channel = csvFile[-21:-15]
                for line in sourcefile:
                    singleFile.write(channel + ',' + line)
            os.remove(csvFile)
