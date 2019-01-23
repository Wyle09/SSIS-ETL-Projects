import os
import time

def get_list_of_files(path, olderThanDays):
    timeNow = time.time()
    olderThanDays *= 86400
    
    for folder, subfolders, files in os.walk(path):
        for file in files:
            filePath = os.path.join(os.path.abspath(folder), file)
            
            if (timeNow - os.path.getmtime(filePath)) >= olderThanDays:
                yield filePath
    
path = "//US31SQLP001/SharedFilesPathM2MReports/"
oldFiles = get_list_of_files(path, 40)

for file in oldFiles:
    os.remove(file)