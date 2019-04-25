import os

FILE_LOG = 'synclogs.txt'

def process_file(text):
    with open(FILE_LOG, 'a') as file:
        file.writelines(text + '\n')
        os.utime(FILE_LOG, None)

def get_data_file():
    if os.path.exists(FILE_LOG) != True:
        with open(FILE_LOG, 'a') as file:
            os.utime(FILE_LOG, None)

    file = open(FILE_LOG, 'r') 
    return file.read().splitlines() 
    

tables=['log_423423_423_423', 'log_234324_23432_44', 'log_234324_23432_42', 'log_23432432_44', 'log_23432_333', 'log_23432_331', 'log_23432_331', 'log_23432_330']

dataFile = get_data_file()
print(dataFile)

for table in tables:
    if table != '' and any(table in t for t in dataFile) != True:
        print(table)
        process_file(table)
