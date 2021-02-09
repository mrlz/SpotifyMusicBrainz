import csv
from multiprocessing import Pool
import time
def create_rows(arg):
    row = arg[0]
    extension_rows = arg[1]
    ans = []
    ans.append(row[0].replace(',', ''))
    ans.append(row[1].replace(',', ''))
    ans.append(row[2].replace(',', ''))
    ans.append(row[3].replace(',', ''))
    ans.append(row[4].replace(',', ''))
    ans.append(row[5].replace(',', ''))
    ans.append(row[6].replace(',', ''))
    ans.append("NONE")
    ans.append("NONE")
    for row2 in extension_rows:
        if row2:
            country = "NONE"
            begin = "NONE"
            breaking = 0
            if (row[2].lower() in row2[0].lower() and row[1].lower() in row2[1].lower()):
                if (len(row2) > 4):
                    begin = row2[4]
                if (len(row2) > 3):
                    country = row2[3]
                    breaking = 1
                if(breaking == 1):
                    ans[7] = country
                    ans[8] = begin
    return ans

pool = Pool(processes = 4)
print("Loading files")
with open('data.csv', 'r') as csvfile:
    reader = csv.reader(csvfile)
    tuples = list(reader)
    tuples = tuples[1:len(tuples)]
    i = 0
    # tuples = tuples[0:100] #DELETE THIS ONE
    with open('artist_country_full_clean.csv', 'r') as csvfile2:
        reader2 = csv.reader(csvfile2)
        tuples2 = list(reader2)
        tuples2 = tuples2[2:len(tuples2)]
        with open('extended_spotify_data.csv', 'w') as csvfile3:
            fieldnames = ['Position', 'Track Name', 'Artist', 'Streams', 'URL', 'Date', 'Region', 'Country', 'Begin_area']
            writer = csv.DictWriter(csvfile3, fieldnames = fieldnames)
            writer.writeheader()
            arguments = []
            for row in tuples:
                if row:
                    arguments.append([row,tuples2])
            print("Computing rows")
            start = time.time()
            results = pool.map(create_rows, arguments)
            end = time.time()
            print("Took", end-start)
            print("Writing rows")
            for res in results:
                # print(res)
                writer.writerow({'Position': res[0], 'Track Name': res[1], 'Artist': res[2], 'Streams': res[3], 'URL': res[4], 'Date': res[5], 'Region': res[6], 'Country': res[7], 'Begin_area': res[8]})
