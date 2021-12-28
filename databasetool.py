import sqlite3
import csv
from tqdm import tqdm


def createbase(name):
    '''
    Creating database for cell_tower.csv
    :param name: name of file
    :return: None
    '''
    connection = sqlite3.connect('%s.db' % name)
    cursor = connection.cursor()

    # Создание таблицы
    cursor.execute("""CREATE TABLE towersgsm
                      (radio text, mcc text, mnc text, lac text, cid text,
                       unit text, lon real, lat real, range text, samples text,
                        changeable text, created text, updated text, averageSignal text)
                   """)
    cursor.execute("""CREATE TABLE towersumts
                          (radio text, mcc text, mnc text, lac text, cid text,
                           unit text, lon real, lat real, range text, samples text,
                            changeable text, created text, updated text, averageSignal text)
                       """)
    cursor.execute("""CREATE TABLE towerslte
                          (radio text, mcc text, mnc text, lac text, cid text,
                           unit text, lon real, lat real, range text, samples text,
                            changeable text, created text, updated text, averageSignal text)
                       """)


def addrow(name):
    '''
    Making sqlite3 database from cell_towers.csv
    :param name: name of database
    :return:
    '''
    connection = sqlite3.connect('%s.db' % name)
    cursor = connection.cursor()
    with open('cell_towers.csv') as file:
        reader = csv.reader(file, delimiter=',')

        # bar1 = IncrementalBar('Countdown', max=10)
        for i in tqdm(reader):
            try:
                recording = next(reader)
                if recording[0] == 'GSM':
                    cursor.execute("""INSERT INTO towersgsm
                                  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", recording
                                   )
                elif recording[0] == 'UMTS':
                    cursor.execute("""INSERT INTO towersumts
                                  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", recording
                                   )
                elif recording[0] == 'LTE':
                    cursor.execute("""INSERT INTO towerslte
                                  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", recording
                                   )
            except:
                connection.commit()



def baseselector(basename, tablename, lonmin, lonmax, latmin, latmax):
    '''
    Returns list of base stations in square region
    :param basename: name of database :type str
    :param tablename: name of table in database :type str
    :param lonmin: lontitude range min :type float
    :param lonmax:  lontitude range max :type float
    :param latmin: latitude range min :type float
    :param latmax: latitude range max :type float
    :return: list of base stations :type list
    '''
    connection = sqlite3.connect('%s.db' % basename)
    cursor = connection.cursor()

    # LTE
    sql = f"SELECT * FROM {tablename} WHERE lat > {latmin} " \
          f"AND lat < {latmax} AND lon > {lonmin} AND lon < {lonmax} LIMIT 100000"
    cursor.execute(sql)
    print(len(cursor.fetchall()))
    # LTE


baseselector('towers', 'towerslte', 37.377372, 37.885489, 55.562614, 55.910502)

