'''
Created on Jan 23, 2012

@author: mraposa
'''

from GeneralUtilities import deployCode
from models import deployForProduction

DESTINATION_DIR = '\\\\vc21\\custom_code\\VOD\\VOD_JukeBox\\' 
SOURCE_DIR = 'T:\\git\\VODOPS\\VOD_JukeBox\\src\\'

if __name__ == '__main__':
    deployCode(SOURCE_DIR, DESTINATION_DIR, includeHelperModules=True, includeSourceCode=False, includePycFiles=True)
    deployForProduction('mssql+pyodbc://JukeBox')