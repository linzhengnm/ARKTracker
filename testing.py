from db_utils import ArkTrackDB
from data_analysis import DataAnalysis
import glob

# db = ArkTrackDB('testing2.db')
# # db.create_db()

# db.view_all()

csvfiles = glob.glob('/Users/linzheng/Desktop/Coding_Projects/ARKTracker/data/*')

DA = DataAnalysis('testing2.db')
# fdir = '/Users/linzheng/Desktop/Coding_Projects/ARKTracker/data/2020-11-30'
# DA.save_to_db(fdir)
# DA.get_all()

for csvfile in csvfiles:
    DA.save_to_db(csvfile)

DA.get_all()









