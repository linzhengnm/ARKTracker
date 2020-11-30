from datetime import datetime
from pandas.tseries.holiday import AbstractHolidayCalendar,Holiday, USMemorialDay,  USMartinLutherKingJr, USPresidentsDay, GoodFriday, \
    USLaborDay, USThanksgivingDay, nearest_workday
 
class NYSECalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('New Years Day', month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday('USIndependenceDay', month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25, observance=nearest_workday),
        ]
    def get_non_trading_days(self):
        current_year = datetime.now().year
        non_trading_days = self.holidays(datetime(current_year, 1, 1), datetime(current_year, 12, 31))
        return non_trading_days
 