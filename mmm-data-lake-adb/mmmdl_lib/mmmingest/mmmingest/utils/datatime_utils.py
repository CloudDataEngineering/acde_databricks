from datetime import datetime


class DatetimeUtils:

    @staticmethod
    def to_date_obj(datetime_str):
        return datetime.fromisoformat(datetime_str.split(".")[0])
    
    @staticmethod
    def time_diff_in_seconds(datetime_str_max, datetime_str_min):
        return int(
            (DatetimeUtils.to_date_obj(datetime_str_max) - DatetimeUtils.to_date_obj(datetime_str_min)).total_seconds())
    
    @staticmethod
    def get_current_ts():
        return datetime.utcnow()