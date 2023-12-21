import datetime

class LogCleaner:
    # Handle 2 continuous logs
    def handle(self, log_obj_1, log_obj_2) -> dict:
        # Validate data
        validate = [
            ('open', 'switch'),
            ('open', 'close'),
            ('switch', 'switch'),
            ('switch', 'close'),
            ('close', 'open'),
        ]
        if (log_obj_1.get('type'), log_obj_2.get('type')) not in validate:
            return None
        
        # Calculate time difference
        time_diff = log_obj_2['unix_created_at'] - log_obj_1['unix_created_at']
        return {
            "id": log_obj_1["id"],
            "category": log_obj_1["category"],
            "user_id": log_obj_1["user_id"],
            "total": int(time_diff / 60),
            "app": log_obj_1["app"],
            "unix_timestamp": log_obj_1["unix_created_at"],
        }

    # Transform result from 2 logs
    def transform_event(self, data: dict):
        if not data: return None
        user_id=str(data['user_id'])
        unix_timestamp=data['unix_timestamp']

        date_time = datetime.datetime.utcfromtimestamp(unix_timestamp)

        day_of_week_index = date_time.weekday()
        days_of_week = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
        day_name = days_of_week[day_of_week_index]

        day_id=date_time.strftime('%Y%m%d')
        hour=date_time.strftime('%H')

        return {
            "id":data['id'],
            'user_id':int(user_id),
            'day_id':day_id,
            'hour':hour,
            'day':day_name,
            'id_string_hour':user_id+'_'+day_id+'_'+hour,
            'id_string_day':user_id+'_'+day_id,
            'category':data['category'],
            'app':data['app'],
            'total':data['total'],
            'unix_timestamp':unix_timestamp,
        }