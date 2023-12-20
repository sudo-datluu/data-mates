from supabase import Client
import datetime
import dateutil.parser

class Worker:
    def __init__(self, supabase: Client) -> None:
        self.supabase = supabase

    # extract first 100 unprocessed logs
    def extract(self, batch_size: int = 100):
        result = self.supabase.table('logs')\
            .select('*')\
            .eq('is_process', False)\
            .order('created_at')\
            .limit(batch_size)\
            .execute()
        for row in result.data:
            timestamp_str = row['created_at']
            dt = dateutil.parser.parse(timestamp_str)  # parse ISO 8601 timestamp string
            unix_timestamp = dt.timestamp()
            row['unix_created_at'] = int(unix_timestamp)
        return result
    
    # Handle events from the logs
    def handle_event(self, event1, event2) -> dict:
        # Validate data
        validate = [
            ('open', 'switch'),
            ('open', 'close'),
            ('switch', 'switch'),
            ('switch', 'close'),
            ('close', 'open'),
        ]
        if (event1.get('type'), event2.get('type')) not in validate:
            return None
        
        # Calculate time difference
        time_diff = event2['unix_created_at'] - event1['unix_created_at']
        return {
            "category": event1["category"],
            "user_id": event1["user_id"],
            "total": int(time_diff / 60),
            "app": event1["app"]
        }
    
    # Transform result from 2 events
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
            'user_id':int(user_id),
            'day_id':day_id,
            'hour':hour,
            'day':day_name,
            'id_string':user_id+'_'+day_id+'_'+hour,
            'category':data['category'],
            'total':data['total']
        }
    
    def load(self):
        return

    def run(self):
        data = self.extract()
        self.transform(data)
        self.load()