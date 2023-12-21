from supabase import Client
from supabase_transformer import SupabaseTransformer
from log_cleaner import LogCleaner
from typing import List
import datetime
import dateutil.parser

class Processor:
    def __init__(self, supabase: Client) -> None:
        self.MAX_PROCESS_LOOP = 1000000
        self.MAX_BATCH_SIZE = 10000
        self.supabase = supabase
        self.transformer = SupabaseTransformer()
        self.log_cleaner = LogCleaner()

    # extract first `batch_size` unprocessed logs
    # Max size = 1000, default = 100
    def extract(self, batch_size: int = 100):
        if batch_size > self.MAX_BATCH_SIZE: batch_size = self.MAX_BATCH_SIZE
        query = self.supabase.table('logs')\
            .select('*', count='exact')\
            .eq('is_process', False)\
            .order('created_at')\
            .limit(100)\
            .execute()
        batch_data = query.data
        for row in batch_data:
            timestamp_str = row['created_at']
            dt = dateutil.parser.parse(timestamp_str)  # parse ISO 8601 timestamp string
            unix_timestamp = dt.timestamp()
            row['unix_created_at'] = int(unix_timestamp)
        return batch_data, query.count
    
    
    def transform(self, batch_data: List[dict]):
        # Handle 2 continuous logs
        for i in range(len(batch_data) - 1):
            log_obj_1 = batch_data[i]
            log_obj_2 = batch_data[i + 1]
            data = self.log_cleaner.handle(log_obj_1, log_obj_2)
            data = self.log_cleaner.transform_event(data)
            if data: self.transformer.transform(self.supabase, data)
            
            self.supabase.table('logs')\
                .update({"is_process": True})\
                .eq('id', log_obj_1['id'])\
                .execute()
        return

    def run(self):
        counter = 0
        while True:
            data, unprocessed_logs_count = self.extract()
            if unprocessed_logs_count < 2 or counter == self.MAX_PROCESS_LOOP:
                break
            self.transform(data)
            counter += 1