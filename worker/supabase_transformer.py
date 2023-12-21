from supabase import Client
import datetime

# Calculate the start and end of the week
def get_week_head_and_tail(day_id, date_format='%Y%m%d'):
    day  = datetime.datetime.strptime(day_id, date_format)
    weekday = day.weekday()
    # If it's Sunday, previous Sunday is the day itself
    # Otherwise, subtract the weekday number from the day to get the previous Sunday
    if weekday == 6:
        prev_sunday = day
    else:
        prev_sunday = day - datetime.timedelta(days=weekday+1)

    # Next Saturday is 6 days after the previous Sunday
    next_saturday = prev_sunday + datetime.timedelta(days=6)

    # Convert dates back to string
    prev_sunday_str = prev_sunday.strftime("%Y-%m-%d")
    next_saturday_str = next_saturday.strftime("%Y-%m-%d")

    return prev_sunday_str, next_saturday_str

class SupabaseTransformer:
    def __init__(self) -> None:
        pass

    '''
    Daily level
    '''
    def report_daily_by_app_table_transform(self, supabase: Client, data: dict):
        query = supabase.table('report_daily')\
            .select('*')\
            .eq('id_string',data["id_string_day"]).execute()
        if not query.data:
            report_daily_by_app_insert_data = {
                "user_id": data['user_id'],
                "day_id": data['day_id'],
                "id_string": data["id_string_day"],
                "total": data['total']
            }
            supabase.table("report_daily")\
                .insert(report_daily_by_app_insert_data)\
                .execute()
        else:
            new_total = query.data[0]['total'] + data['total']
            supabase.table("report_daily")\
                .update({"total": new_total})\
                .eq('id_string',data["id_string_day"])\
                .execute()
    
    def report_daily_by_app_entity_table_transform(self, supabase: Client, data: dict):
        query = supabase.table('report_daily_by_app_entity')\
            .select('*')\
            .eq('report_daily_id',data["id_string_day"])\
            .eq('app', data['app'])\
            .execute()
        if not query.data:
            report_daily_by_app_entity_insert_data = {
                "report_daily_id": data['id_string_day'],
                "app": data['app'],
                "total": data['total']
            }
            supabase.table("report_daily_by_app_entity")\
                .insert(report_daily_by_app_entity_insert_data)\
                .execute()
        else:
            new_total = query.data[0]['total'] + data['total']
            supabase.table("report_daily_by_app_entity")\
                .update({"total": new_total})\
                .eq('report_daily_id',data["id_string_day"])\
                .eq('app', data['app'])\
                .execute()
    
    def report_daily_by_category_entity_table_transform(self, supabase: Client, data: dict):
        query = supabase.table('report_daily_by_category_entity')\
            .select('*')\
            .eq('report_daily_id',data["id_string_day"])\
            .eq('category', data['category'])\
            .execute()
        if not query.data:
            report_daily_by_category_insert_data = {
                "report_daily_id": data['id_string_day'],
                "category": data['category'],
                "total": data['total']
            }
            supabase.table("report_daily_by_category_entity")\
                .insert(report_daily_by_category_insert_data)\
                .execute()
        else:
            new_total = query.data[0]['total'] + data['total']
            supabase.table("report_daily_by_category_entity")\
                .update({"total": new_total})\
                .eq('report_daily_id',data["id_string_day"])\
                .eq('category', data['category'])\
                .execute()
    '''
    Hour level
    '''
    def report_daily_by_hour_table_transform(self, supabase: Client, data: dict):
        query = supabase.table('report_daily_by_hour')\
            .select('*')\
            .eq('id_string',data["id_string_hour"])\
            .execute()
        if not query.data:
            report_daily_by_hour_insert_data = {
                "user_id": data['user_id'],
                "day_id": data['day_id'],
                "hour": data['hour'],
                "id_string": data['id_string_hour'],
                "total": data['total']
            }
            supabase.table("report_daily_by_hour")\
                .insert(report_daily_by_hour_insert_data)\
                .execute()
        else:
            new_total = query.data[0]['total'] + data['total']
            supabase.table("report_daily_by_hour")\
                .update({"total": new_total})\
                .eq('id_string',data["id_string_hour"])\
                .execute()
    
    def report_daily_by_hour_category_entity_transform(self, supabase: Client, data: dict):
        query = supabase.table('report_daily_by_hour_category_entity')\
            .select('*')\
            .eq('report_daily_by_hour_id',data["id_string_hour"])\
            .eq('category', data['category'])\
            .execute()
        if not query.data:
            report_daily_by_hour_category_entity_insert_data = {
                "report_daily_by_hour_id": data['id_string_hour'],
                "category": data['category'],
                "total": data['total']
            }
            supabase.table("report_daily_by_hour_category_entity")\
                .insert(report_daily_by_hour_category_entity_insert_data)\
                .execute()
        else:
            new_total = query.data[0]['total'] + data['total']
            supabase.table("report_daily_by_hour_category_entity")\
                .update({"total": new_total})\
                .eq('report_daily_by_hour_id',data["id_string_hour"])\
                .eq('category', data['category'])\
                .execute()
    
    def report_daily_by_hour_app_entity_transform(self, supabase: Client, data: dict):
        query = supabase.table('report_daily_by_hour_app_entity')\
            .select('*')\
            .eq('report_daily_by_hour_id',data["id_string_hour"])\
            .eq('app', data['app'])\
            .execute()
        if not query.data:
            report_daily_by_hour_app_entity_insert_data = {
                "report_daily_by_hour_id": data['id_string_hour'],
                "app": data['app'],
                "total": data['total']
            }
            supabase.table("report_daily_by_hour_app_entity")\
                .insert(report_daily_by_hour_app_entity_insert_data)\
                .execute()
        else:
            new_total = query.data[0]['total'] + data['total']
            supabase.table("report_daily_by_hour_app_entity")\
                .update({"total": new_total})\
                .eq('report_daily_by_hour_id',data["id_string_hour"])\
                .eq('app', data['app'])\
                .execute()
    
    def report_weekly_transform(self, supabase: Client, data: dict):
        week_head, week_tail = get_week_head_and_tail(data['day_id'])
        
        query = supabase.table('report_weekly')\
            .select('*')\
            .eq('user_id', data['user_id'])\
            .eq('week_head', week_head)\
            .eq('week_tail', week_tail)\
            .execute()
        if not query.data:
                # If no result, insert a new row
            report_weekly_insert_data = {
                "user_id": data['user_id'],
                "week_head": week_head,
                "week_tail": week_tail,
                "total": data['total'],
                "avg": data['total'] / 7,
                data['day']: data['total']
            }
            supabase.table("report_weekly")\
                .insert(report_weekly_insert_data)\
                .execute()
        else:
            # If a result exists, update the total for the given day
            new_total = query.data[0]['total'] + data['total']
            new_total_day = query.data[0][data['day']] + data['total']
            supabase.table("report_weekly")\
                .update({'total': new_total, "avg": new_total / 7, data['day']: new_total_day})\
                .eq('week_head', week_head)\
                .eq('week_tail', week_tail)\
                .execute()

    def transform(self, supabase: Client, data: dict):
        self.report_daily_by_app_table_transform(supabase, data)
        self.report_daily_by_app_entity_table_transform(supabase, data)
        self.report_daily_by_category_entity_table_transform(supabase, data)
        self.report_daily_by_hour_table_transform(supabase, data)
        self.report_daily_by_hour_category_entity_transform(supabase, data)
        self.report_daily_by_hour_app_entity_transform(supabase, data)
        self.report_weekly_transform(supabase, data)
        