from supabase import Client

class SupabaseTransformer:
    def __init__(self) -> None:
        pass

    def report_daily_by_app_table_transform(self, supabase: Client, data: dict):
        report_daily_by_app_query = supabase.table('report_daily_by_app')\
            .select('*')\
            .eq('id_string',data["id_string_day"]).execute()
        if not report_daily_by_app_query.data:
            report_daily_by_app_insert_data = {
                "user_id": data['user_id'],
                "day_id": data['day_id'],
                "id_string": data["id_string_day"],
                "total": data['total']
            }
            supabase.table("report_daily_by_app").insert(report_daily_by_app_insert_data).execute()
        else:
            new_total = report_daily_by_app_query.data[0]['total'] + data['total']
            supabase.table("report_daily_by_app")\
                .update({"total": new_total})\
                .eq('id_string',data["id_string_day"])\
                .execute()