from dotenv import load_dotenv
from supabase import create_client
from worker import Worker
import os

if __name__ == "__main__":
  load_dotenv()
  url: str = os.getenv("SUPABASE_URL")
  key: str = os.getenv("SUPABASE_KEY")
  supabase = create_client(url, key)
  worker = Worker(supabase)
  worker.run()