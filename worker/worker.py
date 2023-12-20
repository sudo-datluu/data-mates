from dotenv import load_dotenv
from supabase import create_client
from processor import Processor
import os

if __name__ == "__main__":
  load_dotenv()
  url: str = os.getenv("SUPABASE_URL")
  key: str = os.getenv("SUPABASE_KEY")
  supabase = create_client(url, key)
  processor = Processor(supabase)
  Processor.run()