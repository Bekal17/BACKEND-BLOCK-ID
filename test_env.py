import os
from dotenv import load_dotenv

load_dotenv()

print("WALLET:", os.getenv("WALLET"))
print("SCORE:", os.getenv("SCORE"))

import os
print("Current dir:", os.getcwd())
