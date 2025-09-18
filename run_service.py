#!/usr/bin/env python3
import os

import uvicorn
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

if __name__ == "__main__":
    uvicorn.run(
        "propel_data_platform.api.main:app",
        host="127.0.0.1",
        port=8000,
        # reload=True,  # Enable auto-reload on code changes
        # reload_dirs=["src/propel_data_platform"],  # Watch these directories for changes
        log_level="debug",
        workers=10,
    )
