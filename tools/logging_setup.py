import logging
from datetime import datetime
from tools.config import path
import json

logger = logging.getLogger(__name__)

def setup_logging():
    path["logs"].mkdir(parents=True, exist_ok=True)
    log_file = path["logs"] / "app.log"

    date_str = datetime.now().strftime("%Y-%m-%d")
    log_file = path["logs"] / f"{date_str}.log"

    counter = 2
    while log_file.exists():
        log_file = path["logs"] / f"{date_str}_{counter}.log"
        counter += 1

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file)
        ]
    )

    logger.info("Logging setup successful.")

class EventLogger:
    def __init__ (self):
        self.data = {
            "job_run_date": datetime.now().strftime("%Y-%m-%d"),
            "table_history": []
        }

    def create_event_log(self, event, table_name, status, state, message=""):
        self.data["table_history"].append({
            "update_timestamp": datetime.now().isoformat(),
            "event": event,
            "table_name": table_name,
            "status": status,
            "state":state,
            "message":message
        })
    
    def save_event_log(self, file_path):
        with open(file_path, "w") as f:
            json.dump(self.data, f, indent=4)