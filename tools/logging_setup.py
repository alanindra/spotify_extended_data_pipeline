import logging
from datetime import datetime
from tools.config import path

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
        level=logging.ERROR,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info("Logging setup successful. Log file: %s", log_file)