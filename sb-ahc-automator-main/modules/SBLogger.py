import datetime


class SBLogger:
    def __init__(self, show_timestamp):
        self.show_timestamp = show_timestamp

    def current_time(self):
        if self.show_timestamp:
            current_datetime = datetime.datetime.now()
            formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
            return formatted_datetime + " "
        else:
            return ""

    def error(self, message):
        print(f"{self.current_time()}\033[91mERROR\033[00m \033[37m::\033[00m {message}")

    def info(self, message):
        print(f"{self.current_time()}\033[94mINFO\033[00m \033[37m::\033[00m {message}")

    def element_info(self, message):
        print(f"{self.current_time()}- \033[37m{message}\033[00m")

    def warning(self, message):
        print(f"{self.current_time()}\033[93mWARNING\033[00m \033[37m::\033[00m {message}")

    def check_start(self, check_name):
        label = check_name.replace("_", " ").upper()
        bar = "─" * 50
        print(f"\n\033[96m{bar}\033[00m")
        print(f"\033[96m  ▶  {label}\033[00m")
        print(f"\033[96m{bar}\033[00m")

    def check_done(self, check_name):
        label = check_name.replace("_", " ").upper()
        print(f"\033[92m  ✔  {label} — DONE\033[00m")

    def check_failed(self, check_name, error_msg=""):
        label = check_name.replace("_", " ").upper()
        err = f": {error_msg[:60]}..." if len(error_msg) > 60 else (f": {error_msg}" if error_msg else "")
        print(f"\033[91m  ✗  {label} — CHECK FAILED{err}\033[00m")
