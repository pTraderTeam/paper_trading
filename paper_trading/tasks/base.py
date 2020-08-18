from apscheduler.schedulers.background import BackgroundScheduler

from ..tasks.dr import ex_dividend
from ..tasks.stocks import sync_data


def init_tasks(app, engine):
    scheduler = BackgroundScheduler()
    scheduler.add_job(sync_data, "cron", day_of_week="mon-fri", hour=15, minute=10)
    scheduler.add_job(ex_dividend, "cron", day_of_week="mon-fri", hour=1)
    scheduler.start()
