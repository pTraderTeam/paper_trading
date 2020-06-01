from apscheduler.schedulers.background import BackgroundScheduler

from paper_trading.tasks.stocks import sync_data


def init_tasks(app, engine):
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        sync_data,
        "cron",
        day_of_week="mon-fri",
        hour=15,
        minute=10
    )
    scheduler.start()
