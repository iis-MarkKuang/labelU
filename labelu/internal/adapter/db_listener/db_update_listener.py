from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker
from apscheduler.schedulers.background import BackgroundScheduler

from labelu.internal.common.config import settings
from labelu.internal.domain.models.task import Task
from loguru import logger
# 配置日志

# 创建数据库会话
engine = create_engine(settings.DATABASE_URL)
Session = sessionmaker(bind=engine)

# 内存缓存 - 存储最新的任务数据
latest_task_cache = None


def get_latest_task_from_db():
    """从数据库获取最新的任务"""
    # sql alchemy sessions need to be created each time for it to be aware of the latest changes.
    session = Session()

    try:
        # 查询最新的一条任务记录
        latest_task = session.query(Task) \
            .filter(Task.deleted_at == None) \
            .order_by(desc(Task.updated_at)) \
            .first()

        return latest_task
    except Exception as e:
        logger.error(f"获取最新任务失败: {e}")
        return None
    finally:
        session.close()


def check_and_process_updates():
    global latest_task_cache

    current_latest_task = get_latest_task_from_db()

    if current_latest_task is None:
        logger.info("数据库中没有任务记录")
        return

    logger.info(f"current_latest_task id: {current_latest_task.id}")


    if latest_task_cache is None:
        logger.info(f"初始化缓存: 最新任务 ID={current_latest_task.id}")
        latest_task_cache = current_latest_task
        return

    # Compare by id or updated_at
    if current_latest_task.id > latest_task_cache.id or \
       current_latest_task.updated_at > latest_task_cache.updated_at:
        logger.info(f"检测到任务更新: ID={current_latest_task.id}, 更新时间={current_latest_task.updated_at}")
        process_task_update(current_latest_task)
        latest_task_cache = current_latest_task
    else:
        logger.info("没有检测到任务更新")

def process_task_update(task):
    """处理任务更新的业务逻辑"""
    logger.info(f"处理任务更新: ID={task.id}, 名称={task.name}")

    # 这里可以添加任何需要执行的业务逻辑
    # 例如发送通知、更新缓存、触发工作流等

    # 示例：打印任务详情
    print(f"任务详情: {task.__dict__}")


def start_polling(interval_seconds=30):
    """启动定时轮询任务"""
    scheduler = BackgroundScheduler()
    logger.info(f"即将启动任务轮询")

    # 添加定时任务，每30秒执行一次
    scheduler.add_job(
        func=check_and_process_updates,
        trigger='interval',
        seconds=interval_seconds,
        id='task_polling_job'
    )

    # 启动调度器
    scheduler.start()
    logger.info(f"已启动任务轮询，间隔: {interval_seconds}秒")

    # 保持主线程运行
    try:
        while True:
            # 可以添加其他主线程逻辑
            pass
    except (KeyboardInterrupt, SystemExit):
        # 关闭调度器
        scheduler.shutdown()
