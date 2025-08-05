from airflow.providers.telegram.hooks.telegram import TelegramHook
from dotenv import load_dotenv
import os

#загружаем переменные из .env
load_dotenv()

TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHATID = os.getenv("TG_CHATID")

def send_success_message(context):
    hook = TelegramHook(
        token=TG_TOKEN,
        chat_id=TG_CHATID
    )
    dag = context['dag'].dag_id
    run_id = context['run_id']
    
    message = f'✅ Исполнение DAG {dag} с id={run_id} прошло успешно!'
    hook.send_message({
        'text': message
    })

def send_failure_message(context):
    hook = TelegramHook(
        token=TG_TOKEN,
        chat_id=TG_CHATID
    )
    dag = context['dag'].dag_id
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
    
    message = f'❌ Исполнение DAG {dag} ({task_instance_key_str}) с id={run_id} провалилось!'
    hook.send_message({
        'text': message
    })