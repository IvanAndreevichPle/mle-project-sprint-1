from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма

def send_telegram_success_message(context): # на вход принимаем словарь с контекстными переменными
    hook = TelegramHook(token='8533497468:AAF0d3dxHUgc2RM49ubhPD9u_C6zxg9yIpg', chat_id='-5094498712')
    dag = context['dag'].dag_id
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({'text': message}) # отправление сообщения

def send_telegram_failure_message(context):
    hook = TelegramHook(token='8533497468:AAF0d3dxHUgc2RM49ubhPD9u_C6zxg9yIpg', chat_id='-5094498712')
    # Получаем dag_id безопасным способом
    if 'dag_id' in context:
        dag_id = context['dag_id']
    elif 'dag' in context and hasattr(context['dag'], 'dag_id'):
        dag_id = context['dag'].dag_id
    else:
        dag_id = str(context.get('dag', 'unknown'))
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
    
    message = f'Исполнение DAG {dag_id} с id={run_id} завершилось неудачно! Задача: {task_instance_key_str}' # определение текста сообщения
    hook.send_message({'text': message}) # отправление сообщения