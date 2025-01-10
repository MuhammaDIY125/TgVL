import re


def clean_text(text :str, source :str):
    """
    Очищает сообщения для уменьшения количества токенов.

    В некоторых телеграм каналах есть такие части текста, которые повторяются в каждой вакансии. Для уменьшения количества токенов я удаляю эти ненужные части и в конце удаляю ненужные символы и смайлики.
    """
    if source == 'UstozShogird':
        text = text.split("@UstozShogird")[0].strip()

    elif source == 'uzdev_jobs':
        text = text.split("Подписаться на канал @UzDev_Jobs")[0].strip()

    elif source == 'kasbim_uz':
        text = text.split("@kasbim_uz")[0].strip()

    elif source == 'data_ish':
        text = text.split("@data_ish")[0].strip()

    elif source == 'rizqimuz':
        text = text.split("@rizqimuz")[0].strip()

    elif source == 'upjobsuz':
        text = text.split("E'lon joylash")[0].strip()

    elif source == 'ayti_jobs':
        text = text.split("Rasmiy kanal")[0].strip()

    elif source == 'freelance_link':
        text = text.split("@freelance_link")[0].strip()

    elif source == 'click_jobs':
        text = text.split("@click_jobs")[0].strip()

    text = re.sub(r'[^\w\s.,!?()\[\]{}:;\'\"–\-–$€¥₴₽@]', '', text)  
    text = re.sub(r'[\u200b-\u200d\u2060-\u206f\ufeff]', '', text)
    return text.strip()
