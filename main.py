from telethon import TelegramClient
from telethon.tl.functions.messages import GetMessageReactionsList
import pandas as pd
import asyncio

api_id = 26735008
api_hash = '6c35a6247e6b6502e5b79173b22af871'
session_name = 'session1'
your_username = 'Andrii_Bilytskyi'

KEYWORDS = [
    'адвокат', 'адвоката', 'адвокатом', 'адвокату',
    'юрист', 'юриста', 'юристу', 'юристом',
    'помощь адвоката', 'полиция', 'прокуратура',
    'поліція', 'прокурор',
    'lawyer', 'attorney', 'police', 'prosecutor', 'court',
    'anwalt', 'rechtsanwalt', 'polizei', 'staatsanwalt', 'gericht'
]

GROUPS_TO_MONITOR = ['Ukrainer_in_Deutschland']

async def main():
    client = TelegramClient(session_name, api_id, api_hash)
    await client.start()
    print("Telegram client запущен")

    all_rows = []

    for group in GROUPS_TO_MONITOR:
        print(f"Обрабатываем группу: {group}")
        entity = await client.get_entity(group)
        async for msg in client.iter_messages(entity, reverse=True):
            if msg.text and any(kw.lower() in msg.text.lower() for kw in KEYWORDS):
                sender = await msg.get_sender()
                # Пропуск сообщений от ботов и системных пользователей
                if not sender or getattr(sender, 'bot', False):
                    continue

                sender_dict = {
                    'msg_id': msg.id,
                    'author_id': sender.id,
                    'username': getattr(sender, 'username', None),
                    'first_name': getattr(sender, 'first_name', None),
                    'last_name': getattr(sender, 'last_name', None),
                    'date': msg.date.isoformat() if msg.date else None,
                    'message_text': msg.text,
                    'reactions': '',
                    'reaction_users': ''
                }

                # Получаем реакции
                reactions_list = []
                reaction_users_list = []
                if msg.reactions:
                    try:
                        result = await client(GetMessageReactionsList(
                            peer=entity,
                            id=msg.id,
                            reaction=None,
                            limit=100
                        ))
                        for reaction_info in result.reactions:
                            reaction_type = getattr(reaction_info.reaction, 'emoticon', None)
                            user_id = reaction_info.peer_id.user_id
                            user_obj = next((u for u in result.users if u.id == user_id), None)
                            # Исключаем ботов среди пользователей с реакциями
                            if user_obj and not getattr(user_obj, 'bot', False):
                                username = getattr(user_obj, 'username', None)
                                first_name = getattr(user_obj, 'first_name', None)
                                last_name = getattr(user_obj, 'last_name', None)
                                reaction_users_list.append(
                                    f"{user_id} ({username}, {first_name} {last_name}): {reaction_type}"
                                )
                                reactions_list.append(reaction_type)
                    except Exception as ex:
                        print(f"Не удалось получить реакции для сообщения {msg.id}: {ex}")

                sender_dict['reactions'] = ', '.join(set(filter(None, reactions_list)))
                sender_dict['reaction_users'] = ' | '.join(reaction_users_list)
                all_rows.append(sender_dict)

    df = pd.DataFrame(all_rows)
    df.to_csv('authors_with_keywords.csv', index=False, encoding='utf-8-sig')
    print('Готово! Данные сохранены в authors_with_keywords.csv')

    # Автоматически отправить файл в "Избранные"
    await client.send_file('me', 'authors_with_keywords.csv', caption='Выгрузка пользователей по ключевым словам')
    print('Файл authors_with_keywords.csv отправлен в Избранные (Saved Messages) Telegram')

if __name__ == '__main__':
    asyncio.run(main())
