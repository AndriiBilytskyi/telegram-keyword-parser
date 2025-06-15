from pyrogram import Client
import pandas as pd

api_id = 26735008
api_hash = '6c35a6247e6b6502e5b79173b22af871'
session_name = "session1"

KEYWORDS = [
    'адвокат', 'адвоката', 'адвокатом', 'адвокату',
    'юрист', 'юриста', 'юристу', 'юристом',
    'помощь адвоката', 'полиция', 'прокуратура',
    'поліція', 'прокурор',
    'lawyer', 'attorney', 'police', 'prosecutor', 'court',
    'anwalt', 'rechtsanwalt', 'polizei', 'staatsanwalt', 'gericht'
]

GROUPS_TO_MONITOR = ['Ukrainer_in_Deutschland']

def parse():
    app = Client(session_name, api_id=api_id, api_hash=api_hash)
    with app:
        all_rows = []
        for chat in GROUPS_TO_MONITOR:
            print(f"Парсим {chat}...")
            for msg in app.get_chat_history(chat):
                if msg.text and any(kw.lower() in msg.text.lower() for kw in KEYWORDS):
                    sender = msg.from_user
                    if not sender or sender.is_bot:
                        continue
                    sender_dict = {
                        'msg_id': msg.message_id,
                        'author_id': sender.id,
                        'username': sender.username,
                        'first_name': sender.first_name,
                        'last_name': sender.last_name,
                        'date': msg.date.isoformat() if msg.date else None,
                        'message_text': msg.text,
                        'reactions': ''
                    }
                    # Реакции (только emoji + count)
                    if msg.reactions:
                        sender_dict['reactions'] = ', '.join([f"{r.emoji} ({r.count})" for r in msg.reactions])
                    all_rows.append(sender_dict)
        df = pd.DataFrame(all_rows)
        df.to_csv('authors_with_keywords.csv', index=False, encoding='utf-8-sig')
        print("Файл authors_with_keywords.csv успешно сохранён.")
        # Автоматически отправить файл себе в Избранные (Saved Messages)
        app.send_document("me", "authors_with_keywords.csv", caption="Выгрузка пользователей по ключевым словам")
        print("Файл отправлен в Избранные Telegram.")

if __name__ == '__main__':
    parse()
