from pyrogram import Client

accounts = [
    {'api_id': 'YOUR_API_ID', 'api_hash': 'YOUR_API_HASH', 'phone_number': 'YOUR_PHONE_NUMBER'}
]

for account in accounts:
    client = Client(account['phone_number'], account['api_id'], account['api_hash'])
    client.start()
    client.stop()
