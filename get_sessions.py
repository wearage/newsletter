from pyrogram import Client

accounts = [
    {'api_id': '21651687', 'api_hash': 'a8385bafc46405d91b75fd20b6acdf63', 'phone_number': '+79154630677'},
    {'api_id': '28483067', 'api_hash': '0f4ebb7a430f2b45b671b86974104fe0', 'phone_number': '+79154633945'},
    {'api_id': '29472466', 'api_hash': '757f0391911f2497c9d5be1dfffd9768', 'phone_number': '+79154633153'},
    {'api_id': '22653758', 'api_hash': '165ed6afc9c244ce644b0f13f70b6254', 'phone_number': '+79154627515'},
    {'api_id': '21764047', 'api_hash': '2763182613ce99ce5773b97353e58e56', 'phone_number': '+79154636036'}

]

for account in accounts:
    client = Client(account['phone_number'], account['api_id'], account['api_hash'])
    client.start()
    client.stop()
