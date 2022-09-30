# data = DB['search_products'].aggregate([
#     {
#         '$search': {
#             'compound': {
#                 'should': [
#                     {
#                         'autocomplete': {
#                             'query': 'milk', 
#                             'path': 'name'
#                         }
#                     }, {
#                         'autocomplete': {
#                             'query': 'milk', 
#                             'path': 'barcode'
#                         }
#                     }
#                 ]
#             }
#         }
#     }, {
#         '$project': {
#             'name': 1
#         }
#     }
# ])

# count = 0
# payload = []
# for i in data:
# 	sounds_like = 'doodh' + ' ' + i.get('name')
# 	payload.append(UpdateOne({'_id': i.get('_id')}, {'$set': {"sounds_like": sounds_like}}))
# DB['search_products'].bulk_write(payload)




# def get_sounds_like(name):
# 	items = name.split(' ')
# 	sounds_like = []
# 	for j in items:
# 		a, b = doublemetaphone(j)
# 		if a:
# 			sounds_like.append(a)
# 		if b:
# 			sounds_like.append(b)
# 	sounds_like = ' '.join(set(sounds_like))
# 	return sounds_like

# DB['search_products'].update_many({}, {'$unset': "sounds_like"})