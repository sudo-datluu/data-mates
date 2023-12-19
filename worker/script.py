event1 = {
  "userid": 123,
  "app": "facebook",
  "category": "social",
  "timestamp": 1702814796,
  "type": "open" #Must be open, switch, close
}


event2 = {
  "userid": 123,
  "app": "youtube",
  "category": "entertainment",
  "timestamp": 1702818712,
  "type": "switch" #Must be open, switch, close
}
{'category':"social",}


'''
    open -> switch
    open -> close

    switch -> switch
    switch -> close

    close -> open
    '''

def handle_event(event1, event2):
    event_types = {'open', 'switch', 'close'}

    if event1['type'] in event_types and event2['type'] in event_types:
        time_diff = event2['timestamp'] - event1['timestamp']
        dict_result = {
            "category": event1["category"],
            "userid": event1["userid"],
            "total": int(time_diff / 60),
            "app": event1["app"]
        }
        
        if (event1["type"] == 'open' and event2["type"] in {'switch', 'close'}) or \
           (event1["type"] == 'switch' and event2["type"] in {'switch', 'close'}) or \
           (event1["type"] == 'close' and event2["type"] == 'open'):
            return dict_result

    return None

print(handle_event(event1, event2))
