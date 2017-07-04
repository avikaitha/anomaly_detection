import json
from datetime import datetime
import traceback
from Graph import Graph


def build_graph():
    with open("../log_input/batch_log.json") as f:
        content = f.readlines()

    init_line = json.loads(content[0])
    D = int(init_line['D'])
    T = int(init_line['T'])
    graph = Graph(D,T)
    content = content[1:]
    for line in content:
        obj = json.loads(line)
        try:
            event_type = obj['event_type']
            if event_type == 'purchase':
                timestamp = datetime.strptime(obj['timestamp'], '%Y-%m-%d %H:%M:%S')
                user_id = obj['id']
                amount = obj['amount']
                graph.purchase(user_id, timestamp, amount)
            elif event_type == 'befriend':
                user_id = obj['id1']
                friend_id = obj['id2']
                graph.befriend(user_id, friend_id)
            elif event_type == 'unfriend':
                user_id = obj['id1']
                friend_id = obj['id2']
                graph.un_friend(user_id, friend_id)
        except:
            pass


def stream_graph():
    with open("../log_input/stream_log.json") as f:
        content = f.readlines()


def main():
    build_graph()


if __name__ == "__main__":
    main()
