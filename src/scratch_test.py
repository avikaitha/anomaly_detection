from Graph import Graph
import json
from datetime import datetime
import traceback


def build_graph(filename):
    with open(filename) as f:
        content = f.readlines()

    init_line = json.loads(content[0])
    D = int(init_line['D'])
    T = int(init_line['T'])
    graph = Graph(D, T)
    content = content[1:]
    print("Total:", len(content))
    count = 0
    for line in content:
        count += 1
        if count % 1000 == 0:
            print(count)
        try:
            obj = json.loads(line)
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

    return graph


def stream_graph(graph, filename):
    with open(filename) as f:
        content = f.readlines()
    count = 0
    for line in content:
        count += 1
        # print("Count:", count)
        if count % 100 == 0:
            print(count)

        try:
            obj = json.loads(line)
            event_type = obj['event_type']
            if event_type == 'purchase':
                timestamp = datetime.strptime(obj['timestamp'], '%Y-%m-%d %H:%M:%S')
                user_id = obj['id']
                amount = obj['amount']
                graph.purchase(user_id, timestamp, amount, batch=False)
            elif event_type == 'befriend':
                user_id = obj['id1']
                friend_id = obj['id2']
                graph.befriend(user_id, friend_id, batch=False)
            elif event_type == 'unfriend':
                user_id = obj['id1']
                friend_id = obj['id2']
                graph.un_friend(user_id, friend_id, batch=False)
        except:
            pass


def get_counts(file_name):
    with open(file_name) as f:
        content = f.readlines()
    init_line = json.loads(content[0])
    try:
        D = int(init_line['D'])
        T = int(init_line['T'])
        content = content[1:]
    except:
        pass
    print("Total:", len(content))
    purchase_count = 0
    befriend_count = 0
    unfriend_count = 0
    for line in content:
        try:
            obj = json.loads(line)
            event_type = obj['event_type']
            if event_type == 'purchase':
                purchase_count += 1
            elif event_type == 'befriend':
                befriend_count += 1
            elif event_type == 'unfriend':
                unfriend_count += 1
        except:
            print("Line:", line)
            traceback.print_exc()
            pass
    print("Purchase Count:", purchase_count, "Percentage:", 100*(purchase_count/len(content)))
    print("Befriend Count:", befriend_count, "Percentage:", 100*(befriend_count/len(content)))
    print("Unfriend Count:",  unfriend_count, "Percentage:", 100*(unfriend_count/len(content)))
    print()

file_path = "../insight_testsuite/tests/test_1/log_input"
graph = build_graph(file_path+"/batch_log.json")
graph.calculate_thresholds()


# print("Graph:", graph.get_graph_dict())
threshold_dict = graph.get_threshold_dict()
print("Total users:",len(threshold_dict))
count = 0
out = open("batch_thresholds.txt",'w')
for key,value in threshold_dict.items():
    if value != float('inf'):
        count += 1
        out.write("User: "+str(key)+" Threshold: "+str(value)+"\n")

out.close()

print("Thresholds:", threshold_dict)
print("Graph:", graph.get_graph_dict())
print("Social Friends:", graph.get_social_friends_dict())
print("Social Transactions:", graph.get_social_transaction_dict())
print()
stream_graph(graph, file_path+"/stream_log.json")

threshold_dict = graph.get_threshold_dict()
print("Total users:",len(threshold_dict))
count = 0
out = open("stream_thresholds.txt",'w')
print("Thresholds:", threshold_dict)
print("Graph:", graph.get_graph_dict())
print("Social Friends:", graph.get_social_friends_dict())
print("Social Transactions:", graph.get_social_transaction_dict())
for key, value in threshold_dict.items():
    if value != float('inf'):
        count += 1
        out.write("User: "+str(key)+" Threshold: "+str(value)+"\n")

out.close()
