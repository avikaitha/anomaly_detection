import math
import heapq
import utils


class UserNotFoundException(Exception):
    pass


class Graph:
    def __init__(self, D, T, graph_dict=None,
                 social_transactions_dict=None,
                 threshold_dict=None, social_friends_dict=None,
                 own_transactions=None):
        if graph_dict is None:
            graph_dict = {}

        if social_transactions_dict is None:
            social_transactions_dict = {}

        if threshold_dict is None:
            threshold_dict = {}

        if social_friends_dict is None:
            social_friends_dict = {}

        if own_transactions is None:
            own_transactions = {}

        self.D = D
        self.T = T
        self.__graph_dict = graph_dict
        self.__social_transactions_dict = social_transactions_dict
        self.__own_transactions = own_transactions
        self.__all_transactions = {}
        self.__threshold_dict = threshold_dict
        self.__social_friends_dict = social_friends_dict

    def get_friends_list(self, user_id, d=None, friends_list=None, crawled_set=None):
        if d is None:
            d = self.D
            crawled_set = set([user_id])
            friends_list = set()
        if d == 0:
            return friends_list
        try:
            new_list = set()
            new_list |= self.__graph_dict[user_id]
            new_list -= crawled_set
        except KeyError:
            return friends_list
        friends_list |= new_list
        for friend in new_list:
            self.get_friends_list(friend, d-1, friends_list,crawled_set)

        return friends_list

    def add_user(self, user_id):
        self.__graph_dict[user_id] = set()
        self.__threshold_dict[user_id] = math.inf

    def propagate_to_friends(self, user_id, befriend, batch=True):
        friends = self.get_friends_list(user_id)
        friends.discard(user_id)
        for friend in friends:
            if befriend:
                if friend not in self.__social_friends_dict:
                    self.__social_friends_dict[friend] = set([user_id])
                else:
                    self.__social_friends_dict[friend].add(user_id)

                if user_id in self.__own_transactions:
                    if friend not in self.__social_transactions_dict:
                        self.__social_transactions_dict[friend] = []
                    self.__social_transactions_dict[friend] = list(heapq.merge(self.__social_transactions_dict[friend],
                                                                               self.__own_transactions[user_id]))

                    while len(self.__social_transactions_dict[friend]) > 50:
                        heapq.heappop(self.__social_transactions_dict[friend])

            else:

                if friend not in self.__social_friends_dict:
                    raise UserNotFoundException
                else:
                    self.__social_friends_dict[friend].discard(user_id)

                    if user_id in self.__all_transactions:
                        if friend not in self.__social_transactions_dict:
                            self.__social_transactions_dict[friend] = []
                        else:
                            for purchase in self.__all_transactions[user_id]:
                                print("Friend", friend, purchase, self.__social_transactions_dict[friend])
                                self.__social_transactions_dict[friend].remove(purchase)
                                print("Friend", friend, purchase, self.__social_transactions_dict[friend])
                                print("----------------------------")

                            heapq.heapify(self.__social_transactions_dict[friend])
                print("Trans:", self.__social_transactions_dict)
        if not batch:
            self.calculate_thresholds(user_id)

    def befriend(self, user_id, friend_id, batch=True):
        if user_id not in self.__graph_dict:
            self.add_user(user_id)
        if friend_id not in self.__graph_dict:
            self.add_user(friend_id)
        self.__graph_dict[user_id].add(friend_id)
        self.__graph_dict[friend_id].add(user_id)
        self.propagate_to_friends(user_id, True, batch)
        self.propagate_to_friends(friend_id, True, batch)

    def un_friend(self, user_id, friend_id, batch=True):
        if user_id not in self.__graph_dict:
            raise UserNotFoundException(str(user_id)+" not found!!")
        self.__graph_dict[user_id].discard(friend_id)
        self.__graph_dict[friend_id].discard(user_id)
        self.propagate_to_friends(user_id, False, batch)
        self.propagate_to_friends(friend_id, False, batch)

    def purchase(self, user_id, timestamp, amount, batch=True):
        if user_id not in self.__graph_dict:
            self.add_user(user_id)

        if not batch:
            if float(amount) >= self.__threshold_dict[user_id]:
                print("Flagged:", user_id, timestamp, amount, "threshold:", self.__threshold_dict[user_id])

        if user_id not in self.__all_transactions:
            self.__all_transactions[user_id] = [(timestamp, float(amount))]
        else:
            if len(self.__all_transactions[user_id]) < self.T:
                heapq.heappush(self.__all_transactions[user_id], (timestamp, float(amount)))
            else:
                heapq.heappushpop(self.__all_transactions[user_id], (timestamp, float(amount)))

        if user_id not in self.__social_friends_dict:
            if user_id not in self.__own_transactions:
                self.__own_transactions[user_id] = [(timestamp, float(amount))]
            else:
                if len(self.__own_transactions[user_id]) < self.T:
                    heapq.heappush(self.__own_transactions[user_id], (timestamp, float(amount)))
                else:
                    heapq.heappushpop(self.__own_transactions[user_id], (timestamp, float(amount)))
        else:
            friends = self.__social_friends_dict[user_id]

            for friend in friends:
                if friend not in self.__social_transactions_dict:
                    self.__social_transactions_dict[friend] = [(timestamp, float(amount))]
                else:
                    if len(self.__social_transactions_dict[friend]) < self.T:
                        heapq.heappush(self.__social_transactions_dict[friend], (timestamp, float(amount)))
                    else:
                        heapq.heappushpop(self.__social_transactions_dict[friend], (timestamp, float(amount)))
        if not batch:
            self.calculate_thresholds(user_id)

    def calculate_thresholds(self, user=None):
        if user is None:
            user_ids = self.__graph_dict.keys()
        else:
            user_ids = self.__social_friends_dict[user]
        for user_id in user_ids:
            try:
                if len(self.__social_transactions_dict[user_id]) >= 2:
                    m = utils.mean(self.__social_transactions_dict[user_id])
                    sd = utils.std_dev(self.__social_transactions_dict[user_id])

                    self.__threshold_dict[user_id] = m + (3 * sd)
            except:
                pass

    def get_graph_dict(self):
        return self.__graph_dict

    def get_threshold_dict(self):
        return self.__threshold_dict

    def get_social_transaction_dict(self):
        return self.__social_transactions_dict

    def get_social_friends_dict(self):
        return self.__social_friends_dict






