# @Filename:    rule_service.py
# @Time:        19-02-2023 20:58
# @Author  : sachin
# @Email   : spb722@gmail.com
from sklearn.metrics import accuracy_score
from sklearn.metrics import classification_report
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.feature_selection import SelectFromModel
import sklearn
import json
import dask.dataframe as dd
import os
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
import pandas as pd
from icecream import ic
import dask.dataframe as dd
import os
import configuration.config as cfg
import configuration.features as f
import traceback
import numpy as np
from fastapi import Depends, FastAPI, HTTPException
from pathlib import Path
import vaex
import pickle
from mlxtend.frequent_patterns import fpgrowth, association_rules
from pathlib import Path
from sql_app.repositories import AssociationRepo

config = cfg.Config().to_json()
features = f.Features().to_json()



class DecisionTreeConverter(object):

    def __init__(self, my_tree=None, features=None, class_names=None, df=None):
        self.my_tree = my_tree
        self.features = features
        self.class_names = class_names
        self.df = df
        self.json = None

        self.json_string = ""

        # self.recursion(self.my_tree.tree_, 0)

        self.recurse(self.my_tree.tree_, 0)

    def node_to_str(self, tree, node_id, criterion):
        if True:
            criterion = "impurity"

        value = tree.value[node_id]
        if tree.n_outputs == 1:
            value = value[0, :]

        jsonValue = ', '.join([str(x) for x in value])

        if tree.children_left[node_id] == sklearn.tree._tree.TREE_LEAF:
            l = 1
            try:

                probablity = np.round(100.0 * value[l] / np.sum(value), 2)
            except:
                probablity = np.round(100.0 * value[0] / np.sum(value), 2)
            return '"id": "%s", "criterion": "%s", "impurity": "%s", "samples": "%s","recomentedPackProbablity":%s, ' \
                   '"value": [%s]' \
                   % (node_id,
                      criterion,
                      tree.impurity[node_id],
                      tree.n_node_samples[node_id],
                      probablity,
                      jsonValue)
        else:

            if self.features is not None:
                feature = self.features[tree.feature[node_id]]
            else:
                feature = tree.feature[node_id]

            if "=" in feature:
                ruleType = "="
                ruleValue = "false"
            else:
                ruleType = "<="
                ruleValue = "%.2f" % tree.threshold[node_id]

            return '"id": "%s", "rule": "%s %s %s", "%s": "%s", "samples": "%s"' \
                   % (node_id,
                      feature,
                      ruleType,
                      ruleValue,
                      criterion,
                      tree.impurity[node_id],
                      tree.n_node_samples[node_id])

    def recurse(self, tree, node_id, criterion='impurity', parent=None, depth=0):
        tabs = "  " * depth
        self.json_string = ""

        left_child = tree.children_left[node_id]
        right_child = tree.children_right[node_id]

        self.json_string = self.json_string + "\n" + \
                           tabs + "{\n" + \
                           tabs + "  " + self.node_to_str(tree, node_id, criterion)

        if left_child != sklearn.tree._tree.TREE_LEAF:
            self.json_string = self.json_string + ",\n" + \
                               tabs + '  "left": ' + \
                               self.recurse(tree, left_child, criterion=criterion, parent=node_id,
                                            depth=depth + 1) + ",\n" + \
                               tabs + '  "right": ' + \
                               self.recurse(tree,
                                            right_child,
                                            criterion=criterion,
                                            parent=node_id,
                                            depth=depth + 1)

        self.json_string = self.json_string + tabs + "\n" + \
                           tabs + "}"

        return self.json_string

    def recursion(self, tree, node_id, parent=None, depth=0, location=None):
        tabs = " " * depth
        self.json = ""
        left_child = tree.children_left[node_id]
        right_child = tree.children_right[node_id]
        self.json = self.json + tabs + "{" + tabs + " " + self.get_node_to_string(tree, node_id, location)
        print(f"the json got in recursion is {self.json}")
        if left_child != sklearn.tree._tree.TREE_LEAF:
            self.json = self.json + "," + tabs + '"left": ' + self.recursion(tree, left_child, node_id, depth + 1,
                                                                             "left") + "," + \
                        tabs + '"right": ' + self.recursion(tree, right_child, node_id, depth + 1, "right")
            print("json formed inside resursion when not tree leaf  ", self.json)
        self.json = self.json + tabs + tabs + "}"
        print("json formed inside resursion when  tree leaf  ", self.json)
        return self.json

    def get_node_to_string(self, tree, node_id, location):
        value = tree.value[node_id]
        print(f"the value of the node  {node_id}  is -- {value}")
        if tree.n_outputs == 1:
            value = value[0, :]
            print(f" the type of value is {type(value)}")
        json_val = ", ".join([str(x) for x in value])

        if tree.children_left[node_id] == sklearn.tree._tree.TREE_LEAF:
            # l = np.argmax(value)
            l = 1
            try:

                probablity = np.round(100.0 * value[l] / np.sum(value), 2)
            except:
                probablity = np.round(100.0 * value[0] / np.sum(value), 2)

            print(f"the left child is a leaf node ")
            return_val = f' "id": {node_id}, "class":"{self.class_names[l]}" ,"samples":{tree.n_node_samples[node_id]} , "recomentedPackProbablity": {probablity}'
            print(f"the return value of convert tree to json when left child is a leaf node is {return_val}")
            return return_val

        else:
            if self.features is not None:
                print(f" the features are  {self.features} ,   the node is is {node_id}")
                print(location)

                feature = self.features[tree.feature[node_id]]
            else:
                print("no features list given ")

            rule_val = "%.2f" % tree.threshold[node_id]

            if location is not None and location == 'left':
                minimum_value = self.df[feature].min()
                if minimum_value == 0:
                    operator = f"<= {rule_val}"
                else:
                    operator = f'between {round(float(minimum_value), 2)} and {round(float(rule_val), 2)} '

            else:

                maximum_value = self.df[feature].max()
                operator = f'between {round(float(rule_val), 2)} and {round(float(maximum_value), 2)} '

            # no need of samples and values

            return_val = f' "id":{node_id}, "rule": "{feature} {operator} " '
            print(f"the return value of convert tree to json when left child is a  not leaf node is {return_val}")
            return return_val

    def get_json(self):
        if self.json_string is not None:
            return self.json_string


def replace_rule(rule_condition, columns_for_dummies):
    try:
        if rule_condition is None:
            return rule_condition
        name = rule_condition.split("<=")[0]
        if name is None or len(name) == 0:
            return rule_condition

        split_name = name.split(":")
        key = split_name[0]
        if key not in columns_for_dummies:
            return rule_condition
        value = split_name[1]
        value = value.strip()
        rule_m = f"{key} == '{round(float(value), 2)}' "
        return rule_m

    except Exception as e:
        print("the error in replace rule is ", e)
        return rule_condition

def pruneTree(root, columns_for_dummies):
    if root.get('recomentedPackProbablity') is not None and root.get('recomentedPackProbablity') < 50:
        return None

    if root.get('left') is not None:
        root['rule'] = replace_rule(root.get('rule'), columns_for_dummies)
        root['left'] = pruneTree(root.get('left'), columns_for_dummies)
    if root.get('right') is not None:
        root['rule'] = replace_rule(root.get('rule'), columns_for_dummies)
        root['right'] = pruneTree(root.get('right'), columns_for_dummies)
    if root.get('left') is not None or root.get('right') is not None or root.get(
            'recomentedPackProbablity') is not None and root.get('recomentedPackProbablity') >= 50:
        # samples =samples +int(root.get('samples'))
        return root
    else:
        return None


def train_model(data):
    def run_decision_tree(X_train, X_test, y_train, y_test):
        clf = DecisionTreeClassifier(max_leaf_nodes=12)
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)
        print('Accuracy run_decision_tree: ', accuracy_score(y_test, y_pred))
        print("confusion matrix run_decision_tree", confusion_matrix(y_test, y_pred))
        print(classification_report(y_test, y_pred))
        return clf

    X = data.drop(['msisdn', 'label'], axis=1)
    y = data['label']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
    print(f"X_train.shape = {X_train.shape}, X_test.shape =  {X_test.shape}")
    # sel = SelectFromModel(RandomForestClassifier(n_estimators=100, random_state=0, n_jobs=-1))
    sel = SelectFromModel(DecisionTreeClassifier(max_leaf_nodes=12))
    sel.fit(X_train, y_train)
    sel.get_support()
    features = X_train.columns[sel.get_support()]
    print("the feature selection features are ", features)
    X_train_rfc = sel.transform(X_train)
    X_test_rfc = sel.transform(X_test)
    clf1 = run_decision_tree(X_train_rfc, X_test_rfc, y_train, y_test)
    features_importance = list(clf1.feature_importances_)
    dic = {features[i]: features_importance[i] for i in range(len(features))}
    t = None
    try:
        t = pd.read_csv('temp.csv')
        tt = t.append(dic, ignore_index=True)
    except:
        t = pd.DataFrame(columns=list(X_train.columns))
        tt = t.append(dic, ignore_index=True)

    print(tt.head(5))
    tt.to_csv('temp.csv', header=True, index=False)
    return clf1, features


def get_rule_query(segements):

    return None

