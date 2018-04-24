#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: furushchev <furushchev@jsk.imi.i.u-tokyo.ac.jp>

from bson import json_util
import pymongo
import subprocess
import rospy
import unittest
from mongodb_store.util import import_MongoClient, wait_for_mongo
from mongodb_store.message_store import MessageStoreProxy
from geometry_msgs.msg import Wrench, Pose


class TestReplication(unittest.TestCase):
    def test_replication(self):
        replication_db = "replication_test"
        replication_col = "replication_test"
        # connect to destination for replication
        self.assertTrue(wait_for_mongo(), "wait for mongodb server")
        try:
            dst_client = import_MongoClient()("localhost", 49163)
            count = dst_client[replication_db][replication_col].count()
            self.assertEqual(count, 0, "No entry in destination")
        except pymongo.errors.ConnectionFailure:
            self.fail("Failed to connect to destination for replication")

        # insert an entry to move
        msg_store = MessageStoreProxy(
            database=replication_db, collection=replication_col)
        msg = Wrench()
        msg_name = "replication test message"
        msg_store.insert_named(msg_name, msg)

        # move entries
        retcode = subprocess.check_call([
            'rosrun', 'mongodb_store', 'replicator_client.py',
            '--move-before', '0',
            replication_db, replication_col])
        self.assertEqual(retcode, 0, "replicator_client returns code 0")

        # check if replication was succeeded
        count = dst_client[replication_db][replication_col].count()
        self.assertGreater(count, 0, "entry moved to the destination")

        # test deletion after move
        data, meta = msg_store.query_named(msg_name, Wrench._type)
        self.assertIsNotNone(data, "entry is still in source")
        retcode = subprocess.check_call([
            'rosrun', 'mongodb_store', 'replicator_client.py',
            '--move-before', '0',
            '--delete-after-move',
            replication_db, replication_col])
        self.assertEqual(retcode, 0, "replicator_client returns code 0")
        data, meta = msg_store.query_named("replication test", Wrench._type)
        self.assertIsNone(data, "moved entry is deleted from source")

    def test_replication_with_query(self):
        replication_db = "replication_test_with_query"
        replication_col = "replication_test_with_query"
        # connect to destination for replication
        self.assertTrue(wait_for_mongo(), "wait for mongodb server")
        try:
            dst_client = import_MongoClient()("localhost", 49163)
            count = dst_client[replication_db][replication_col].count()
            self.assertEqual(count, 0, "No entry in destination")
        except pymongo.errors.ConnectionFailure:
            self.fail("Failed to connect to destination for replication")

        # insert an entry to move
        msg_store = MessageStoreProxy(
            database=replication_db, collection=replication_col)
        for i in range(5):
            msg = Wrench()
            msg.force.x = i
            msg_store.insert(msg)
            msg = Pose()
            msg.position.x = i
            msg_store.insert(msg)

        # move entries with query
        query = {'_meta.stored_type': Pose._type}
        retcode = subprocess.check_call([
            'rosrun', 'mongodb_store', 'replicator_client.py',
            '--move-before', '0',
            '--query', json_util.dumps(query),
            replication_db, replication_col])
        self.assertEqual(retcode, 0, "replicator_client returns code 0")

        # check if replication was succeeded
        count = dst_client[replication_db][replication_col].count()
        self.assertEqual(count, 5, "replicated entry exists in destination")


if __name__ == '__main__':
    import rostest
    rospy.init_node("test_replication")
    rostest.rosrun("mongodb_store", "test_replication", TestReplication)
