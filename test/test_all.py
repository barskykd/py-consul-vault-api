import logging
import os
import unittest
import subprocess
from time import sleep

from consul_vault_api import *


class TestAll(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        logging.basicConfig(level=logging.DEBUG)

        cls.__consul = subprocess.Popen(["consul", "agent", "-dev"])
        os.environ['CONSUL_HTTP_ADDR'] = "127.0.0.1:8500"
        sleep(2)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.__consul.terminate()

    def test_kv(self):
        c = ConsulClientV1()
        c.kv_set("a/b/c", "123")
        c.kv_set("a/b/d", "456")
        r = c.kv_get("a/b/c")
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0].Key, "a/b/c")
        self.assertEqual(r[0].get_value_str(), "123")

        r = c.kv_get("a/b/c", raw=True)
        self.assertEqual(r, b"123")

        r = c.kv_get("a", recurse=True)
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0].Key, "a/b/c")
        self.assertEqual(r[0].get_value_str(), "123")

        r = c.kv_get("a", keys=True)
        self.assertCountEqual(r, ['a/b/c', 'a/b/d'])

        r = c.kv_get("a", keys=True, separator='b')
        self.assertCountEqual(r, ['a/b'])




if __name__ == '__main__':
    unittest.main()
