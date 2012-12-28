# -*- coding: utf-8 -*-
# Copyright (c) 2012, Google, Inc.
# All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

"""Unit tests for GS versioning support."""

import StringIO
import os
import tempfile
import time
from boto.exception import GSResponseError
from boto.gs.connection import GSConnection
from tests.integration.gs.util import has_google_credentials
from tests.unit import unittest


@unittest.skipUnless(has_google_credentials(),
                     "Google credentials are required to run the Google "
                     "Cloud Storage tests.  Update your boto.cfg to run "
                     "these tests.")
class GSVersioningTest(unittest.TestCase):
    gs = True

    def setUp(self):
        self.conn = GSConnection()
        self.buckets = []

    def tearDown(self):
        for b in self.buckets:
            bucket = self.conn.get_bucket(b)
            while len(list(bucket.list_versions())) > 0:
                for k in bucket.list_versions():
                    bucket.delete_key(k.name, generation=k.generation)
            bucket.delete()

    def _BucketName(self):
        b = "boto-gs-test-%s" % repr(time.time()).replace(".", "-")
        self.buckets.append(b)
        return b

    def _Bucket(self):
        b = self.conn.create_bucket(self._BucketName())
        return b

    def testConditionalSetContentsFromFile(self):
        b = self._Bucket()
        k = b.new_key("foo")
        s1 = "test1"
        fp = StringIO.StringIO(s1)
        try:
            k.set_contents_from_file(fp, if_generation=999)
        except GSResponseError as e:
            self.assertEqual(e.status, 412)

        fp = StringIO.StringIO(s1)
        k.set_contents_from_file(fp, if_generation=0)
        k = b.get_key("foo")
        g1 = k.generation

        s2 = "test2"
        fp = StringIO.StringIO(s2)
        try:
            k.set_contents_from_file(fp, if_generation=int(g1)+1)
        except GSResponseError as e:
            self.assertEqual(e.status, 412)

        fp = StringIO.StringIO(s2)
        k.set_contents_from_file(fp, if_generation=g1)
        k = b.get_key("foo")
        self.assertEqual(k.get_contents_as_string(), s2)

    def testConditionalSetContentsFromString(self):
        b = self._Bucket()
        k = b.new_key("foo")
        s1 = "test1"
        try:
            k.set_contents_from_string(s1, if_generation=999)
        except GSResponseError as e:
            self.assertEqual(e.status, 412)

        k.set_contents_from_string(s1, if_generation=0)
        k = b.get_key("foo")
        g1 = k.generation

        s2 = "test2"
        try:
            k.set_contents_from_string(s2, if_generation=int(g1)+1)
        except GSResponseError as e:
            self.assertEqual(e.status, 412)

        k.set_contents_from_string(s2, if_generation=g1)
        k = b.get_key("foo")
        self.assertEqual(k.get_contents_as_string(), s2)

    def testConditionalSetContentsFromFilename(self):
        s1 = "test1"
        s2 = "test2"
        f1 = tempfile.NamedTemporaryFile(prefix="boto-gs-test", delete=False)
        f2 = tempfile.NamedTemporaryFile(prefix="boto-gs-test", delete=False)
        fname1 = f1.name
        fname2 = f2.name
        f1.write(s1)
        f1.close()
        f2.write(s2)
        f2.close()

        try:
            b = self._Bucket()
            k = b.new_key("foo")

            try:
                k.set_contents_from_filename(fname1, if_generation=999)
            except GSResponseError as e:
                self.assertEqual(e.status, 412)

            k.set_contents_from_filename(fname1, if_generation=0)
            k = b.get_key("foo")
            g1 = k.generation

            try:
                k.set_contents_from_filename(fname2, if_generation=int(g1)+1)
            except GSResponseError as e:
                self.assertEqual(e.status, 412)

            k.set_contents_from_filename(fname2, if_generation=g1)
            k = b.get_key("foo")
            self.assertEqual(k.get_contents_as_string(), s2)
        finally:
            os.remove(fname1)
            os.remove(fname2)
