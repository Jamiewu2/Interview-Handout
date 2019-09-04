"""
    DO NOT MODIFY

    An in-memory store to mimic some operations available in PyMongo
"""

from time import sleep
import random

class DB:

    def __init__(self):
        self.store = {}


    def _find(self, obj, many=False):
        """
            Helper function for shared functionality of find one and many
            Externally, use find_one or find_many
        """

        if not type(obj) is dict:
            raise Exception("Query Object must be key/value")
        found = []
        for _, value in self.store.items():
            matched = True
            for k, v in obj.items():
                if k not in value or value[k] != v:
                    matched = False
            if matched:
                if not many:
                    return value
                found.append(value)
        if not many:
            return None
        return found


    def count(self, obj):
        """
            Returns the number of documents matching the query obj

            Args
                obj: a dict representing a filter on exact matches in the store
                Ex: { key1: value1, key2: value2, ...}

            Returns
                an integer representing how many objects in store match filter
        """

        if not type(obj) is dict:
            raise Exception("Query Object must be key/value")
        count = 0
        for _, value in self.store.items():
            matched = True
            for k, v in obj.items():
                if k not in value or value[k] != v:
                    matched = False
            if matched:
                count += 1
        return count


    def find_many(self, obj):
        """
            Args:
                obj: a dict representing a filter on exact matches in the store
                Ex: { key1: value1, key2: value2, ...}

            Returns:
                An array of objects that match the filter
        """

        return self._find(obj, True)

    def find_one(self, obj):
        """
            Args:
                obj: a dict representing a filter on exact matches in the store
                Ex: { key1: value1, key2: value2, ...}

            Returns:
                A object that matches the filter or None
        """

        return self._find(obj, False)


    def delete_inserts_on_failure(self, inserted_bulk_obj):
        if not type(inserted_bulk_obj) is list:
            raise Exception("Bulk DB Object must be list")
        for obj in inserted_bulk_obj:
            self._delete(obj)


    def insert_many(self, bulk_obj):
        if not type(bulk_obj) is list:
            raise Exception("Bulk DB Object must be list")
        for i in range(len(bulk_obj)):
            obj = bulk_obj[i]
            try:
                self.insert_one(obj)
            except Exception as e:
                # on failure remove successful inserts than reraise exception
                self.delete_inserts_on_failure(bulk_obj[0:i])
                raise e


    def insert_one(self, obj):
        """
            Insert puts an object into the store
            It must contain an _id field, as the internal data structure
            is a dictionary with key=_id

            Args:
                obj: a dict representing a filter on exact matches in the store
                Ex: { key1: value1, key2: value2, ...}
        """

        if not type(obj) is dict:
            raise Exception("DB Object must be key/value")
        if '_id' not in obj:
            raise Exception("DB Object must provide _id key")
        # simulate network latency
        sleep(random.random())
        key = obj['_id']
        if key in self.store:
            raise Exception("DuplicateKeyError")
        else:
            self.store[key] = obj


    def _delete(self, obj, many=False):
        """
            Helper function for shared functionality of delete one and many
        """

        if not type(obj) is dict:
            raise Exception("Query Object must be key/value")
        matches = set()
        for key, value in self.store.items():
            matched = True
            for k, v in obj.items():
                if k not in value or value[k] != v:
                    matched = False
            if matched:
                if not many:
                    del self.store[key]
                    return
                matches.add(key)
        for key in matches:
            del self.store[key]


    def delete_one(self, obj):
        """
            Deletes one entry that matches provided filters

            Args:
                obj: a dict representing a filter on exact matches in the store
                Ex: { key1: value1, key2: value2, ...}
        """

        self._delete(obj)


    def delete_many(self, obj):
        """
            Deletes all entries that match provided filters

            Args:
                obj: a dict representing a filter on exact matches in the store
                Ex: { key1: value1, key2: value2, ...}
        """

        self._delete(obj, many=True)


    def update_one(self, obj_filter, update):
        """
            Update one object that matches provided filter

            Args:
                obj: a dict representing a filter on exact matches in the store
                    Ex: { key1: value1, key2: value2, ...}
                update: a dict representing new key values for matches
        """

        for key, value in self.store.items():
            matched = True
            for k, v in obj_filter.items():
                if k not in value or value[k] != v:
                    matched = False
            if matched:
                for prop_key, new_value in update.items():
                    self.store[key][prop_key] = new_value
                return self.store[key]


    def update_many(self, obj_filter, update):
        """
            Update all objects that matches provided filter

            Args:
                obj: a dict representing a filter on exact matches in the store
                    Ex: { key1: value1, key2: value2, ...}
                update: a dict representing new key values for matches
        """

        for key, value in self.store.items():
            matches = []
            matched = True
            for k, v in obj_filter.items():
                if k not in value or value[k] != v:
                    matched = False
            if matched:
                for prop_key, new_value in update.items():
                    self.store[key][prop_key] = new_value
                matches.append(self.store[key])
