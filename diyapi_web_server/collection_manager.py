# -*- coding: utf-8 -*-
"""
collection_manager.py

class CollectionManager wrap collection functions
"""
import logging

class CollectionManager(object):
    """
    wrap collection functions
    """
    def __init__(self, central_connection, cluster_id):
        self._log = logging.getLogger("CollectionManager")
        self._central_connection = central_connection
        self._cluster_id = cluster_id

    def get_collection_id_dict(self, avatar_id):
        """
        return a list of (collection, collection_id) for all the collections
        the avatar owns on the local cluster   
        """
        result = self._central_connection.fetch_all_rows("""
            select name, id from diy_central.collection
            where  cluster_id = %s
            and avatar_id = %s
        """, [self._cluster_id, avatar_id, ]
        )

        # if this avatar doesn't have a row, we'll give him a default
        # recurse loop back
        if result is None or len(result) == 0:
            self._central_connection.execute("""
                begin;
                insert into diy_central.collection
                (cluster_id, avatar_id)
                values (%s, %s);
                commit;
            """, [self._cluster_id, avatar_id, ]
            )
            return self.get_collection_id_dict(avatar_id)

        return dict(result)

    def add_collection(self, avatar_id, collection_name):
        """
        add a collection to the local cluster
        """
        self._central_connection.execute("""
            begin;
            insert into diy_central.collection
            (cluster_id, name, avatar_id)
            select %s, %s, %s where not exists (
                select 1 from diy_central.collection where name = %s
            );
            commit;
        """, [self._cluster_id, collection_name, avatar_id, collection_name, ]
        )

    def list_collections(self, avatar_id):
        """
        list all collections for the avatar, for all clusters
        """
        result = self._central_connection.fetch_all_rows("""
            select diy_central.collection.name as collection_name, 
                   diy_central.cluster.name as cluster_name
            from diy_central.collection inner join diy_central.cluster
            on diy_central.collection.cluster_id = diy_central.cluster.id
            where diy_central.collection.avatar_id = %s
        """, [avatar_id, ]
        )
        if result is None or len(result) == 0:
            return []

        return result

    def delete_collection(self, avatar_id, collection_name):
        """
        delete the collection from the database
        """
        self._central_connection.execute("""
            delete from diy_central.collection
            where cluster_id = %s and name = %s and avatar_id =%s
        """, [self._cluster_id, collection_name, avatar_id, ]
        )

