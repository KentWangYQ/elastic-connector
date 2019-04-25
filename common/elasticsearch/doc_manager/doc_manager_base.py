class DocManagerBase:
    @staticmethod
    def apply_update(doc, update_spec):
        """Apply an update operation to a document."""

        if "$set" not in update_spec and "$unset" not in update_spec:
            # update spec contains the new document in its entirety
            return update_spec

        # Helper to cast a key for a list or dict, or raise ValueError
        def _convert_or_raise(container, key):
            if isinstance(container, dict):
                return key
            elif isinstance(container, list):
                return int(key)
            else:
                raise ValueError

        # Helper to retrieve (and/or create)
        # a dot-separated path within a document.
        def _retrieve_path(container, path, create=False):
            looking_at = container
            for part in path:
                if isinstance(looking_at, dict):
                    if create and part not in looking_at:
                        looking_at[part] = {}
                    looking_at = looking_at[part]
                elif isinstance(looking_at, list):
                    index = int(part)
                    # Do we need to create additional space in the array?
                    if create and len(looking_at) <= index:
                        # Fill buckets with None up to the index we need.
                        looking_at.extend([None] * (index - len(looking_at)))
                        # Bucket we need gets the empty dictionary.
                        looking_at.append({})
                    looking_at = looking_at[index]
                else:
                    raise ValueError
            return looking_at

        def _set_field(doc, to_set, value):
            if "." in to_set:
                path = to_set.split(".")
                where = _retrieve_path(doc, path[:-1], create=True)
                index = _convert_or_raise(where, path[-1])
                wl = len(where)
                if isinstance(where, list) and index >= wl:
                    where.extend([None] * (index + 1 - wl))
                where[index] = value
            else:
                doc[to_set] = value

        def _unset_field(doc, to_unset):
            try:
                if "." in to_unset:
                    path = to_unset.split(".")
                    where = _retrieve_path(doc, path[:-1])
                    index_or_key = _convert_or_raise(where, path[-1])
                    if isinstance(where, list):
                        # Unset an array element sets it to null.
                        where[index_or_key] = None
                    else:
                        # Unset field removes it entirely.
                        del where[index_or_key]
                else:
                    del doc[to_unset]
            except (KeyError, IndexError, ValueError):
                raise

                # wholesale document replacement

        try:
            # $set
            for to_set in update_spec.get("$set", []):
                value = update_spec["$set"][to_set]
                _set_field(doc, to_set, value)

            # $unset
            for to_unset in update_spec.get("$unset", []):
                _unset_field(doc, to_unset)

        except (KeyError, ValueError, AttributeError, IndexError):
            raise
        return doc

    def index(self, doc, namespace, timestamp):
        """Index document"""
        raise NotImplementedError()

    def update(self, doc, namespace, timestamp):
        """Update document"""
        raise NotImplementedError()

    def delete(self, doc_id, namespace, timestamp):
        """Delete document by doc_id"""
        raise NotImplementedError()

    def handle_command(self, command_doc, namespace, timestamp):
        """Handle a command."""
        raise NotImplementedError()

    def bulk_upsert(self):
        """"""
        raise NotImplementedError()

    def commit(self):
        """Send bulk buffer to Elasticsearch, then refresh."""
        raise NotImplementedError()

    def stop(self):
        """Stop auto committer"""
        raise NotImplementedError()

    def search(self):
        """"""
        raise NotImplementedError()
