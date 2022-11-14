import fnmatch


class GlobMatcher:
    SPECIAL_STARTERS = ['*', '?', '[']

    def __init__(self, folder_to_list: str):
        self.pattern = folder_to_list

    def get_prefix(self) -> str:
        """
        Ensures that we return a proper prefix for given glob-name.

        e.g.:
        - 'b/test-*.txt' –> 'b/'
        - '*.txt' –> ''
        - 'b/*/test.txt' –> 'b/'
        - 'b/*.txt' –> 'b/'
        """
        shortest_prefix = self.pattern
        # Ensure that the vanilla prefix ends with a slash to indicate directory.
        if not shortest_prefix.endswith('/'):
            shortest_prefix += '/'

        for starter in self.SPECIAL_STARTERS:
            if starter not in self.pattern:
                continue

            pre_starter, _ = self.pattern.split(starter, maxsplit=1)

            if '/' not in pre_starter:
                # Shortest prefix in this case is '', no need to search any longer.
                return ''

            last_slash_index = pre_starter.rindex('/')
            # +1 to include the final '/' character.
            new_prefix = pre_starter[:last_slash_index + 1]
            if len(new_prefix) < len(shortest_prefix):
                shortest_prefix = new_prefix

        return shortest_prefix

    def does_match(self, path: str) -> bool:
        """
        Ensures that given path matches our specific wildcard string.
        """
        # fnmatchcase caches regexes internally.
        # Using fnmatchcase instead of PurePath.match because PurePath.match does reverse search
        # which e.g. blocks `*` to a single level and not makes it work like `**`.
        return fnmatch.fnmatchcase(path, self.pattern)
