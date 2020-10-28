from collections import defaultdict


class StringContainsAlign:
    def __init__(
        self,
        text_list_0=None,
        text_list_1=None,
        sep=" ",
        fixed_ends=False,
        min_text_length=None,
    ):
        self.sep = sep
        self.fixed_ends = fixed_ends
        self.min_text_length = min_text_length
        self.text_list_0 = text_list_0
        self.text_list_1 = text_list_1
        self.index_0 = None
        self.index_1 = None

    def create_index(self):
        assert self.text_list_0
        assert self.text_list_1
        self.index_0 = self._text_list_to_token_index(self.text_list_0)
        self.index_1 = self._text_list_to_token_index(self.text_list_1)

    def _text_list_to_token_index(self, text_list):
        token_index = defaultdict(set)
        for i, text in enumerate(text_list):
            text_tokens = text.split(self.sep)
            for token in text_tokens:
                token_index[token].add(i)
        return token_index

    def run(self, reversed=False):
        text_list_needle = self.text_list_1 if reversed else self.text_list_0
        text_list_haystack = self.text_list_0 if reversed else self.text_list_1

        index_haystack = self.index_0 if reversed else self.index_1

        result = []

        for needle_index, needle in enumerate(text_list_needle):
            if self.min_text_length and len(needle) < self.min_text_length:
                continue

            needle_tokens = needle.split(self.sep)

            if not self.fixed_ends:
                # Remove first and last token so that e.g. adding just one letter to the
                # last token is possible
                needle_tokens = needle_tokens[1:-1]

            if needle_tokens:
                candidates = index_haystack[needle_tokens[0]]
                for token in needle_tokens[1:]:
                    candidates = candidates.intersection(index_haystack[token])

                    # For performence
                    if len(candidates) <= 1:
                        break
            else:
                candidates = range(len(text_list_haystack))
                print(needle)

            for haystack_index in candidates:
                target_text = text_list_haystack[haystack_index]
                if needle in target_text:
                    result.append((needle_index, haystack_index))

        return result
