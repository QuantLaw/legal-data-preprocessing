import json

from quantlaw.utils.beautiful_soup import create_soup

from statics import US_REG_AUTHORITY_EDGELIST_PATH
from statutes_pipeline_steps.us_crossreference_edgelist import UsCrossreferenceEdgelist


class UsAuthorityEdgelist(UsCrossreferenceEdgelist):
    @property
    def dest(self):
        assert self.regulations
        return US_REG_AUTHORITY_EDGELIST_PATH

    def make_edge_list(self, yearfile_path, key_df):
        soup = create_soup(self.src + "/" + yearfile_path)
        edge_list = []

        # for debug
        # problem_matches = set()
        # problem_keys = set()

        for item in soup.find_all(auth_text_parsed=True):
            node_out = item.get("key")
            refs = json.loads(item.attrs["auth_text_parsed"])
            for ref in refs:
                try:  # for debug
                    key = "_".join(ref[:2])
                    matches = key_df.at[key, "key"]

                    # # for debug
                    # if type(matches) != str:
                    #     problem_matches.add(tuple(matches))

                    node_in = matches if type(matches) == str else matches[0]
                    edge_list.append([node_out, node_in])
                    assert len(ref) > 1

                except KeyError:
                    # # for debug
                    # problem_keys.add(key)
                    pass

        # # for debug
        # if len(problem_matches) > 0:
        #     print(f"{yearfile_path} Problem Matches:\n",
        #           sorted(list(problem_matches)))
        # if len(problem_keys) > 0:
        #     print(f"{yearfile_path} Problem Matches:\n", sorted(list(problem_keys)))
        return edge_list
