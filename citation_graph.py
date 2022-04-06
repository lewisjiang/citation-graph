from pybliometrics.scopus.exception import Scopus404Error
from pybliometrics.scopus import AbstractRetrieval
from collections import namedtuple
import time


# Abstract Retrieval, 10,000 per week, 9 per sec.

class CitationGraph:
    def __init__(self, doi_lst, ignore_lst=[], max_age=30, min_gap=0.1):
        assert max_age > 7
        self.max_age = max_age
        self.curr_refs = dict()  # entry: ref.id: [Reference, local_id_set]

        self.doi_list = []
        for doi in doi_lst:
            self.doi_list.append(doi.strip())  # lower creates a new profile, but the online query is case insensitive.

        self.ignored_refs = set()  # a set of scopus_id strings that we wish to block
        for i in ignore_lst:
            self.ignored_refs.add(i.strip())

        print("A total %d input dois." % len(doi_lst))

        self.v_full = []
        self.v_ref = []  # list of list of references

        self.fail_set = set()  # failed doi
        self.t_last = time.time()
        self.q_gap = min_gap

    def print_curr_papers(self):
        """
        Sort current papers by citations
        :return:
        """
        tab_head = ["#", "cites", " " * 50 + "title", "cover date", " " * 15 + "source title abbr",
                    " " * 5 + "last author",
                    " " * 25 + "first affil",
                    " " * 25 + "doi"]
        col_gap = []
        fmt = ""
        for hd in tab_head:
            col_gap.append(4 * (len(hd) // 4 + 1))
            fmt += "%%%ds | " % col_gap[-1]

        dat = [(i, j,) for i, j in enumerate(self.v_full) if j]

        for case in range(2):
            if case == 0:

                print("\n" + "#" * 32, "Query papers (Input order)")
                print(fmt % tuple(tab_head))

            elif case == 1:

                print("\n" + "#" * 32, "Query papers (Most cited)")
                print(fmt % tuple(tab_head))

                dat.sort(key=lambda ent: int(ent[1].citedby_count) if ent[1].citedby_count else 0, reverse=True)
            else:
                continue

            for itm in dat:
                print(fmt % (str(" %2dr" % itm[0])[:col_gap[0]],
                             str(itm[1].citedby_count)[:col_gap[1]],
                             str(itm[1].title)[:col_gap[2]],
                             str(itm[1].coverDate)[:col_gap[3]],
                             str(itm[1].sourcetitle_abbreviation)[:col_gap[4]],
                             "" if len(itm[1].authors) < 1 else str(itm[1].authors[-1].indexed_name)[:col_gap[5]],
                             "" if len(itm[1].affiliation) < 1 else str(itm[1].affiliation[0].name)[:col_gap[6]],
                             itm[1].doi[:col_gap[7]] if itm[1].doi else ""
                             ))

    def print_refs(self):
        num_ignored = 0

        tab_head = ["#", "local cites", "total cites", " " * 50 + "title", "pub year",
                    " " * 30 + "source title", " " * 5 + "scopus_id"]
        col_gap = []
        fmt = ""
        for hd in tab_head:
            col_gap.append(4 * (len(hd) // 4 + 1))
            fmt += "%%%ds | " % col_gap[-1]

        print("\n" + "#" * 32, "Cited papers")
        print(fmt % tuple(tab_head))

        dat = []
        for itm in self.curr_refs.items():
            if itm[1]:
                dat.append(itm[1])
                if itm[1][0].id and itm[1][0].id in self.ignored_refs:
                    num_ignored += 1

        dat.sort(key=lambda x: (-len(x[1]) if x[0].id in self.ignored_refs else len(x[1]),
                                int(x[0].citedbycount) if x[0].citedbycount else 0,),
                 reverse=True)

        for i, itm in enumerate(dat):
            if i == len(dat) - num_ignored:
                print("-" * 32, "Ignored references:")
            print(fmt % (str(i + 1)[:col_gap[0]],
                         str(len(itm[1]))[:col_gap[1]],
                         str(itm[0].citedbycount or '-')[:col_gap[2]],
                         str(itm[0].title)[:col_gap[3]],
                         str(itm[0].publicationyear[:4] if itm[0].publicationyear else "-")[:col_gap[4]],
                         str(itm[0].sourcetitle)[:col_gap[5]],
                         str(itm[0].id)[:col_gap[6]],
                         ), end='\t')
            for j in sorted(list(itm[1])):
                print(" %2dr" % j, end=",")
            print()

    def get_bibliography_info(self):
        # query FULL data
        for i, itm in enumerate(self.doi_list):
            try:
                t_ready = time.time()
                if t_ready - self.t_last < self.q_gap:
                    time.sleep(self.q_gap)

                print("[+] Query FULL %d/%d" % (i + 1, len(self.doi_list)))
                ab = AbstractRetrieval(itm, view='FULL', refresh=self.max_age)
                quota_rem = ab.get_key_remaining_quota()

                if quota_rem:  # really queried Scopus instead of reading cache
                    self.t_last = time.time()
                    print("[+] Remaining quota: %s " % quota_rem)

                self.v_full.append(ab)

            except Scopus404Error as e1:
                print("[!] FULL view of DOI: ", itm, "cannot be found!")
                self.fail_set.add(itm)
                self.v_full.append(None)
            except Exception as e:
                print("[!] Unhandled exception:", e)
                self.fail_set.add(itm)
                self.v_full.append(None)

        # query REF data
        for i, itm in enumerate(self.doi_list):
            try:
                all_curr_refs = []
                start_ref = 1  # start at 1, but give a 0 is ok (still fetches first 40 references)
                need_refresh = False

                while True:
                    t_ready = time.time()
                    if t_ready - self.t_last < self.q_gap:
                        time.sleep(self.q_gap)

                    print("[+] Query REF %d/%d" % (i + 1, len(self.doi_list)))
                    ab = AbstractRetrieval(itm, view='REF', refresh=True if need_refresh else self.max_age,
                                           startref=start_ref)

                    quota_rem = ab.get_key_remaining_quota()
                    if quota_rem:  # really queried Scopus instead of reading cache
                        self.t_last = time.time()
                        print("[+] Remaining quota: %s " % quota_rem)

                    if len(ab.references) == ab.refcount:
                        all_curr_refs += ab.references
                        break

                    if need_refresh:
                        all_curr_refs += ab.references
                        start_ref += 40
                        if len(all_curr_refs) == ab.refcount:
                            break
                    else:
                        need_refresh = True

                for ref in all_curr_refs:  # build a dict of the works referred.
                    dict_ent = self.curr_refs.get(ref.id)
                    if dict_ent:
                        # parent id is not available in REF view:
                        dict_ent[1].add(i)  # TODO: should we enforce length?
                    else:
                        self.curr_refs[ref.id] = [ref, {i, }]

                self.v_ref.append(all_curr_refs)

            except Scopus404Error as e1:
                print("[!] REF view of DOI: ", itm, "cannot be found!")
                self.fail_set.add(itm)
                self.v_ref.append(None)
            except Exception as e:
                print("[!] Unhandled exception:", e)
                self.fail_set.add(itm)
                self.v_ref.append(None)

        # plot the reference info (local citation count, total citation count, queried named-tuples)
        pass


if __name__ == "__main__":
    # dois = [
    #     "10.1002/rob.21762",
    #     "10.1109/TPAMI.2017.2658577",
    # ]

    # 1. sparsification
    dois = [
        "10.1109/IROS.2016.7759502",
        "10.1109/ICRA.2019.8793836",
        "10.1109/LRA.2018.2798283",
        "10.1177/0278364917691110",
        "10.1109/LRA.2019.2961227",
        "10.1109/TRO.2014.2347571",
        "10.1177/0278364915581629",
        "10.1016/j.robot.2019.06.004",
        "10.1109/IROS.2018.8594007",
        "10.1109/ICRA.2013.6630556",
        "10.1109/ECMR.2013.6698835",
    ]
    ignored = ["84871676827",
               "58249138093",
               "84856742278",
               ]

    cg = CitationGraph(dois, ignored)

    cg.get_bibliography_info()
    cg.print_curr_papers()
    cg.print_refs()
