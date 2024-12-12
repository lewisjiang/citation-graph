from pybliometrics.scopus.exception import Scopus404Error
from pybliometrics.scopus import AbstractRetrieval
from pybliometrics.scopus import init as scopus_init
from pybliometrics.scopus.utils.constants import BASE_PATH
from collections import namedtuple
import time
import re
import os
import csv
import datetime
import random

import requests
import pyperclip
from bs4 import BeautifulSoup
import queue
import multiprocessing as mp


# Abstract Retrieval, 10,000 per week, 9 per sec.

class CitationGraph:
    class UnifiedObsMetadata:
        """
        To accommodate both scopus and doi response in paper note generation pipeline.
        """

        # TODO: accommodate with arxiv: url = "https://arxiv.org/bibtex/1234.5678"
        #  Note the author names have given name in the front.

        def __init__(self):
            self.title = None
            self.doi = ""
            self.citedby_count = ""
            self.sourcetitle_abbr = ""
            self.year = ""
            self.authors = []
            self.scopus_id = ""
            self.link = ""
            self.updated = ""

    def __init__(self, doi_lst, ignore_lst=None, max_age=30, min_refresh=7, num_proc=1):
        if ignore_lst is None:
            ignore_lst = []
        assert max_age >= 7 and max_age >= min_refresh
        self.max_age = max_age  # manually increase age when the internet is not available and you want to read old data
        self.min_refresh = min_refresh  # if a record \in [min_ref, max_age], it has a chance to be updated in a query
        self.num_proc = num_proc  # number of parallel processes
        self.curr_refs = dict()  # entry: ref.id: [Reference, (local_id_ref_pos)_set]

        self.input_doi = []  # identifiers, not necessarily DOI
        for doi in doi_lst:
            self.input_doi.append(doi.strip())  # lower creates a new profile, but the online query is case insensitive.
        self.input_scopus_id = set()

        self.ignored_refs = set()  # a set of scopus_id strings that we wish to block
        for i in ignore_lst:
            self.ignored_refs.add(i.strip())

        print("A total %d input dois." % len(doi_lst))

        self.v_full = []
        self.v_ref = []  # list of lists of references

        self.fail_set = set()  # failed id

        # change accordingly when the corresponding part in pybliometrics changes
        # as of v3.5.1
        fields = 'position id doi title authors authors_auid ' \
                 'authors_affiliationid sourcetitle publicationyear coverDate ' \
                 'volume issue first last citedbycount type text fulltext'
        self.OldRefTup = namedtuple('Reference', fields)

        self.cache_ref_dir = os.path.join(BASE_PATH, "my_parsed_bib_cache")

    @staticmethod
    def create_obsidian_note_from_full(uom, md_dir, topic):
        """

        :param md_dir:
        :param uom:
        :param topic:
        :return:
        """
        # current obsidian format: 2023-12-08
        """
        ---
        title: <% tp.file.title %>
        date: <% tp.file.creation_date("YYYY-MM-DD HH:mm:ss") %>
        updated: <% tp.file.creation_date("YYYY-MM-D HH:mm:ss") %>
        tags: [paper, meta_incomplete, preprint]
        aliases: []

        full_title:
        status: unread
        doi: 
        scopus_id: 
        citedby: 
        authors: []
        venue:
        year:
        ---
        """

        # TODO: check existence by scopus id
        # FIXME: it is still possible to generate duplicate files for the same paper, since doi lookup and scopus lookup
        #  are not synchronized. More dangerously, they will generate 2 files that only differ in capitalization.
        #  e.g. (Lazier than lazy greedy)
        for root, dirs, files in os.walk(md_dir):
            for fname in files:
                heads = []
                with open(os.path.join(md_dir, fname), "r", encoding="utf-8") as rec:
                    in_frontmatter = False
                    try:
                        while True:
                            line = next(rec)
                            if line.rstrip() == "---":
                                if not in_frontmatter:
                                    in_frontmatter = True
                                else:
                                    raise StopIteration
                            if in_frontmatter:
                                heads.append(line)

                    except StopIteration:
                        pass

                curr_full_title = None
                curr_scopus_id = None
                curr_doi = None

                same_id = False
                for line in heads:
                    words = line.strip().split(":")
                    if len(words) > 1:
                        if words[0].strip() == "scopus_id":
                            curr_scopus_id = words[1].strip().strip("\"'")
                        elif words[0].strip() == "doi":
                            curr_doi = words[1].strip().strip("\"'")
                        elif words[0].strip() == "full_title":
                            curr_full_title = " ".join(words[1:]).strip("\"'")

                    same_id = curr_scopus_id and uom.scopus_id.lower() == curr_scopus_id.lower() \
                              or curr_doi \
                              and uom.doi.lower() == curr_doi.lower()
                    if curr_full_title is not None and same_id:
                        break
                if same_id:
                    print("[-] Paper \"%s\" Obsidian record exists! Skipping: %s\"" % (
                        uom.scopus_id, curr_full_title))
                    return

            break

        tags = ["paper", "need_review"]
        if isinstance(topic, str) and topic.strip():
            tags.append(topic.strip().replace(" ", "_"))

        title_soup = BeautifulSoup(uom.title, "html.parser")
        title_wo_html = title_soup.get_text()

        title_short = "\"" + title_wo_html.split(":")[0] + "\""
        if len(title_short) - 2 > len(title_wo_html) // 4:
            title_short = ""

        key_val = [
            "title:", "\"" + (title_wo_html or "Unknown" + str(time.time())[-4:]).strip() + "\"",
            "date:", "\"" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\"",
            "updated:", "\"" + (uom.updated or datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + "\"",
            "tags:", str(tags),
            "aliases:", "[%s]" % title_short,
            "", "",
            "full_title:", "\"" + uom.title + "\"" or "",
            "status:", "unread",
            "doi:", "\"" + (uom.doi or "N/A") + "\"" or "",  # no doi on Scopus (e.g. Lazier than lazy greedy)
            "link:", uom.link or "",
            "scopus_id:", uom.scopus_id,
            "citedby:", str(uom.citedby_count),
            "authors:", str(uom.authors),
            "venue:", "\"" + str(uom.sourcetitle_abbr) + "\"",
            "year:", str(uom.year),
        ]
        lines = [" ".join(key_val[2 * i: 2 * i + 2]) for i in range(len(key_val) // 2)]

        md_path = os.path.join(md_dir, "".join(x for x in key_val[1] if (x.isalnum() or x in "._-()+ ")) + ".md")
        if os.path.isfile(md_path):
            print("[!] Overwriting markdown file of different ID and same file name! title: %s" % title_wo_html)
            assert False
        with open(md_path, "w", encoding="utf-8") as f:
            f.write("---\n")
            for line in lines:
                f.write(line + "\n")
            f.write("---\n\n")
            fields = ["followers::\nsource_code::",
                      "## Overview\nkeynovelty::\n\n",
                      "### Concise remarks",
                      "## Contribution/Problems solved", "## Past/Related works",
                      "## Main methods",
                      "## My focus", "## Doubts", "## Misc"]  # current obsidian format: 2023-12-08
            for fie in fields:
                f.write(fie + "\n\n")
            print("[+] Paper \"%s\" Obsidian record created: \"%s\"" % (uom.scopus_id if uom.scopus_id else uom.doi,
                                                                        title_wo_html))

    @staticmethod
    def read_val_by_key_from_frontmatter(fpath, key_name):
        """
        Explicitly parses the frontmatter surrounded by "---" in the beginning of a file
        TODO: parse multiline entries, add early termination by line number
        :param fpath:
        :param key_name:
        :return:
        """
        with open(fpath, "r", encoding="utf-8") as rec:
            in_frontmatter = False
            try:
                while True:
                    line = next(rec)
                    if line.rstrip() == "---":
                        if not in_frontmatter:
                            in_frontmatter = True
                        else:
                            raise StopIteration
                    if in_frontmatter:
                        line_seg = line.split(":")
                        if len(line_seg) > 1 and line_seg[0].strip() == key_name.strip():
                            val = "".join(line_seg[1:]).strip()
                            return val

            except StopIteration:
                pass
        return ""

    @staticmethod
    def create_obsidian_notes_from_dois(all_dois, md_dir, topic, skip_by_doi=True):
        """

        :param all_dois:
        :param md_dir:
        :param topic:
        :param skip_by_doi: whether to skip requesting by looking up doi in files
        :return:
        """

        def separate_authors(authors_str):
            """
            This is used to separate author entry into individual authors surrounded with ""
            """
            cleaned = authors_str.strip().split("\n")[0]
            au_list = cleaned.split(" and ")
            res = "\', \'".join([i.strip() for i in au_list])
            res = "\'" + res + "\'"
            # print(res)
            return res

        doi_site = "http://dx.doi.org/"
        headers = {
            "Accept": "application/x-bibtex"
        }

        assert md_dir

        existing_dois = dict()
        if skip_by_doi:
            for root, dirs, files in os.walk(md_dir):
                for fname in files:
                    curr_doi = CitationGraph.read_val_by_key_from_frontmatter(os.path.join(root, fname), "doi")
                    if curr_doi:
                        existing_dois[curr_doi.lower().strip().strip("\"'")] = fname

        for doi in all_dois:
            if skip_by_doi:
                q_res = existing_dois.get(doi.strip().lower())
                if q_res:
                    print("[-] record of doi %s exists! Skipping: %s" % (doi, str(q_res)))
                    continue

            url = doi_site + doi

            req = requests.get(url=url, headers=headers)  # essentially a bibtex file
            if req.status_code != 200:
                print("Error query doi.org: %d" % req.status_code, url)
                continue
            resp_txt = req.text.strip()

            uom = CitationGraph.UnifiedObsMetadata()

            # TODO: use more robust bibtex parser

            p1 = re.search("title={(.+?)}", resp_txt)
            if p1:
                uom.title = p1.groups()[0]

            p1 = re.search("DOI={(.+?)}", resp_txt)
            if p1:
                uom.doi = p1.groups()[0]

            p1 = re.search("booktitle={(.+?)}", resp_txt)
            if p1:
                uom.sourcetitle_abbr = CitationGraph.simplify_source_title(p1.groups()[0])

            p1 = re.search("journal={(.+?)}", resp_txt)
            if p1:
                uom.sourcetitle_abbr = CitationGraph.simplify_source_title(p1.groups()[0])

            p1 = re.search("author={(.+?)}", resp_txt)
            if p1:
                uom.authors = "[%s]" % separate_authors(p1.groups()[0])

            p1 = re.search("year={(.+?)}", resp_txt)
            if p1:
                uom.year = p1.groups()[0]

            CitationGraph.create_obsidian_note_from_full(uom, md_dir, topic)

    @staticmethod
    def create_obsidian_notes_from_arxiv(all_aids, md_dir, topic, skip_by_aid=True):
        """
        http://export.arxiv.org/oai2?verb=GetRecord&identifier=oai:arXiv.org:2405.03413&metadataPrefix=arXiv
        """
        pat1 = re.compile("ar[Xx]iv:(\d{4}\.\d{4,5})")
        pat2 = re.compile("arxiv.org/abs/(\d{4}\.\d{4,5})")

        existing_aids = dict()
        if skip_by_aid:
            for root, dirs, files in os.walk(md_dir):
                for fname in files:
                    curr_arxiv_link = CitationGraph.read_val_by_key_from_frontmatter(os.path.join(root, fname), "link")
                    match_aid = pat2.search(curr_arxiv_link)
                    if match_aid:
                        existing_aids[match_aid.groups()[0]] = fname

        for aid in all_aids:
            match_aid1 = pat1.search(aid)
            match_aid2 = pat2.search(aid)
            if not match_aid1 and not match_aid2:
                print("[-] Invalid arxiv id: %s" % aid)
                continue
            aid = match_aid1.groups()[0] if match_aid1 else match_aid2.groups()[0]
            print("[+] Processing arxiv %s" % aid)

            if skip_by_aid:
                q_res = existing_aids.get(aid.strip())
                if q_res:
                    print("[-] record of arxiv %s exists! Skipping: %s" % (aid, str(q_res)))
                    continue

            url = "http://export.arxiv.org/oai2?verb=GetRecord&identifier=oai:arXiv.org:%s&metadataPrefix=arXiv" % aid
            # usage guideline: https://info.arxiv.org/help/bulk_data.html#harvest
            req = requests.get(url)
            time.sleep(1)
            if req.status_code != 200:
                print("Error query arxiv: %d" % req.status_code, url)
                continue
            resp_txt = req.text.strip()
            soup = BeautifulSoup(resp_txt, "lxml-xml")

            uom = CitationGraph.UnifiedObsMetadata()

            arx_tag = soup.find("arXiv")
            if arx_tag:
                uom.title = re.sub("\s+", " ", arx_tag.find("title").text).strip()
                id_ret = arx_tag.find("id").text
                assert id_ret == aid
                uom.link = "http://arxiv.org/abs/" + aid  # keep arxiv id in links
                time_tag = arx_tag.find("updated")
                if not time_tag:
                    time_tag = arx_tag.find("created")
                uom.updated = time_tag.text[:10]
                uom.year = arx_tag.find("created").text[:4]

                uom.authors = []
                authors = arx_tag.find_all("author")
                for au in authors:
                    name1 = au.find("forenames").text.strip()
                    name2 = au.find("keyname").text.strip()
                    uom.authors.append("%s, %s" % (name2, name1))

                CitationGraph.create_obsidian_note_from_full(uom, md_dir, topic)

    @staticmethod
    def update_obsidian_note_meta_citedby(uom, md_dir):
        """
        The first file matching the scopus_id will have its citedby count updated or leaf unchanged.
        :param uom:
        :param md_dir:
        :return:
        """
        new_cites = str(uom.citedby_count)
        for root, dirs, files in os.walk(md_dir):
            for fname in files:
                # find the file to update
                this_file = False
                with open(os.path.join(md_dir, fname), "r", encoding="utf-8") as rec:
                    in_frontmatter = False
                    try:
                        while True:
                            line = next(rec)
                            if line.rstrip() == "---":
                                if not in_frontmatter:
                                    in_frontmatter = True
                                else:
                                    raise StopIteration
                            if in_frontmatter:
                                words = line.strip().split(":")
                                if len(words) == 2 and \
                                        (words[0].strip() == "scopus_id" and words[1].strip() == uom.scopus_id or
                                         words[0].strip() == "doi" and
                                         words[1].strip().lower().strip("\"'") == uom.doi.lower()):
                                    this_file = True
                                    break

                    except StopIteration:
                        pass

                if this_file:
                    if uom.scopus_id:
                        print("[+] Find the file of scopus_id:\"%s\" to update cite count." % uom.scopus_id)
                    elif uom.doi:
                        print("[+] Find the file of doi:\"%s\" to update cite count." % uom.doi)
                    with open(os.path.join(md_dir, fname), "r+", encoding="utf-8") as rec:
                        lines = rec.readlines()
                        is_beg = False
                        is_fin = False
                        pat_front_mat = re.compile("^---\s*")
                        for i, line in enumerate(lines):
                            if is_fin:
                                break
                            mat_fm = pat_front_mat.match(line)
                            if mat_fm and is_beg:
                                is_fin = True
                            if mat_fm and not is_beg:
                                is_beg = True
                            if not is_beg:
                                continue

                            words = line.strip().split(":")
                            if len(words) > 0 and words[0].strip() == "citedby":
                                if len(words) > 1 and words[1].strip() == new_cites:
                                    print("[+] cite count unchanged")
                                else:
                                    print("[+] %s -> %s" % (words[1].strip() if len(words) > 1 else "NA", new_cites))
                                    lines[i] = "citedby: " + new_cites + "\n"
                                    rec.seek(0)
                                    rec.write("".join(lines))
                                    rec.truncate()  # default to current pointer pos.
                                return  # still closes the file
                            # TODO: update the "updated" field in metadata
                        print("[-] No \"citedby\" key found in this file.")

    def update_md_citecount(self, md_dir):
        """
        It is a little stupid to update from the input dois instead of the whole folder. But this process pipeline is
        more robust. Use another function to find all the dois in the files in a folder
        :param md_dir:
        :return:
        """
        os.makedirs(md_dir, exist_ok=True)
        for i, full in enumerate(self.v_full):
            if full:
                uom = CitationGraph.UnifiedObsMetadata()
                uom.scopus_id = full.eid[7:] if full.eid else ""
                uom.citedby_count = full.citedby_count
                uom.doi = full.doi or ""
                CitationGraph.update_obsidian_note_meta_citedby(uom, md_dir)

    def print_curr_papers(self, md_dir="", topic=""):
        """
        Sort current papers by citations
        :return:
        """

        tab_head = ["#", "cites", "title", "cover date", "source title abbr",
                    "last author",
                    "first affil",
                    "doi"]

        col_widths = [6, 6, 60, 12, 44, 20, 44, 32]
        assert len(col_widths) == len(tab_head)

        fmt = "".join([" %%%d.%ds |" % (cw, cw) for cw in col_widths])

        qpapers = [(i, j,) for i, j in enumerate(self.v_full) if j]

        if md_dir:  # if create obsidian note templ at the same time.
            os.makedirs(md_dir, exist_ok=True)
            for qp in qpapers:
                uom = CitationGraph.UnifiedObsMetadata()
                uom.title = qp[1].title
                uom.doi = qp[1].doi or ""  # if null, keep as default
                uom.citedby_count = qp[1].citedby_count
                uom.sourcetitle_abbr = qp[1].sourcetitle_abbreviation
                uom.year = qp[1].coverDate[:4] if qp[1].coverDate else ""

                authors = []
                authors_id_set = set()
                for au in qp[1].authors:
                    if au.auid in authors_id_set:
                        continue
                    authors_id_set.add(au.auid)
                    authors.append("%s, %s" % (str(au.surname), str(au.given_name)))
                uom.authors = authors

                uom.scopus_id = qp[1].eid[7:] if qp[1].eid else ""

                CitationGraph.create_obsidian_note_from_full(uom, md_dir, topic)

        for case in range(2):
            if case == 0:

                print("\n" + "#" * 32, "Query papers (Input order)")
                print(fmt % tuple(tab_head))

            elif case == 1:

                print("\n" + "#" * 32, "Query papers (Most cited)")
                print(fmt % tuple(tab_head))

                qpapers.sort(key=lambda ent: int(ent[1].citedby_count) if ent[1].citedby_count else 0, reverse=True)
            else:
                continue

            for itm in qpapers:
                au1 = '-'
                af0 = '-'
                if itm[1].authors:
                    if len(itm[1].authors) > 0:
                        au1 = str(itm[1].authors[-1].indexed_name)
                if itm[1].affiliation:
                    if len(itm[1].affiliation) > 0:
                        af0 = str(itm[1].affiliation[0].name)
                print(fmt % (str(" %2d:" % itm[0]),
                             str(itm[1].citedby_count),
                             str(itm[1].title),
                             str(itm[1].coverDate),
                             str(itm[1].sourcetitle_abbreviation),
                             au1,
                             af0,
                             itm[1].doi or ""
                             ))

    @staticmethod
    def simplify_source_title(src):
        src += " "
        abbr_list = [
            ("Transactions", "Trans."),
            ("International", "Int."),
            ("Journal", "J."),
            (r"Robot[ics]*?\s", "Robot. "),
            ("Research", "Res."),
            ("Proceedings?", "Proc."),
            ("Conferences?", "Conf."),
            ("Intelligen(t|ce)", "Intell."),
            (r"Systems?", "Syst."),
            ("Science", "Sci."),
            ("Automation", "Autom."),
            ("Letters?", "Lett."),
            (r" \- ", " "),
            (r"Comput\w+?\s", "Comput. "),
            (r"Europ\w+?\s", "Eur. ")
        ]

        for div in abbr_list:
            src = re.sub(div[0], div[1], src)
        return src.strip()

    @staticmethod
    def parse_ref_two_authors(aunms, auids):
        au1 = au2 = '-'
        if aunms:
            aunms_raw = [x.strip() for x in aunms.split(";")]
            auids_raw = [str(j) for j in range(len(aunms_raw))]

            if auids:
                auids_raw = [x.strip() for x in auids.split(";")]
                assert len(aunms_raw) == len(auids_raw)

            auid_set = set()
            authors = []
            for aa in range(len(aunms_raw)):
                if auids_raw[aa] not in auid_set:
                    authors.append(aunms_raw[aa])
                    auid_set.add(auids_raw[aa])

            if len(authors) > 1:
                au1 = authors[0]
                au2 = authors[-1]
            elif len(authors) == 1:
                au1 = authors[0]
        return au1, au2

    def print_refs(self, show_ref_pos=False, min_refs=1):
        assert min_refs > 0
        num_ignored = 0

        tab_head = ["#", "ref L", "ref G", "title", "year",
                    "first author", "last author",
                    "source title", "scopus_id"]

        col_widths = [6, 6, 6, 60, 4, 20, 20, 44, 12]
        assert len(col_widths) == len(tab_head)

        fmt = "".join([" %%%d.%ds |" % (cw, cw) for cw in col_widths])

        print("\n" + "#" * 32, "Cited papers by the group of %d:" % len(self.v_ref))
        print(fmt % tuple(tab_head))

        dat = []
        for pair in self.curr_refs.items():
            if pair[1]:
                dat.append(pair[1])  # pair = (ref.id, [Reference, local_id_refpos_set])
                if pair[1][0].id and pair[1][0].id in self.ignored_refs:
                    num_ignored += 1

        dat.sort(key=lambda x: (-len(x[1]) if x[0].id in self.ignored_refs else len(x[1]),
                                int(x[0].citedbycount) if x[0].citedbycount and str(
                                    x[0].citedbycount).isdigit() else 0,),
                 reverse=True)

        ref_cnt = min_refs
        for i, itm in enumerate(dat):
            if 0 < len(itm[1]) < ref_cnt:
                continue

            if i >= len(dat) - num_ignored and ref_cnt > 0:
                print("-" * 32, "References pinned to bottom:")
                ref_cnt = -1

            local_sign = ""
            if str(itm[0].id) in self.input_scopus_id:  # if the referred paper is an input query paper
                local_sign = "*"

            au1, au2 = self.parse_ref_two_authors(itm[0].authors, itm[0].authors_auid)

            print(fmt % (str(i + 1),
                         local_sign + str(len(itm[1])),
                         str(itm[0].citedbycount) if str(itm[0].citedbycount).isdigit() else '-',
                         str(itm[0].title),
                         str(itm[0].coverDate[:4] if itm[0].coverDate else "-"),
                         au1,
                         au2,
                         self.simplify_source_title(str(itm[0].sourcetitle)),
                         str(itm[0].id),
                         ), end='\t')
            for j in sorted(list(itm[1])):
                if show_ref_pos:
                    print(" %2d:[%d]" % (j[0], int(j[1])), end=",")  # case 2: show ref position
                else:
                    print(" %2d:" % (j[0],), end=",")  # case 1: do not show reference position in each paper
            print()

    # Only supports doi as qid
    def load_bibliography_from_file(self, q_id):
        cache_name = q_id.replace('/', '_') + ".csv"
        cache_path = os.path.join(self.cache_ref_dir, cache_name)

        ret = []
        with open(cache_path, 'r', encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile, delimiter=';', quotechar='\"')
            title_ok = False
            for ref in reader:
                if len(ref) != len(self.OldRefTup._fields):
                    print(" !  `Reference` entry length inconsistent")  # TODO: should we raise error?
                    return []
                if not title_ok:
                    for i, itm in enumerate(ref):
                        if itm != self.OldRefTup._fields[i]:
                            print(" !  `Reference` structure inconsistent")
                            return []
                    title_ok = True
                    continue

                tmp = self.OldRefTup(*ref)
                ret.append(tmp)
            return ret

    # save especially reference list data to a well parsed form so that we can use across platform without
    #  internet connection
    # Only supports doi as qid
    def save_bibliography_to_file(self, ref_lst, q_id):
        """
        Check if a historical record exists and not spires
        :param ref_lst:
        :param q_id:
        :return:
        """
        if not ref_lst:
            return

        cache_name = q_id.replace('/', '_') + ".csv"
        cache_path = os.path.join(self.cache_ref_dir, cache_name)
        os.makedirs(self.cache_ref_dir, exist_ok=True)

        with open(cache_path, 'w', newline='', encoding="utf-8") as csvfile:
            csvw = csv.writer(csvfile, delimiter=';', quotechar='\"', quoting=csv.QUOTE_MINIMAL)
            csvw.writerow(list(ref_lst[0]._fields))
            for ref in ref_lst:
                csvw.writerow(list(ref))

            print("[+] Saved parsed refs for: ", q_id)

    def print_one_bib_entry(self, fmt, ref):
        au1, au2 = self.parse_ref_two_authors(ref.authors, ref.authors_auid)
        print(fmt % (
            "[%s]" % str(ref.position),
            str(ref.citedbycount or '-'),
            str(ref.title),
            str(ref.coverDate[:4] if ref.coverDate else "-"),
            au1,
            au2,
            self.simplify_source_title(str(ref.sourcetitle)),
            str(ref.id)
        ))

    def print_paper_bibliography(self, ii):
        """
        Print the bib of one paper from the input doi list
        :param ii:
        :return:
        """
        assert len(self.input_doi) == len(self.v_ref)
        assert len(self.input_doi) == len(self.v_full)
        assert 0 <= ii < len(self.v_ref)
        assert self.v_ref[ii]  # not none

        print("\n" + "#" * 32, "Items cited by \"%s\":" % str(self.v_full[ii].title))
        tab_head = ["[#]", "total cites", "title", "year",
                    "first author", "last author",
                    "source title", "scopus_id"]
        col_widths = [6, 12, 60, 4, 20, 20, 44, 12]
        assert len(col_widths) == len(tab_head)

        fmt = "".join([" %%%d.%ds |" % (cw, cw) for cw in col_widths])
        print(fmt % tuple(tab_head))

        for ref in self.v_ref[ii]:
            self.print_one_bib_entry(fmt, ref)

    def live_bib_lookup(self, ii):
        def cbk(clipboard_content):
            res = re.search(r"\d+", clipboard_content)
            if res:
                num = int(res.group())
                if 0 < num < 1000:
                    print("Found number %d in: `%s`" % (num, str(clipboard_content)))
                    return num
            return -1

        assert len(self.input_doi) == len(self.v_ref)
        assert len(self.input_doi) == len(self.v_full)
        assert 0 <= ii < len(self.v_ref)
        assert self.v_ref[ii]  # not none

        max_ref = len(self.v_ref[ii])

        col_widths = [6, 12, 60, 4, 20, 20, 44, 12]
        fmt = "".join([" %%%d.%ds |" % (cw, cw) for cw in col_widths])

        recent_value = ""
        while True:
            tmp_value = pyperclip.paste()
            if tmp_value != recent_value:
                print(recent_value, tmp_value)
                recent_value = tmp_value
                pos = cbk(recent_value)
                if 0 < pos <= max_ref:
                    self.print_one_bib_entry(fmt, self.v_ref[ii][pos - 1])
            time.sleep(0.5)

    @staticmethod
    def get_bib_entry_worker(q_id, q_fail, sig, refresh_days=99, proc_id=0):
        """
        Query using items from the queue of ids, and save the result to the queue of results
        :param q_id: (item_id)
        :param q_fail: (item_id), or None if success.
        :param sig:
        :param refresh_days:
        :param proc_id:
        :return:
        """

        cnt_total = 0
        cnt_quota = 0
        while True:
            res = [None] * 3  # temp result holder, will be appended to q_res as tuple (scopus_id, v_full, v_ref)
            try:
                itm_rem = q_id.qsize()
                iid = q_id.get(block=True, timeout=1)
                print("[+] Remaining: %d" % itm_rem)
            except queue.Empty:
                if q_id.empty():  # failure to get() does not mean the queue is empty, weird
                    print("[!] Queue is empty!")
                    break
                else:
                    continue

            try:
                print("[+] Querying FULL %s" % iid)
                ab = AbstractRetrieval(iid, view='FULL', refresh=refresh_days)
                quota_rem = ab.get_key_remaining_quota()
                if quota_rem:
                    cnt_quota += 1
                    print("[+] Remaining quota: %s " % quota_rem)
                res[0] = ab.eid[7:]
                res[1] = ab
            except Scopus404Error as e1:
                print(" !  FULL view of DOI: ", iid, "cannot be found!")
            except Exception as e:
                print(" !  Unhandled exception:", e)

            try:
                if res[1] is None:
                    raise ValueError("FULL view already failed.")
                print("[+] Query REF %s" % iid)
                ab = AbstractRetrieval(iid, view='REF', refresh=refresh_days)
                quota_rem = ab.get_key_remaining_quota()
                if quota_rem:
                    cnt_quota += 1
                    print("[+] Remaining quota: %s " % quota_rem)
                if not ab.references:
                    raise ValueError(" !  Empty references!")
                assert len(ab.references) == ab.refcount
                res[2] = ab.references
            except Scopus404Error as e1:
                print(" !  REF view of DOI: ", iid, "cannot be found!")
            except ValueError as e2:
                print(" !  REF view of DOI: ", iid, e2)
            finally:
                if res[1] is None or res[2] is None:
                    q_fail.put(iid)
                else:
                    q_fail.put(None)  # placeholder for success
                cnt_total += 1

        print("[+] Process #%d finished. %d items processed, %d quota used." % (proc_id, cnt_total, cnt_quota))
        with sig.get_lock():
            sig.value += 1

    def get_bibliography_info_parallel(self):
        """
        A brilliant way to bypass the limitation of the inability of pickling pybliometrics objects during
        multiprocessing, using the caching system of pybliometrics.
        :return:
        """
        q_id = mp.Queue()
        q_fail = mp.Queue()
        fin_sig = mp.Value('i', 0)
        procs = []

        for iid in self.input_doi:
            q_id.put(iid)

        for i in range(self.num_proc):
            rolled_refresh_days = random.randint(self.min_refresh, self.max_age)
            p = mp.Process(target=self.get_bib_entry_worker, args=(q_id, q_fail, fin_sig, rolled_refresh_days, i,))
            procs.append(p)

        for p in procs:
            p.start()

        # wait till finished, then we start to harvest the results
        while fin_sig.value < self.num_proc:
            time.sleep(0.1)
        assert q_fail.qsize() == len(self.input_doi)  # this is certain and reliable since all the processes are done

        # empty the queue otherwise the main process will be blocked
        lst_res = []
        while len(lst_res) < len(self.input_doi):
            # time.sleep(0.01)  # q_res.qsize() and q_res.empty() are not reliable. Is q_res.get() reliable?
            # print(q_fail.qsize(), q_fail.empty())
            lst_res.append(q_fail.get())

        # print(q_fail.qsize(), q_fail.empty())

        print(len(lst_res))
        print("[+] All processes finished...")
        for p in procs:
            p.join()
        print("[+] All processes joined.")

        print("#" * 32 + " Funnel into the main process")

        # find the failed ones and add to the fail set
        for res in lst_res:
            if res:
                self.fail_set.add(res)

        # use the single process method to get the references by reading the cache
        CitationGraph.get_bibliography_info(self, refresh_offset=999)

    def get_bibliography_info(self, refresh_offset=0):
        for i, doi in enumerate(self.input_doi):
            rolled_refresh_days = random.randint(self.min_refresh, self.max_age) + refresh_offset

            # query FULL data
            try:
                print("[+] Query FULL %d/%d" % (i + 1, len(self.input_doi)))

                if doi in self.fail_set:
                    raise ValueError("In fail set, skipped.")

                ab = AbstractRetrieval(doi, view='FULL', refresh=rolled_refresh_days)
                quota_rem = ab.get_key_remaining_quota()

                if quota_rem:  # really queried Scopus instead of reading cache
                    print("[+] Remaining quota: %s " % quota_rem)

                self.v_full.append(ab)
                self.input_scopus_id.add(ab.eid[7:])

            except Scopus404Error as e1:
                print(" !  FULL view of DOI: ", doi, "cannot be found!")
                self.fail_set.add(doi)
                self.v_full.append(None)
            except ValueError as e2:
                print(" !  FULL view of DOI: ", doi, e2)
                self.fail_set.add(doi)
                self.v_full.append(None)
            except Exception as e:
                print(" !  Unhandled exception:", e)
                self.fail_set.add(doi)
                self.v_full.append(None)

            # query REF data
            try:
                all_curr_refs = []
                # Outdated as of pybliometrics v4.1
                # start_ref = 1  # start at 1, but give a 0 is ok (still fetches first 40 references)

                # test if corresponding FULL is successful
                if doi in self.fail_set:
                    raise ValueError("FULL view already failed.")

                print("[+] Query REF %d/%d" % (i + 1, len(self.input_doi)))
                ab = AbstractRetrieval(doi, view='REF', refresh=rolled_refresh_days)

                quota_rem = ab.get_key_remaining_quota()
                if quota_rem:  # really queried Scopus instead of reading cache
                    print("[+] Remaining quota: %s " % quota_rem)

                if not ab.references:
                    raise ValueError(" !  Empty references!")

                all_curr_refs += ab.references
                assert len(ab.references) == ab.refcount

                for ref in all_curr_refs:  # build a dict of the works referred.
                    dict_ent = self.curr_refs.get(ref.id)
                    if dict_ent:
                        # parent id is not available in REF view:
                        dict_ent[1].add((i, ref.position,))  # TODO: should we enforce length?
                    else:
                        self.curr_refs[ref.id] = [ref, {(i, ref.position,), }]

                self.v_ref.append(all_curr_refs)

            except Scopus404Error as e1:
                print(" !  REF view of DOI: ", doi, "cannot be found!", e1)
                self.fail_set.add(doi)
                self.v_ref.append(None)
            except ValueError as e2:
                print(" !  REF view of DOI: ", doi, e2)
                self.fail_set.add(doi)
                self.v_ref.append(None)
            except Exception as e:
                print(" !  Unhandled exception:", e)
                self.fail_set.add(doi)
                self.v_ref.append(None)
                raise e

        print("#" * 32 + " Failed: %d out of %d" % (self.v_ref.count(None), len(self.v_ref)))
        for i, ref in enumerate(self.v_ref):
            if not ref:
                print("%4d | %32s |" % (i, self.input_doi[i]))
        print()


def update_cite_count_in_md(md_dir):
    """
    TODO: construct queries from either doi or scopus id
    :param md_dir:
    :return:
    """

    md_dois = []
    for root, dirs, files in os.walk(md_dir):
        for fname in files:
            fdoi = CitationGraph.read_val_by_key_from_frontmatter(os.path.join(root, fname), "doi")
            if fdoi.strip().strip("\"'"):
                md_dois.append(fdoi.strip().strip("\"'"))

    cg1 = CitationGraph(md_dois, num_proc=4)
    # cg1.get_bibliography_info()
    cg1.get_bibliography_info_parallel()

    cg1.update_md_citecount(md_dir)


if __name__ == "__main__":
    scopus_init()

    #  obsidian notes' temp folder.
    obsidian_tmp_dir = os.path.join(os.path.split(os.path.realpath(__file__))[0], "obs_tmp")

    # dois = [
    #     "10.1002/rob.21762",
    #     "10.1109/TPAMI.2017.2658577",
    # ]

    ignored = ["84871676827",
               "58249138093",
               "84856742278",  # ISAM2
               "33750968800",
               "84866704163",  # KITTI
               "0019574599",  # ransac
               ]

    # # 1. sparsification
    # group_topic = "sparsification"
    # dois = [
    #     "10.1109/IROS.2016.7759502",
    #     "10.1109/ICRA.2019.8793836",
    #     "10.1109/LRA.2018.2798283",
    #     "10.1177/0278364917691110",
    #     "10.1109/LRA.2019.2961227",
    #     "10.1109/TRO.2014.2347571",
    #     "10.1177/0278364915581629",
    #     "10.1016/j.robot.2019.06.004",
    #     "10.1109/IROS.2018.8594007",
    #     "10.1109/ICRA.2013.6630556",
    #     "10.1109/ECMR.2013.6698835",
    #     "10.1109/TRO.2016.2624754"
    # ]

    # 2. loop closure
    # group_topic = "long_term_slam"
    group_topic = ""
    dois = [
        "10.1109/CVPR52688.2022.00545",
        "10.1109/TRO.2021.3139964",
        "10.1109/LRA.2021.3140054",
        "10.1109/IROS51168.2021.9636676",
        "10.1109/TRO.2022.3174476",
        "10.1177/0278364915581629",
        "10.1109/LRA.2019.2961227"

    ]

    ################################
    # use case a. Normal query
    cg = CitationGraph(dois, ignored, num_proc=4)
    # cg.get_bibliography_info()
    cg.get_bibliography_info_parallel()

    cg.print_curr_papers(md_dir=obsidian_tmp_dir, topic=group_topic)
    # cg.print_curr_papers(topic=group_topic)

    cg.print_refs(show_ref_pos=True, min_refs=1)

    # # show the bib of one paper
    # cg.print_paper_bibliography(31)

    ################################
    # use case b. update cite count
    # copy obsidian notes to 'obs_tuc' if you want to update "citedby"
    to_update_cite_dir = os.path.join(os.path.split(os.path.realpath(__file__))[0], "obs_tuc")
    update_cite_count_in_md(to_update_cite_dir)

    # ################################
    # use case c. Generate paper note template from DOI (no cross referencing)
    dois = [
        "10.1609/aaai.v29i1.9486",
        "10.1109/TRO.2016.2624754",  # tro16 slam
    ]

    CitationGraph.create_obsidian_notes_from_dois(dois, md_dir=obsidian_tmp_dir, topic="")

    ################################
    # use case d. Generate paper note template from arXiv (no cross referencing)
    arxiv_ids = [
        "arXiv:2312.01616"
    ]

    CitationGraph.create_obsidian_notes_from_arxiv(arxiv_ids, obsidian_tmp_dir, topic="")
