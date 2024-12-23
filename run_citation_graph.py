from citation_graph import CitationGraph, update_cite_count_in_md
import os
import yaml

if __name__ == '__main__':
    script_dir = os.path.split(os.path.realpath(__file__))[0]
    pth_default_ids = os.path.join(script_dir, "config", "default_config.yaml")
    #  obsidian notes' temp folder.
    obsidian_tmp_dir = os.path.join(script_dir, "obs_tmp")

    with open(pth_default_ids, 'r') as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)
        print(cfg)

    ################################
    # use case a. Normal query
    cg = CitationGraph(cfg["default_pub_identifiers"], cfg["default_pub_identifiers_ignored"], num_proc=4)
    # cg.get_bibliography_info()
    cg.get_bibliography_info_parallel()

    cg.print_curr_papers(md_dir=obsidian_tmp_dir, topic=cfg["group_topic"]["default_pub_identifiers"])
    # cg.print_curr_papers(topic=cfg["group_topic"]["default_pub_identifiers"])

    cg.print_refs(show_ref_pos=True, min_refs=1)

    # # show the bib of one paper
    # cg.print_paper_bibliography(31)

    ################################
    # use case b. update cite count
    # copy obsidian notes to 'obs_tuc' if you want to update "citedby", and add "scopus_id" in the frontmatter
    to_update_cite_dir = os.path.join(os.path.split(os.path.realpath(__file__))[0], "obs_tuc")
    update_md_metadata(to_update_cite_dir)

    # ################################
    # use case c. Generate paper note template from DOI (no cross referencing)
    CitationGraph.create_obsidian_notes_from_dois(cfg["digital_object_ids"], md_dir=obsidian_tmp_dir, topic="")

    ################################
    # use case d. Generate paper note template from arXiv (no cross referencing)
    CitationGraph.create_obsidian_notes_from_arxiv(cfg["arxiv_ids"], obsidian_tmp_dir, topic="")
