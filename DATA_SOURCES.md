# Data sources

## CMS Provider Data Catalog: Hospitals

- Publisher: Centers for Medicare & Medicaid Services (CMS)
- Topic: Hospitals
- Dataset used: Hospital General Information
- Current dataset page: https://data.cms.gov/provider-data/dataset/xubh-q36u
- Hospital archive page: https://data.cms.gov/provider-data/archived-data/hospitals
- Public access level: public
- Raw data committed to this repository: **No**

The current Hospital General Information dataset contains Medicare-registered hospitals and fields such as facility ID, address, hospital type, ownership, emergency-services indicator, and overall hospital rating.

This project is most useful with two chronological CMS hospital **snapshot** archives. For example, CMS lists 2026 snapshots dated **2026-02-25** and **2026-05-13**. The normalization script extracts `HOSPITAL_GENERAL_INFORMATION.CSV` from each downloaded archive and converts selected columns into JSONL for comparison and MERGE processing.

Because CMS updates and archive URLs can change, the repository links to the official archive page rather than hard-coding transient download URLs.

This is an independent portfolio project and is not affiliated with or endorsed by CMS.
