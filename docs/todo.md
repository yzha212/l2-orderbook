# what is this document

This document is the parking lot for items that will be discussed and possible implement later

- ingest reconnect, conn_id + seq_num
- book builder: run_id + seq_num
  - dashboard: ignore anything not newer, show staleness
  - book archiver: write everything, dedup later
- what if book builder needs a new snapshot from ingest?
- sequence number validation, handle when frame is lost and when sequence number processed is out-of-order, maybe done by the first ingest service
- validation: crossed book check and zero / negative price check
- metrics: for example, system time when message is processed - coinbase message time

