from jina import Executor, DocumentArray, requests
from typing import Any, Dict, Optional, Sequence


class ChunkFlattener(Executor):
    def __init__(
        self,
        traversal_paths: str = "@r",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.traversal_paths = traversal_paths

    @requests
    def flatten_chunks(self, docs: DocumentArray, parameters: Dict[str, Any], **kwargs):
        traversal_paths = parameters.get("traversal_paths", self.traversal_paths)
        for doc in docs[traversal_paths]:
            doc.chunks = doc.chunks.flatten()
