from __future__ import annotations

from chromadb.utils import embedding_functions

from deerflow.config import get_app_config


def main() -> None:
    model_name = get_app_config().structured_memory.embedding_model_name
    print(f"[embedding] preparing model: {model_name}")
    embedding_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name=model_name
    )
    # Trigger lazy model download during startup.
    embedding_fn(["warmup"])
    print(f"[embedding] model ready: {model_name}")


if __name__ == "__main__":
    main()
