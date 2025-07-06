CREATE INDEX IF NOT EXISTS idx_product_embeddings_cosine
    ON product_embeddings USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);