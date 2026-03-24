SELECT pipeline_name,
       dataset_name,
       source_layer,
       target_layer,
       watermark_column,
       last_processed_at,
       updated_at,
       metadata_json
FROM audit.processing_state
ORDER BY updated_at DESC, pipeline_name ASC, dataset_name ASC;
