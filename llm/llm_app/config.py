import os

# Base directory for storing model and data, intended for Google Drive
# In a local environment, this can be any directory.
# In Colab, it would typically be "/content/drive/MyDrive/llm_space"
STORAGE_BASE_PATH = os.getenv("LLM_STORAGE_PATH", "./llm_storage")

# Redirect HuggingFace cache to the storage path if desired, 
# to ensure everything (including base model) stays on Google Drive.
HF_CACHE_PATH = os.path.join(STORAGE_BASE_PATH, ".cache", "huggingface")
if not os.path.exists(HF_CACHE_PATH):
    os.makedirs(HF_CACHE_PATH, exist_ok=True)
os.environ["HF_HOME"] = HF_CACHE_PATH

MODEL_ID = "TinyLlama/TinyLlama-1.1B-Chat-v1.0"
LORA_OUTPUT_PATH = os.path.join(STORAGE_BASE_PATH, "lora_weights")
DATA_LOG_PATH = os.path.join(STORAGE_BASE_PATH, "interactions.jsonl")

# Training Config
TRAINING_ARGS = {
    "output_dir": os.path.join(STORAGE_BASE_PATH, "training_output"),
    "per_device_train_batch_size": 1,
    "gradient_accumulation_steps": 4,
    "learning_rate": 2e-4,
    "num_train_epochs": 1,
    "logging_steps": 10,
    "save_strategy": "no", # We save the LORA adapter manually
}
