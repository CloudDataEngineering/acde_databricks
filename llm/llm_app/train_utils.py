import json
import os
from datetime import datetime
from transformers import Trainer, TrainingArguments, DataCollatorForLanguageModeling
from datasets import Dataset
from .config import DATA_LOG_PATH, LORA_OUTPUT_PATH, TRAINING_ARGS

def log_interaction(original_text, improved_text):
    """Logs the original and the manually improved text for future fine-tuning."""
    os.makedirs(os.path.dirname(DATA_LOG_PATH), exist_ok=True)
    
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "instruction": "Rewrite the following message to be more professional and less verbose.",
        "input": original_text,
        "output": improved_text
    }
    
    with open(DATA_LOG_PATH, "a") as f:
        f.write(json.dumps(log_entry) + "\n")

def run_fine_tuning(model, tokenizer):
    """Runs a LoRA fine-tuning pass on the logged interactions."""
    if not os.path.exists(DATA_LOG_PATH):
        print("No training data found.")
        return False
    
    # Load data
    data = []
    with open(DATA_LOG_PATH, "r") as f:
        for line in f:
            data.append(json.loads(line))
            
    if len(data) < 1:
        print("Not enough training data.")
        return False

    def tokenize_function(examples):
        prompts = [
            f"<|system|>\n{instr}</s>\n<|user|>\n{inp}</s>\n<|assistant|>\n{out}</s>"
            for instr, inp, out in zip(examples["instruction"], examples["input"], examples["output"])
        ]
        return tokenizer(prompts, truncation=True, padding="max_length", max_length=512)

    dataset = Dataset.from_list(data)
    tokenized_dataset = dataset.map(tokenize_function, batched=True, remove_columns=dataset.column_names)

    training_args = TrainingArguments(**TRAINING_ARGS)
    
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=tokenized_dataset,
        data_collator=DataCollatorForLanguageModeling(tokenizer, mlm=False),
    )
    
    print("Starting fine-tuning...")
    trainer.train()
    
    # Save the adapter
    print(f"Saving LoRA weights to {LORA_OUTPUT_PATH}")
    model.save_pretrained(LORA_OUTPUT_PATH)
    
    return True
