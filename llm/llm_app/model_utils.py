import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
from peft import LoraConfig, get_peft_model, PeftModel
import os
from .config import MODEL_ID, LORA_OUTPUT_PATH

def get_model_and_tokenizer():
    tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
    # Set padding token
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
        
    device = "cuda" if torch.cuda.is_available() else "cpu"
    
    # Load base model
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_ID,
        device_map="auto",
        torch_dtype=torch.float16 if device == "cuda" else torch.float32
    )
    
    if device == "cpu":
        # Ensure we can train on CPU if needed (though slow)
        model.to(torch.float32)
        model.enable_input_require_grads()

    # Check if LoRA weights exist
    if os.path.exists(os.path.join(LORA_OUTPUT_PATH, "adapter_config.json")):
        print(f"Loading LoRA weights from {LORA_OUTPUT_PATH}")
        model = PeftModel.from_pretrained(model, LORA_OUTPUT_PATH)
    else:
        print("No LoRA weights found. Using base model with new LoRA config.")
        config = LoraConfig(
            r=8,
            lora_alpha=32,
            target_modules=["q_proj", "v_proj"],
            lora_dropout=0.05,
            bias="none",
            task_type="CAUSAL_LM"
        )
        model = get_peft_model(model, config)
    
    return model, tokenizer

def generate_rewrite(model, tokenizer, text):
    prompt = f"<|system|>\nYou are a professional editor. Rewrite the following message to be more professional and less verbose.</s>\n<|user|>\n{text}</s>\n<|assistant|>\n"
    inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
    with torch.no_grad():
        outputs = model.generate(**inputs, max_new_tokens=100, do_sample=True, temperature=0.7)
    
    response = tokenizer.decode(outputs[0], skip_special_tokens=True)
    # Extract only the assistant's part
    if "<|assistant|>" in response:
        response = response.split("<|assistant|>")[-1].strip()
    else:
        response = response.replace(prompt, "").strip()
    return response

def generate_suggestions(model, tokenizer, text):
    prompt = f"<|system|>\nYou are a writing coach. Analyze the user's writing and provide 2-3 specific suggestions to improve their professional writing skills.</s>\n<|user|>\n{text}</s>\n<|assistant|>\n"
    inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
    with torch.no_grad():
        outputs = model.generate(**inputs, max_new_tokens=150, do_sample=True, temperature=0.7)
    
    response = tokenizer.decode(outputs[0], skip_special_tokens=True)
    if "<|assistant|>" in response:
        response = response.split("<|assistant|>")[-1].strip()
    return response
