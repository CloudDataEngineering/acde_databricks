# Professional Writing Assistant with LoRA Fine-Tuning

This application is a small LLM (Large Language Model) assistant that helps users rewrite content professionally and provides writing improvement suggestions. It is designed to be stored in Google Drive and can be auto-trained based on user interactions.

## Features
1. **Professional Rewriting:** Rewrites messages to be more professional and less verbose.
2. **Writing Suggestions:** Provides actionable suggestions to improve writing skills.
3. **Continuous Learning:** Logged interactions can be used to fine-tune the model using LoRA (Low-Rank Adaptation) for a personalized experience.
4. **Simple UI:** A Streamlit-based web interface for easy interaction.

## Installation and Setup

### 1. Requirements
Ensure you have Python 3.9+ installed and a GPU (recommended for training).
Install the necessary dependencies:
```bash
pip install torch transformers peft accelerate bitsandbytes streamlit datasets
```

### 2. Google Drive Integration (Optional but recommended)
To store the model and training data in your Google Drive:
1. Mount your Google Drive (e.g., in a Colab notebook):
   ```python
   from google.colab import drive
   drive.mount('/content/drive')
   ```
2. Set the environment variable `LLM_STORAGE_PATH` to your Google Drive folder:
   ```bash
   export LLM_STORAGE_PATH="/content/drive/MyDrive/llm_space"
   ```

### 3. Running the Application
To launch the Streamlit interface, run:
```bash
streamlit run llm_app/main.py
```

## How to Use
1. **Drafting:** Enter your draft message in the text area.
2. **Analysis:** Click "Analyze and Rewrite" to see the professional version and writing suggestions.
3. **Feedback:** Edit the rewritten version to your liking and click "Log as Improved." This will save the interaction for training.
4. **Auto-Training:** Go to the sidebar and click "Trigger Auto-Training" to fine-tune the model on your logged interactions. The LoRA weights will be saved to your specified storage path.

## Project Structure
- `llm_app/main.py`: Streamlit User Interface.
- `llm_app/config.py`: Configuration and storage paths.
- `llm_app/model_utils.py`: Logic for loading the model and generating text.
- `llm_app/train_utils.py`: Logic for data logging and LoRA fine-tuning.
