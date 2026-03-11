import streamlit as st
from llm_app.model_utils import get_model_and_tokenizer, generate_rewrite, generate_suggestions
from llm_app.train_utils import log_interaction, run_fine_tuning
import os

st.set_page_config(page_title="Professional Writing Assistant", layout="wide")

st.title("Professional Writing Assistant (LoRA Trained)")
st.markdown("""
This app helps you rewrite messages to be more professional and provides writing suggestions.
Interactions are logged and used for auto-training the underlying model.
""")

@st.cache_resource
def load_llm():
    return get_model_and_tokenizer()

model, tokenizer = load_llm()

# Input section
st.header("1. Your Draft Message")
user_input = st.text_area("Enter your draft here:", height=150)

if st.button("Analyze and Rewrite"):
    if user_input.strip():
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Professional Rewrite")
            rewrite = generate_rewrite(model, tokenizer, user_input)
            st.session_state.rewrite = rewrite
            st.text_area("Rewrite result:", value=rewrite, height=200, key="rewrite_area")
            
        with col2:
            st.subheader("Writing Suggestions")
            suggestions = generate_suggestions(model, tokenizer, user_input)
            st.markdown(suggestions)
            
        st.info("You can edit the rewritten version above and click 'Log as Improved' to help train the model.")
    else:
        st.warning("Please enter some text first.")

# Feedback / Training Data Collection
if "rewrite" in st.session_state:
    st.header("2. Provide Feedback")
    improved_version = st.text_area("Improved Version (edit this to your liking):", 
                                    value=st.session_state.rewrite, 
                                    height=200)
    
    if st.button("Log as Improved"):
        log_interaction(user_input, improved_version)
        st.success("Interaction logged successfully! This data will be used for the next training pass.")

# Training Section
st.sidebar.header("Admin / Training")
if st.sidebar.button("Trigger Auto-Training"):
    with st.sidebar.status("Training in progress..."):
        success = run_fine_tuning(model, tokenizer)
        if success:
            st.sidebar.success("Training complete! Please restart the app to use the updated model.")
        else:
            st.sidebar.error("Training failed or not enough data.")

st.sidebar.markdown("---")
st.sidebar.write(f"Model: {model.config._name_or_path}")
st.sidebar.write(f"Device: {model.device}")