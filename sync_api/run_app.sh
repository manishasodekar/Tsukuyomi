#!/bin/bash

# Set environment and corresponding port
if [ "$1" == "stage" ]; then
    ENVIRONMENT="stage"
    PORT=8502
elif [ "$1" == "dev" ]; then
    ENVIRONMENT="dev"
    PORT=8501
else
    echo "Invalid environment. Please specify 'stage' or 'dev'."
    exit 1
fi

# Export the ENVIRONMENT variable
export ENVIRONMENT

# Run the Streamlit app on the specified port
streamlit run transcription_ui.py --server.port $PORT
