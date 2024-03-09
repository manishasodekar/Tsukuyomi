import streamlit as st
import requests
from utils import heconstants
from utils.s3_operation import S3SERVICE


def main():
    # Define the API endpoint URL
    api_url = heconstants.SYNC_SERVER + "/history"

    s3 = S3SERVICE()

    # Function to fetch filenames from the API
    def fetch_filenames():
        dirs = s3.get_dirs_matching_pattern(pattern="c*")
        if dirs:
            return dirs
        else:
            return []

    # Streamlit UI
    st.title("Transcripts")

    # Fetch filenames from the API
    filenames = fetch_filenames()

    # Create a sidebar with a search bar
    search_term = st.sidebar.text_input("Search Filename")
    filtered_filenames = [filename for filename in filenames if search_term.lower() in filename.lower()]

    # Display the filtered filenames in a scrollable list in the sidebar as buttons
    st.sidebar.markdown("### Filenames")
    for filename in filtered_filenames:
        if st.sidebar.button(filename):
            st.write(f"Selected Filename: {filename}")

            # Fetch JSON data for the selected filename
            response = requests.get(api_url + f"?conversation_id={filename}")
            if response.status_code == 200:
                json_data = response.json()
                st.write("JSON Response:")
                st.json(json_data)
            else:
                st.warning("Failed to fetch JSON response for the selected filename.")


if __name__ == "__main__":
    main()
