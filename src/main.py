import streamlit as st
import pages.page1
import pages.page2

PAGES = {
    "Page 1": pages.page1,
    "Page 2": pages.page2
}

def main():
    st.sidebar.title("Navigation")
    selection = st.sidebar.radio("Aller à", list(PAGES.keys()))

    page = PAGES[selection]
    page.app()


if __name__ == "__main__":
    main()
