import pandas as pd
import numpy as np
import altair as alt
import streamlit as st

df = pd.DataFrame(np.random.randn(200, 3), columns=['a', 'b', 'c'])
c = alt.Chart(df).mark_circle().encode(x='a', y='b', size='c',
                                       color='c')
st.altair_chart(c) # , width=-1

st.sidebar.title("About")

<<<<<<< Updated upstream
st.sidebar.title("Train Neural Network")
if st.sidebar.button('Train CNN'):
    print("Train NN")
=======
st.sidebar.info(
    "Hi This still is a demo application written to help you understand Streamlit. The application identifies the animal in the picture. It was built using a Convolution Neural Network (CNN).")

>>>>>>> Stashed changes

st.sidebar.title("Sidebar title")
if st.sidebar.button('Im a button'):
    st.write("You pressed a button!.")

st.sidebar.title("Predict New Images")

st.title('Animal Identification')
st.write("Pick an image from the left. You'll be able to view the image.")
st.write("When you're ready, submit a prediction on the left.")

st.markdown("** bold? **" )
st.altair_chart(c)
st.title('Animal Identification')
st.write("Pick an image from the left. You'll be able to view the image.")
st.write("When you're ready, submit a prediction on the left.")
st.markdown("** bold? **" )
st.altair_chart(c)
st.title('Animal Identification')
st.write("Pick an image from the left. You'll be able to view the image.")
st.write("When you're ready, submit a prediction on the left.")
st.markdown("** bold? **" )
st.altair_chart(c)
st.title('Animal Identification')
st.write("Pick an image from the left. You'll be able to view the image.")
st.write("When you're ready, submit a prediction on the left.")
st.markdown("** bold? **" )
st.altair_chart(c)
st.title('Animal Identification')
st.write("Pick an image from the left. You'll be able to view the image.")
st.write("When you're ready, submit a prediction on the left.")
st.markdown("** bold? **" )
st.altair_chart(c)
st.title('Animal Identification')
st.write("Pick an image from the left. You'll be able to view the image.")
st.write("When you're ready, submit a prediction on the left.")
st.markdown("** bold? **" )
st.altair_chart(c)
st.title('Animal Identification')
st.write("Pick an image from the left. You'll be able to view the image.")
st.write("When you're ready, submit a prediction on the left.")
st.markdown("** bold? **" )
st.altair_chart(c)

st.info(
    "This still is a demo application written to help you understand Streamlit. The application identifies the animal in the picture. It was built using a Convolution Neural Network (CNN).")

