import streamlit as st
import numpy as np
import matplotlib.pyplot as plt
import time

def app():
    st.title('Page 1')
    st.write('Bienvenue sur la page 1!')

    # Création d'un placeholder pour le graphique
    graph_placeholder = st.empty()

    fig, ax = plt.subplots()
    x, y = [], []
    line, = ax.plot(x, y, 'r-')  # Initialisation de la ligne rouge

    for t in range(100):
        x.append(t)
        y.append(np.sin(t / 10))  # Exemple de données : sinus

        line.set_xdata(x)
        line.set_ydata(y)

        ax.relim()
        ax.autoscale_view()

        # Mise à jour du graphique dans le placeholder
        graph_placeholder.pyplot(fig)
        time.sleep(0.1)  # Pause pour simuler les données arrivant en temps réel

if __name__ == "__main__":
    app()

