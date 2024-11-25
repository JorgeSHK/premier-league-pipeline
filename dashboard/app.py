import streamlit as st
import pandas as pd
import plotly.express as px
from src.loaders.data_loader import PremierLeagueLoader
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()


class PremierLeagueDashboard:
    def __init__(self):
        self.loader = PremierLeagueLoader()

    def run(self):
        st.set_page_config(
            page_title="Premier League Stats",
            page_icon="⚽",
            layout="wide"
        )

        # Header
        st.title("⚽ Premier League Dashboard")
        st.markdown("---")

        # Crear dos columnas
        col1, col2 = st.columns([2, 1])

        with col1:
            st.subheader("📊 League Table")
            # Obtener tabla de posiciones con DISTINCT ON para eliminar duplicados
            query = """
                SELECT DISTINCT ON (t.name)
                    t.name as team,
                    ts.position,
                    ts.played,
                    ts.won,
                    ts.drawn,
                    ts.lost,
                    ts.goals_for,
                    ts.goals_against,
                    ts.goal_difference,
                    ts.points
                FROM teams t
                JOIN team_stats ts ON t.team_id = ts.team_id
                WHERE ts.season = '2023-2024'
                    AND ts.updated_at = (
                        SELECT MAX(updated_at)
                        FROM team_stats
                        WHERE season = '2023-2024'
                    )
                ORDER BY t.name, ts.updated_at DESC, ts.position;
            """
            league_table = pd.read_sql(query, self.loader.conn)

            # Reordenar por posición
            league_table = league_table.sort_values('position')
            st.dataframe(league_table, use_container_width=True)

        with col2:
            st.subheader("⭐ Top Scorers")

            # Añadir slider para seleccionar número de goleadores
            n_scorers = st.slider("Number of top scorers to display", 5, 50, 10)

            # Obtener goleadores con la misma lógica pero sin límite fijo
            query = f"""
                SELECT DISTINCT ON (p.name)
                    p.name as player,
                    t.name as team,
                    ps.goals,
                    ps.penalties,
                    (ps.goals - ps.penalties) as goals_from_play
                FROM players p
                JOIN teams t ON p.team_id = t.team_id
                JOIN player_stats ps ON p.player_id = ps.player_id
                WHERE ps.season = '2023-2024'
                    AND ps.updated_at = (
                        SELECT MAX(updated_at)
                        FROM player_stats
                        WHERE season = '2023-2024'
                    )
                    AND ps.goals > 0  -- Solo mostrar jugadores que han anotado
                ORDER BY p.name, ps.goals DESC
                LIMIT {n_scorers};
            """
            scorers = pd.read_sql(query, self.loader.conn)

            # Añadir pestañas para diferentes visualizaciones
            tab1, tab2 = st.tabs(["📊 Chart", "📋 Table"])

            with tab1:
                # Crear gráfico de barras
                fig = px.bar(
                    scorers,
                    x='player',
                    y=['goals_from_play', 'penalties'],
                    title=f'Top {n_scorers} Goal Scorers',
                    labels={
                        'player': 'Player',
                        'value': 'Goals',
                        'variable': 'Type'
                    },
                    hover_data=['team'],
                    color_discrete_map={
                        'goals_from_play': '#1f77b4',
                        'penalties': '#ff7f0e'
                    }
                )
                fig.update_layout(
                    barmode='stack',
                    xaxis_tickangle=-45,
                    legend_title="Goal Type",
                    height=500
                )
                st.plotly_chart(fig, use_container_width=True)

            with tab2:
                # Mostrar tabla de goleadores
                st.dataframe(
                    scorers.style.format({
                        'goals': '{:.0f}',
                        'penalties': '{:.0f}',
                        'goals_from_play': '{:.0f}'
                    }),
                    use_container_width=True
                )

            # Añadir estadísticas de goles
            st.markdown("---")
            st.markdown("### 📊 Scoring Statistics")

            total_goals = scorers['goals'].sum()
            total_penalties = scorers['penalties'].sum()
            total_from_play = scorers['goals_from_play'].sum()

            goal_stats = st.columns(3)
            with goal_stats[0]:
                st.metric("Total Goals", f"{total_goals:,.0f}")
            with goal_stats[1]:
                st.metric("Goals from Play", f"{total_from_play:,.0f}")
            with goal_stats[2]:
                st.metric("Penalties", f"{total_penalties:,.0f}")

        # Métricas generales
        st.markdown("---")
        st.subheader("📈 League Stats")

        # Obtener estadísticas generales
        query = """
            WITH latest_stats AS (
                SELECT *
                FROM team_stats
                WHERE season = '2023-2024'
                    AND updated_at = (
                        SELECT MAX(updated_at)
                        FROM team_stats
                        WHERE season = '2023-2024'
                    )
            )
            SELECT 
                SUM(goals_for) as total_goals,
                ROUND(AVG(goals_for::float)::numeric, 2) as avg_goals_per_team,
                MAX(points) as max_points,
                MIN(points) as min_points
            FROM latest_stats;
        """
        stats = pd.read_sql(query, self.loader.conn)

        # Mostrar métricas
        metrics = st.columns(4)
        with metrics[0]:
            st.metric("Total Goals", int(stats['total_goals'].iloc[0]))
        with metrics[1]:
            st.metric("Avg Goals per Team", f"{stats['avg_goals_per_team'].iloc[0]:.1f}")
        with metrics[2]:
            st.metric("Highest Points", int(stats['max_points'].iloc[0]))
        with metrics[3]:
            st.metric("Lowest Points", int(stats['min_points'].iloc[0]))


if __name__ == "__main__":
    dashboard = PremierLeagueDashboard()
    dashboard.run()