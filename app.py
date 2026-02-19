"""
📊 Data Dashboard Pro - Analizzatore di File Excel/CSV
Applicazione professionale per analisi dati con grafici interattivi e filtri dinamici.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime
import re
import io
import os

# ============================================================
# CONFIGURAZIONE PAGINA
# ============================================================
st.set_page_config(
    page_title="📊 Data Dashboard Pro",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# CSS PERSONALIZZATO
# ============================================================
def load_css():
    css_path = os.path.join(os.path.dirname(__file__), "style.css")
    if os.path.exists(css_path):
        with open(css_path, "r", encoding="utf-8") as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

load_css()

# ============================================================
# FUNZIONI UTILITÀ
# ============================================================

def _clean_unnamed_columns(df):
    """Rimuove colonne 'Unnamed' generate da pandas durante il caricamento.
    Rimuove SOLO colonne Unnamed che sono completamente vuote (tutti NaN)
    o che sono chiaramente un vecchio indice numerico sequenziale.
    Le colonne Unnamed con dati reali vengono mantenute.
    """
    unnamed_cols = [c for c in df.columns if str(c).startswith('Unnamed')]
    if unnamed_cols:
        cols_to_drop = []
        for col in unnamed_cols:
            non_null_count = df[col].notna().sum()
            if non_null_count == 0:
                # Colonna completamente vuota
                cols_to_drop.append(col)
            elif df[col].dtype in [np.int64, np.float64] and non_null_count == len(df):
                # Probabilmente è un vecchio indice: numerico sequenziale completo
                try:
                    if (df[col].values == np.arange(len(df))).all():
                        cols_to_drop.append(col)
                except Exception:
                    pass
            # Se ha dati reali (anche parziali), la teniamo
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
    return df


def _auto_detect_header(file_or_path, sheet_name=0):
    """Rileva automaticamente la riga header in un file Excel.
    Se la prima riga contiene molte colonne 'Unnamed', cerca una riga
    più in basso che abbia più nomi significativi.
    Ritorna il numero di riga header (0-based) da usare con pd.read_excel(header=N).
    """
    try:
        df_raw = pd.read_excel(file_or_path, sheet_name=sheet_name, header=None, nrows=20)
    except Exception:
        return 0

    best_header = 0
    best_named_count = 0

    for i in range(min(10, len(df_raw))):
        row = df_raw.iloc[i]
        # Conta quanti valori sono stringhe non-vuote e non sembrano "Unnamed"
        named = 0
        for val in row:
            if isinstance(val, str) and val.strip() and not val.strip().startswith('Unnamed'):
                named += 1
        if named > best_named_count:
            best_named_count = named
            best_header = i

    # Usa header auto-detectato solo se trovava significativamente più nomi
    # (almeno 3 colonne con nome nella riga candidata)
    if best_header > 0 and best_named_count >= 3:
        return best_header
    return 0


def _read_excel_smart(file_or_path, sheet_name=0):
    """Legge un foglio Excel rilevando automaticamente la riga header."""
    header_row = _auto_detect_header(file_or_path, sheet_name)
    if hasattr(file_or_path, 'seek'):
        file_or_path.seek(0)
    df = pd.read_excel(file_or_path, sheet_name=sheet_name, header=header_row)
    return _clean_unnamed_columns(df)


def load_data(uploaded_file):
    """Carica dati da file Excel o CSV con rilevamento automatico."""
    try:
        file_name = uploaded_file.name.lower()
        if file_name.endswith('.csv'):
            # Prova diversi separatori
            for sep in [',', ';', '\t', '|']:
                try:
                    df = pd.read_csv(uploaded_file, sep=sep, encoding='utf-8')
                    if len(df.columns) > 1:
                        return _clean_unnamed_columns(df), None
                    uploaded_file.seek(0)
                except Exception:
                    uploaded_file.seek(0)
            # Fallback
            df = pd.read_csv(uploaded_file, encoding='utf-8')
            return _clean_unnamed_columns(df), None
        elif file_name.endswith(('.xlsx', '.xls')):
            xls = pd.ExcelFile(uploaded_file)
            if len(xls.sheet_names) > 1:
                sheets = {name: _read_excel_smart(xls, sheet_name=name)
                          for name in xls.sheet_names}
                return sheets, xls.sheet_names
            else:
                df = _read_excel_smart(xls, sheet_name=0)
                return df, None
        else:
            return None, "Formato file non supportato. Usa .csv, .xlsx o .xls"
    except Exception as e:
        return None, str(e)


def fix_duplicate_columns(df):
    """Rinomina colonne duplicate aggiungendo un suffisso numerico."""
    df = df.copy()
    cols = df.columns.tolist()
    seen = {}
    new_cols = []
    for col in cols:
        if col in seen:
            seen[col] += 1
            new_cols.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            new_cols.append(col)
    df.columns = new_cols
    return df


def detect_column_types(df):
    """Rileva automaticamente i tipi di colonna.
    Converte colonne object che contengono prevalentemente numeri in numeriche.
    Converte colonne object che contengono prevalentemente date in datetime.
    """
    # Prima: prova a convertire colonne object in numerico
    for col in df.select_dtypes(include=['object', 'category']).columns:
        converted = pd.to_numeric(df[col], errors='coerce')
        non_null_original = df[col].notna().sum()
        if non_null_original > 0:
            pct_numeric = converted.notna().sum() / non_null_original
            if pct_numeric > 0.5:
                df[col] = converted

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
    date_cols = df.select_dtypes(include=['datetime64']).columns.tolist()

    # Prova a convertire colonne stringa rimaste in date
    for col in categorical_cols[:]:
        try:
            converted = pd.to_datetime(df[col], format='mixed', dayfirst=True, utc=True)
            converted = converted.dt.tz_localize(None)  # Rimuovi timezone per uniformità
            if converted.notna().sum() > len(df) * 0.5:
                df[col] = converted
                date_cols.append(col)
                categorical_cols.remove(col)
        except Exception:
            pass

    return numeric_cols, categorical_cols, date_cols


def format_number(num):
    """Formatta numeri in modo leggibile."""
    if pd.isna(num):
        return "N/A"
    if np.isinf(num):
        return "∞" if num > 0 else "-∞"
    if abs(num) >= 1_000_000_000:
        return f"{num/1_000_000_000:.1f}B"
    elif abs(num) >= 1_000_000:
        return f"{num/1_000_000:.1f}M"
    elif abs(num) >= 1_000:
        return f"{num/1_000:.1f}K"
    elif isinstance(num, float):
        return f"{num:.2f}"
    return str(num)


def create_kpi_card(label, value, delta=None, color="#4361ee"):
    """Crea una card KPI con stile professionale."""
    delta_html = ""
    if delta is not None:
        arrow = "▲" if delta > 0 else "▼" if delta < 0 else "●"
        delta_color = "#2ecc71" if delta > 0 else "#e74c3c" if delta < 0 else "#95a5a6"
        delta_html = f'<div style="color:{delta_color}; font-size:14px; margin-top:4px;">{arrow} {abs(delta):.1f}%</div>'

    return f"""
    <div class="kpi-card" style="border-left: 4px solid {color};">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {delta_html}
    </div>
    """


# ============================================================
# SIDEBAR - UPLOAD E FILTRI
# ============================================================
def render_sidebar():
    """Renderizza la sidebar con upload e filtri."""
    with st.sidebar:
        st.markdown("## 📊 Data Dashboard Pro")
        st.markdown("---")

        # Upload file 1
        st.markdown("### 📁 Carica Dati")
        uploaded_file_1 = st.file_uploader(
            "📄 File 1 - Principale",
            type=['csv', 'xlsx', 'xls'],
            help="Supporta file CSV, Excel (.xlsx, .xls)",
            key="file_upload_1"
        )

        # Upload file 2 (opzionale)
        uploaded_file_2 = st.file_uploader(
            "📄 File 2 - Opzionale (per merge)",
            type=['csv', 'xlsx', 'xls'],
            help="Carica un secondo file per unirlo al primo",
            key="file_upload_2"
        )

        # File di esempio
        st.markdown("---")
        if st.button("📥 Genera dati di esempio", use_container_width=True):
            sample_df = generate_sample_data()
            st.session_state['sample_data'] = sample_df
            st.success("Dati di esempio caricati!")

        return uploaded_file_1, uploaded_file_2


def generate_sample_data():
    """Genera un dataset di esempio per la demo."""
    np.random.seed(42)
    n = 500
    dates = pd.date_range(start='2024-01-01', periods=n, freq='D')
    categories = np.random.choice(['Elettronica', 'Abbigliamento', 'Casa', 'Sport', 'Libri'], n)
    regions = np.random.choice(['Nord', 'Centro', 'Sud', 'Isole'], n)
    cities = np.random.choice(['Milano', 'Roma', 'Napoli', 'Torino', 'Palermo', 'Firenze', 'Bologna'], n)

    df = pd.DataFrame({
        'Data': dates,
        'Categoria': categories,
        'Regione': regions,
        'Città': cities,
        'Vendite': np.random.randint(100, 10000, n),
        'Quantità': np.random.randint(1, 100, n),
        'Profitto': np.round(np.random.uniform(10, 5000, n), 2),
        'Sconto': np.round(np.random.uniform(0, 0.5, n), 2),
        'Valutazione': np.round(np.random.uniform(1, 5, n), 1)
    })
    return df


def apply_filters(df, numeric_cols, categorical_cols, date_cols):
    """Applica filtri dinamici basati sul tipo di colonna."""
    filtered_df = df.copy()

    with st.sidebar:
        st.markdown("---")
        st.markdown("### 🔍 Filtri")

        # Filtri per colonne categoriche
        for col in categorical_cols:
            unique_vals = df[col].dropna().unique().tolist()
            if 1 < len(unique_vals) <= 50:
                selected = st.multiselect(
                    f"📌 {col}",
                    options=unique_vals,
                    default=[],
                    key=f"filter_{col}"
                )
                if selected:
                    filtered_df = filtered_df[filtered_df[col].isin(selected)]

        # Filtri per colonne date
        for col in date_cols:
            # Assicurati che la colonna sia effettivamente datetime
            try:
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    filtered_df[col] = pd.to_datetime(filtered_df[col], errors='coerce')
                min_date = df[col].dropna().min()
                max_date = df[col].dropna().max()
            except Exception:
                continue

            if pd.notna(min_date) and pd.notna(max_date):
                st.markdown(f"**📅 {col}**")
                date_range = st.date_input(
                    f"Range {col}",
                    value=(min_date.date(), max_date.date()),
                    min_value=min_date.date(),
                    max_value=max_date.date(),
                    key=f"date_{col}"
                )
                if len(date_range) == 2:
                    start, end = date_range
                    filtered_df = filtered_df[
                        (filtered_df[col].dt.date >= start) &
                        (filtered_df[col].dt.date <= end)
                    ]

        # Filtri per colonne numeriche (range slider)
        for col in numeric_cols[:5]:  # Limita a 5 colonne numeriche
            min_val = float(df[col].dropna().min()) if not df[col].dropna().empty else 0.0
            max_val = float(df[col].dropna().max()) if not df[col].dropna().empty else 0.0
            if pd.notna(min_val) and pd.notna(max_val) and min_val < max_val:
                values = st.slider(
                    f"📊 {col}",
                    min_value=min_val,
                    max_value=max_val,
                    value=(min_val, max_val),
                    key=f"slider_{col}"
                )
                filtered_df = filtered_df[
                    (filtered_df[col] >= values[0]) &
                    (filtered_df[col] <= values[1])
                ]

        # Mostra conteggio filtrato
        st.markdown("---")
        st.info(f"📋 Righe: **{len(filtered_df):,}** / {len(df):,}")

    return filtered_df


# ============================================================
# SEZIONE KPI
# ============================================================
def render_kpi_section(df, numeric_cols):
    """Renderizza le card KPI principali."""
    st.markdown("### 📈 Indicatori Chiave (KPI)")

    if df.empty:
        st.info("Nessun dato da mostrare per i KPI.")
        return

    cols_to_show = numeric_cols[:6]
    if not cols_to_show:
        st.info("Nessuna colonna numerica trovata per i KPI.")
        return

    cols = st.columns(len(cols_to_show))
    colors = ["#4361ee", "#3a0ca3", "#7209b7", "#f72585", "#4cc9f0", "#4895ef"]

    for i, col_name in enumerate(cols_to_show):
        with cols[i]:
            total = df[col_name].sum()
            mean = df[col_name].mean()
            st.markdown(
                create_kpi_card(
                    label=col_name,
                    value=format_number(total),
                    color=colors[i % len(colors)]
                ),
                unsafe_allow_html=True
            )
            st.caption(f"Media: {format_number(mean)}")


# ============================================================
# SEZIONE GRAFICI
# ============================================================
def render_chart_section(df, numeric_cols, categorical_cols, date_cols):
    """Renderizza la sezione grafici interattivi."""
    st.markdown("---")
    st.markdown("### 📊 Grafici Interattivi")

    # Selettore tipo grafico
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Barre", "📈 Linee", "🔵 Scatter", "🍩 Torta", "🗺️ Heatmap"
    ])

    # ---- TAB GRAFICO A BARRE ----
    with tab1:
        render_bar_chart(df, numeric_cols, categorical_cols, date_cols)

    # ---- TAB GRAFICO A LINEE ----
    with tab2:
        render_line_chart(df, numeric_cols, categorical_cols, date_cols)

    # ---- TAB SCATTER PLOT ----
    with tab3:
        render_scatter_chart(df, numeric_cols, categorical_cols)

    # ---- TAB GRAFICO A TORTA ----
    with tab4:
        render_pie_chart(df, numeric_cols, categorical_cols)

    # ---- TAB HEATMAP ----
    with tab5:
        render_heatmap(df, numeric_cols)


def render_bar_chart(df, numeric_cols, categorical_cols, date_cols):
    """Grafico a barre."""
    col1, col2, col3 = st.columns(3)
    all_cats = categorical_cols + date_cols

    if not all_cats or not numeric_cols:
        st.info("Servono almeno una colonna categorica e una numerica.")
        return

    with col1:
        x_col = st.selectbox("Asse X", all_cats, key="bar_x")
    with col2:
        y_col = st.selectbox("Valore Y", numeric_cols, key="bar_y")
    with col3:
        agg = st.selectbox("Aggregazione", ["sum", "mean", "count", "max", "min"], key="bar_agg")

    color_col = st.selectbox("Colore (opzionale)", ["Nessuno"] + categorical_cols, key="bar_color")

    grouped = df.groupby(x_col)[y_col].agg(agg).reset_index()
    grouped = grouped.sort_values(y_col, ascending=False).head(30)

    if color_col != "Nessuno":
        grouped = df.groupby([x_col, color_col])[y_col].agg(agg).reset_index()
        fig = px.bar(grouped, x=x_col, y=y_col, color=color_col,
                     template="plotly_white", barmode="group")
    else:
        fig = px.bar(grouped, x=x_col, y=y_col,
                     template="plotly_white",
                     color_discrete_sequence=["#4361ee"])

    fig.update_layout(
        height=500,
        font=dict(family="Segoe UI", size=12),
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.1)")
    )
    st.plotly_chart(fig, use_container_width=True)


def render_line_chart(df, numeric_cols, categorical_cols, date_cols):
    """Grafico a linee."""
    all_x = date_cols + categorical_cols + numeric_cols

    if not all_x or not numeric_cols:
        st.info("Servono colonne per creare un grafico a linee.")
        return

    col1, col2 = st.columns(2)
    with col1:
        x_col = st.selectbox("Asse X", all_x, key="line_x")
    with col2:
        y_cols = st.multiselect("Valori Y", numeric_cols,
                                default=[numeric_cols[0]] if numeric_cols else [],
                                key="line_y")

    if not y_cols:
        st.warning("Seleziona almeno un valore per Y.")
        return

    color_col = st.selectbox("Raggruppa per (opzionale)", ["Nessuno"] + categorical_cols, key="line_color")

    if color_col != "Nessuno":
        fig = px.line(df.sort_values(x_col), x=x_col, y=y_cols[0], color=color_col,
                      template="plotly_white", markers=True)
    else:
        fig = go.Figure()
        colors = px.colors.qualitative.Set2
        sorted_df = df.sort_values(x_col)
        for i, y_col in enumerate(y_cols):
            fig.add_trace(go.Scatter(
                x=sorted_df[x_col], y=sorted_df[y_col],
                name=str(y_col), mode='lines+markers',
                line=dict(color=colors[i % len(colors)], width=2),
                marker=dict(size=4)
            ))

    fig.update_layout(
        height=500,
        template="plotly_white",
        font=dict(family="Segoe UI", size=12),
        plot_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified"
    )
    st.plotly_chart(fig, use_container_width=True)


def render_scatter_chart(df, numeric_cols, categorical_cols):
    """Scatter plot."""
    if len(numeric_cols) < 2:
        st.info("Servono almeno 2 colonne numeriche per lo scatter plot.")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        x_col = st.selectbox("Asse X", numeric_cols, index=0, key="scatter_x")
    with col2:
        y_col = st.selectbox("Asse Y", numeric_cols,
                             index=min(1, len(numeric_cols)-1), key="scatter_y")
    with col3:
        size_col = st.selectbox("Dimensione (opzionale)",
                                ["Nessuno"] + numeric_cols, key="scatter_size")

    color_col = st.selectbox("Colore", ["Nessuno"] + categorical_cols + numeric_cols, key="scatter_color")

    kwargs = dict(x=x_col, y=y_col, template="plotly_white")
    if color_col != "Nessuno":
        kwargs["color"] = color_col
    if size_col != "Nessuno":
        kwargs["size"] = size_col
        kwargs["size_max"] = 30

    fig = px.scatter(df, **kwargs, opacity=0.7)
    fig.update_layout(
        height=500,
        font=dict(family="Segoe UI", size=12),
        plot_bgcolor="rgba(0,0,0,0)"
    )

    # Aggiungi trendline
    if st.checkbox("Mostra trendline", key="scatter_trend"):
        fig = px.scatter(df, **kwargs, opacity=0.7, trendline="ols")
        fig.update_layout(height=500, font=dict(family="Segoe UI", size=12))

    st.plotly_chart(fig, use_container_width=True)


def render_pie_chart(df, numeric_cols, categorical_cols):
    """Grafico a torta/donut."""
    if not categorical_cols or not numeric_cols:
        st.info("Servono colonne categoriche e numeriche.")
        return

    col1, col2 = st.columns(2)
    with col1:
        cat_col = st.selectbox("Categorie", categorical_cols, key="pie_cat")
    with col2:
        val_col = st.selectbox("Valori", numeric_cols, key="pie_val")

    donut = st.checkbox("Stile Donut", value=True, key="pie_donut")

    grouped = df.groupby(cat_col)[val_col].sum().reset_index()
    grouped = grouped.sort_values(val_col, ascending=False).head(15)

    fig = px.pie(
        grouped, names=cat_col, values=val_col,
        hole=0.4 if donut else 0,
        template="plotly_white",
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    fig.update_traces(textposition='inside', textinfo='percent+label')
    fig.update_layout(
        height=500,
        font=dict(family="Segoe UI", size=12)
    )
    st.plotly_chart(fig, use_container_width=True)


def render_heatmap(df, numeric_cols):
    """Matrice di correlazione heatmap."""
    if len(numeric_cols) < 2:
        st.info("Servono almeno 2 colonne numeriche per la heatmap.")
        return

    selected_cols = st.multiselect(
        "Seleziona colonne",
        numeric_cols,
        default=numeric_cols[:8],
        key="heatmap_cols"
    )

    if len(selected_cols) < 2:
        st.warning("Seleziona almeno 2 colonne.")
        return

    corr = df[selected_cols].corr()

    fig = px.imshow(
        corr,
        text_auto=".2f",
        color_continuous_scale="RdBu_r",
        zmin=-1, zmax=1,
        template="plotly_white"
    )
    fig.update_layout(
        height=500,
        font=dict(family="Segoe UI", size=12),
        title="Matrice di Correlazione"
    )
    st.plotly_chart(fig, use_container_width=True)


# ============================================================
# SEZIONE TABELLA DATI
# ============================================================
def render_data_table(df):
    """Renderizza la tabella dati con ricerca e ordinamento."""
    st.markdown("---")
    st.markdown("### 📋 Esplora Dati")

    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        search = st.text_input("🔍 Cerca nei dati", "", key="search_table")
    with col2:
        n_rows = st.selectbox("Righe da mostrare", [10, 25, 50, 100, 500], key="n_rows")
    with col3:
        col_list = [str(c) for c in df.columns.tolist()]
        sort_col = st.selectbox("Ordina per", col_list, key="sort_col")

    display_df = df.copy()
    # Assicurati che i nomi colonna siano stringhe
    display_df.columns = [str(c) for c in display_df.columns]

    # Ricerca
    if search:
        escaped_search = re.escape(search)
        mask = display_df.astype(str).apply(
            lambda x: x.str.contains(escaped_search, case=False, na=False)
        ).any(axis=1)
        display_df = display_df[mask]

    # Ordinamento
    ascending = st.checkbox("Ordine crescente", value=True, key="sort_asc")
    display_df = display_df.sort_values(sort_col, ascending=ascending)

    # Mostra tabella
    st.dataframe(
        display_df.head(n_rows),
        use_container_width=True,
        height=400
    )

    # Statistiche descrittive
    with st.expander("📊 Statistiche Descrittive"):
        st.dataframe(df.describe().round(2), use_container_width=True)


# ============================================================
# SEZIONE GRAFICI AVANZATI
# ============================================================
def render_advanced_charts(df, numeric_cols, categorical_cols, date_cols):
    """Grafici avanzati: istogramma, box plot, area chart."""
    st.markdown("---")
    st.markdown("### 🔬 Analisi Avanzata")

    tab1, tab2, tab3 = st.tabs(["📊 Istogramma", "📦 Box Plot", "📈 Area Chart"])

    with tab1:
        if numeric_cols:
            col1, col2 = st.columns(2)
            with col1:
                hist_col = st.selectbox("Colonna", numeric_cols, key="hist_col")
            with col2:
                n_bins = st.slider("Numero bin", 5, 100, 30, key="hist_bins")

            color = st.selectbox("Colore per", ["Nessuno"] + categorical_cols, key="hist_color")

            if color != "Nessuno":
                fig = px.histogram(df, x=hist_col, nbins=n_bins, color=color,
                                   template="plotly_white", marginal="box")
            else:
                fig = px.histogram(df, x=hist_col, nbins=n_bins,
                                   template="plotly_white", marginal="box",
                                   color_discrete_sequence=["#4361ee"])

            fig.update_layout(height=450, font=dict(family="Segoe UI", size=12))
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        if numeric_cols and categorical_cols:
            col1, col2 = st.columns(2)
            with col1:
                box_y = st.selectbox("Valori", numeric_cols, key="box_y")
            with col2:
                box_x = st.selectbox("Gruppi", categorical_cols, key="box_x")

            fig = px.box(df, x=box_x, y=box_y, template="plotly_white",
                         color=box_x, color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_layout(height=450, font=dict(family="Segoe UI", size=12),
                              showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Servono colonne numeriche e categoriche per il box plot.")

    with tab3:
        if date_cols and numeric_cols:
            col1, col2 = st.columns(2)
            with col1:
                area_x = st.selectbox("Asse X (tempo)", date_cols, key="area_x")
            with col2:
                area_y = st.selectbox("Valori", numeric_cols, key="area_y")

            color_area = st.selectbox("Raggruppa per", ["Nessuno"] + categorical_cols, key="area_color")

            if color_area != "Nessuno":
                agg_df = df.groupby([area_x, color_area])[area_y].sum().reset_index()
                fig = px.area(agg_df.sort_values(area_x), x=area_x, y=area_y,
                              color=color_area, template="plotly_white")
            else:
                agg_df = df.groupby(area_x)[area_y].sum().reset_index()
                fig = px.area(agg_df.sort_values(area_x), x=area_x, y=area_y,
                              template="plotly_white",
                              color_discrete_sequence=["#4361ee"])

            fig.update_layout(height=450, font=dict(family="Segoe UI", size=12))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Serve almeno una colonna data e una numerica per l'area chart.")


# ============================================================
# SEZIONE ORE PAGATE
# ============================================================
def render_ore_pagate_section(df):
    """Sezione dedicata per analisi Ore Pagate per User FID e giorno."""
    # Cerca colonne rilevanti (case-insensitive, match parziale)
    cols_lower = {str(c).lower().strip(): c for c in df.columns}

    # Trova colonna ore pagate (match specifico)
    ore_col = None
    for key, original in cols_lower.items():
        if 'ore pag' in key or 'ore_pag' in key or key == 'ore pagate':
            ore_col = original
            break

    # Trova colonna user fid (match specifico, evita falsi positivi con 'fid')
    user_col = None
    for key, original in cols_lower.items():
        if 'user fid' in key or 'user_fid' in key or 'userfid' in key:
            user_col = original
            break

    # Trova colonna data/giorno (match specifico per evitare "aggiornadata" ecc.)
    date_col = None
    date_patterns = ['data', 'date', 'giorno', 'day']
    for key, original in cols_lower.items():
        # Controlla che sia una parola intera, non parte di un'altra parola
        for pattern in date_patterns:
            if key == pattern or key.startswith(pattern + ' ') or key.startswith(pattern + '_') or key.endswith(' ' + pattern) or key.endswith('_' + pattern):
                date_col = original
                break
        if date_col:
            break

    # Se non trova le colonne, non mostra la sezione
    if ore_col is None:
        return

    st.markdown("---")
    st.markdown("### ⏱️ Analisi Ore Pagate")

    # Lavora su una copia per non modificare il DataFrame filtrato originale
    df = df.copy()

    # Converti ore pagate in numerico
    df[ore_col] = pd.to_numeric(df[ore_col], errors='coerce').fillna(0)

    # --- Riepilogo per User FID ---
    if user_col and date_col:
        # Converti data se necessario
        try:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df['_giorno'] = df[date_col].dt.date
        except Exception:
            df['_giorno'] = df[date_col]

        st.markdown("#### 📋 Ore Pagate per Utente e Giorno")

        # Tabella riepilogativa: somma ore per user fid + giorno
        summary = df.groupby([user_col, '_giorno'])[ore_col].sum().reset_index()
        summary.columns = [user_col, 'Giorno', 'Totale Ore Pagate']
        summary = summary.sort_values(['Giorno', user_col], ascending=[False, True])

        # KPI veloce
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Totale Ore Pagate", f"{df[ore_col].sum():,.0f}")
        with col2:
            st.metric("Utenti Unici", f"{df[user_col].nunique():,}")
        with col3:
            st.metric("Media Ore/Utente", f"{summary['Totale Ore Pagate'].mean():.1f}")

        # Tabella dettagliata
        st.dataframe(summary, use_container_width=True, hide_index=True, height=400)

        # Grafico a barre: ore per utente
        st.markdown("#### 📊 Ore Pagate per Utente")
        ore_per_user = df.groupby(user_col)[ore_col].sum().reset_index()
        ore_per_user.columns = [user_col, 'Totale Ore']
        ore_per_user = ore_per_user.sort_values('Totale Ore', ascending=False).head(30)

        fig = px.bar(
            ore_per_user, x=user_col, y='Totale Ore',
            template="plotly_white",
            color_discrete_sequence=["#4361ee"],
            text='Totale Ore'
        )
        fig.update_traces(textposition='outside')
        fig.update_layout(height=450, font=dict(family="Segoe UI", size=12))
        st.plotly_chart(fig, use_container_width=True)

        # Grafico per giorno
        st.markdown("#### 📅 Ore Pagate per Giorno")
        ore_per_giorno = df.groupby('_giorno')[ore_col].sum().reset_index()
        ore_per_giorno.columns = ['Giorno', 'Totale Ore']
        ore_per_giorno = ore_per_giorno.sort_values('Giorno')

        fig2 = px.bar(
            ore_per_giorno, x='Giorno', y='Totale Ore',
            template="plotly_white",
            color_discrete_sequence=["#7209b7"],
            text='Totale Ore'
        )
        fig2.update_traces(textposition='outside')
        fig2.update_layout(height=450, font=dict(family="Segoe UI", size=12))
        st.plotly_chart(fig2, use_container_width=True)

        # Scarica riepilogo
        csv_summary = summary.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📥 Scarica Riepilogo Ore Pagate",
            data=csv_summary,
            file_name=f"ore_pagate_riepilogo_{datetime.now().strftime('%Y%m%d')}.csv",
            mime='text/csv',
            use_container_width=True
        )

        # Cleanup colonna temporanea
        df.drop(columns=['_giorno'], inplace=True, errors='ignore')

    elif user_col:
        # Solo per user fid senza data
        st.markdown("#### 📋 Ore Pagate per Utente")
        ore_per_user = df.groupby(user_col)[ore_col].sum().reset_index()
        ore_per_user.columns = [user_col, 'Totale Ore Pagate']
        ore_per_user = ore_per_user.sort_values('Totale Ore Pagate', ascending=False)

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Totale Ore Pagate", f"{df[ore_col].sum():,.0f}")
        with col2:
            st.metric("Utenti Unici", f"{df[user_col].nunique():,}")

        st.dataframe(ore_per_user, use_container_width=True, hide_index=True)

    else:
        # Solo colonna ore pagate, senza user fid
        st.metric("Totale Ore Pagate", f"{df[ore_col].sum():,.0f}")


# ============================================================
# SEZIONE EXPORT
# ============================================================
def render_export_section(df):
    """Sezione per esportare i dati filtrati."""
    st.markdown("---")
    st.markdown("### 💾 Esporta Dati")

    col1, col2, col3 = st.columns(3)

    with col1:
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Scarica CSV",
            data=csv,
            file_name=f"dashboard_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime='text/csv',
            use_container_width=True
        )

    with col2:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Dati')
        excel_data = buffer.getvalue()
        st.download_button(
            label="📥 Scarica Excel",
            data=excel_data,
            file_name=f"dashboard_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            use_container_width=True
        )

    with col3:
        json_data = df.to_json(orient='records', date_format='iso').encode('utf-8')
        st.download_button(
            label="📥 Scarica JSON",
            data=json_data,
            file_name=f"dashboard_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime='application/json',
            use_container_width=True
        )


# ============================================================
# MAIN APP
# ============================================================
def render_merge_section(df1, df2):
    """Mostra opzioni di merge tra due DataFrame."""
    st.markdown("---")
    st.markdown("### 🔗 Merge dei File")
    st.info(f"📄 **File 1**: {df1.shape[0]} righe × {df1.shape[1]} colonne  |  📄 **File 2**: {df2.shape[0]} righe × {df2.shape[1]} colonne")

    # Trova colonne in comune
    common_cols = sorted(set(df1.columns) & set(df2.columns))

    # Scelta: merge o no
    do_merge = st.checkbox("🔗 Vuoi unire (merge) i due file?", value=False, key="do_merge")

    if not do_merge:
        # Mostra scelta quale file usare
        file_choice = st.radio(
            "Quale file vuoi analizzare?",
            ["📄 File 1", "📄 File 2"],
            key="file_choice",
            horizontal=True
        )
        return df1 if file_choice == "📄 File 1" else df2

    # Opzioni merge
    col1, col2 = st.columns(2)

    with col1:
        merge_type = st.selectbox(
            "Tipo di merge",
            ["inner", "left", "right", "outer"],
            index=0,
            key="merge_type",
            help="inner = solo righe comuni | left = tutte dal File 1 | right = tutte dal File 2 | outer = tutte"
        )

    with col2:
        if common_cols:
            merge_on = st.multiselect(
                "Colonna/e chiave (comuni)",
                common_cols,
                default=[common_cols[0]],
                key="merge_on",
                help="Seleziona le colonne che collegano i due file"
            )
        else:
            merge_on = []
            st.warning("⚠️ Nessuna colonna con lo stesso nome tra i due file.")

    # Merge manuale con colonne diverse
    if not common_cols or st.checkbox("Usa colonne diverse come chiave", key="diff_key"):
        col_a, col_b = st.columns(2)
        with col_a:
            left_key = st.selectbox("Chiave File 1", df1.columns.tolist(), key="left_key")
        with col_b:
            right_key = st.selectbox("Chiave File 2", df2.columns.tolist(), key="right_key")
        merge_on = None  # segnale per usare left_on/right_on
    else:
        left_key = None
        right_key = None

    # Gestione suffissi per colonne duplicate
    suffix_l = "_file1"
    suffix_r = "_file2"

    # Esegui merge
    if st.button("🚀 Esegui Merge", use_container_width=True, type="primary", key="btn_merge"):
        try:
            if merge_on and len(merge_on) > 0:
                merged = pd.merge(df1, df2, on=merge_on, how=merge_type, suffixes=(suffix_l, suffix_r))
            elif left_key and right_key:
                merged = pd.merge(df1, df2, left_on=left_key, right_on=right_key,
                                  how=merge_type, suffixes=(suffix_l, suffix_r))
            else:
                st.error("❌ Seleziona almeno una colonna chiave per il merge.")
                return df1

            st.session_state['merged_df'] = merged
            st.success(f"✅ Merge completato! Risultato: **{merged.shape[0]} righe × {merged.shape[1]} colonne**")
        except Exception as e:
            st.error(f"❌ Errore durante il merge: {e}")
            return df1

    # Ritorna il risultato
    if 'merged_df' in st.session_state:
        st.markdown("**Anteprima merge (prime 5 righe):**")
        st.dataframe(st.session_state['merged_df'].head(), use_container_width=True)
        return st.session_state['merged_df']

    st.info("👆 Configura le opzioni e premi **Esegui Merge** per unire i file.")
    return df1


def main():
    """Funzione principale dell'applicazione."""

    uploaded_file_1, uploaded_file_2 = render_sidebar()

    # Header
    st.markdown("""
    <div style="text-align: center; padding: 1rem 0;">
        <h1 style="color: #4361ee; margin-bottom: 0.2rem;">📊 Data Dashboard Pro</h1>
        <p style="color: #6c757d; font-size: 1.1rem;">Analizza i tuoi dati Excel e CSV con grafici professionali</p>
        <p style="color: #495057; font-size: 0.9rem; margin-top: 0.5rem; font-weight: 500;">Krijuar nga Enigert Hasanllari</p>
    </div>
    """, unsafe_allow_html=True)

    # Determina sorgente dati
    df = None
    df1 = None
    df2 = None

    # Carica File 1
    if uploaded_file_1 is not None:
        result, extra = load_data(uploaded_file_1)
        if isinstance(result, dict):
            sheet = st.selectbox("📄 Seleziona foglio (File 1)", extra, key="sheet_f1")
            df1 = result[sheet]
        elif isinstance(result, pd.DataFrame):
            df1 = result
        else:
            st.error(f"❌ Errore nel caricamento File 1: {extra}")
            return

    # Carica File 2
    if uploaded_file_2 is not None:
        result2, extra2 = load_data(uploaded_file_2)
        if isinstance(result2, dict):
            sheet2 = st.selectbox("📄 Seleziona foglio (File 2)", extra2, key="sheet_f2")
            df2 = result2[sheet2]
        elif isinstance(result2, pd.DataFrame):
            df2 = result2
        else:
            st.error(f"❌ Errore nel caricamento File 2: {extra2}")
            return

    # Logica: due file caricati → mostra opzione merge
    if df1 is not None and df2 is not None:
        df = render_merge_section(df1, df2)
    elif df1 is not None:
        df = df1
    elif 'sample_data' in st.session_state:
        df = st.session_state['sample_data']

    if df is None:
        # Schermata di benvenuto
        st.markdown("---")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("""
            <div class="feature-card">
                <h3>📁 Carica File</h3>
                <p>Supporta CSV, Excel (.xlsx, .xls) con rilevamento automatico del formato</p>
            </div>
            """, unsafe_allow_html=True)
        with col2:
            st.markdown("""
            <div class="feature-card">
                <h3>� Merge File</h3>
                <p>Carica 2 file e uniscili con merge automatico sulle colonne in comune</p>
            </div>
            """, unsafe_allow_html=True)
        with col3:
            st.markdown("""
            <div class="feature-card">
                <h3>📊 Grafici Interattivi</h3>
                <p>Barre, linee, scatter, torta, heatmap, box plot e molto altro</p>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("""
        <div style="text-align:center; padding: 2rem; color: #6c757d;">
            <h3>👈 Carica un file dalla sidebar o genera dati di esempio per iniziare</h3>
        </div>
        """, unsafe_allow_html=True)
        return

    # Sanitizza nomi colonne (converti None, datetime, ecc. in stringhe)
    df.columns = [str(c) if c is not None else f"Colonna_{i}" for i, c in enumerate(df.columns)]

    # Rileva tipi colonne
    df = fix_duplicate_columns(df)
    numeric_cols, categorical_cols, date_cols = detect_column_types(df)

    # Applica filtri
    filtered_df = apply_filters(df, numeric_cols, categorical_cols, date_cols)

    # Info dataset
    with st.expander("ℹ️ Info Dataset", expanded=False):
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Righe", f"{len(filtered_df):,}")
        col2.metric("Colonne", f"{len(filtered_df.columns):,}")
        col3.metric("Numeriche", len(numeric_cols))
        col4.metric("Categoriche", len(categorical_cols))

        # Info colonne
        dtypes_df = pd.DataFrame({
            'Colonna': filtered_df.columns,
            'Tipo': filtered_df.dtypes.astype(str),
            'Non-Null': filtered_df.notna().sum().values,
            'Null (%)': (filtered_df.isna().sum() / max(len(filtered_df), 1) * 100).round(1).values
        })
        st.dataframe(dtypes_df, use_container_width=True, hide_index=True)

    # Sezione Ore Pagate (se presente)
    render_ore_pagate_section(filtered_df)

    # Sezioni Dashboard
    render_kpi_section(filtered_df, numeric_cols)
    render_chart_section(filtered_df, numeric_cols, categorical_cols, date_cols)
    render_advanced_charts(filtered_df, numeric_cols, categorical_cols, date_cols)
    render_data_table(filtered_df)
    render_export_section(filtered_df)

    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align:center; padding:1rem;'>"
        "<p style='color:#adb5bd; margin-bottom:0.3rem;'>📊 Data Dashboard Pro | Creato con Streamlit & Plotly</p>"
        "<p style='color:#6c757d; font-size:0.95rem; font-weight:500;'>Krijuar nga Enigert Hasanllari</p>"
        "</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
