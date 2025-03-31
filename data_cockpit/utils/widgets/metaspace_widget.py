import os
import ipywidgets as widgets
from IPython.display import display, clear_output, HTML, Javascript
import json
from math import ceil
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import asyncio

# Load CSS styles
css_path = os.path.join(os.path.dirname(__file__), "styles", "custom_styles.css")
with open(css_path, "r") as f:
    styles = f.read()
display(HTML(f"<style>{styles}</style>"))

# Use custom components for consistent styling
from .custom_components import (
    create_custom_header,
    create_custom_text,
    create_custom_dropdown,
    create_custom_button,
    create_custom_slider,
    create_custom_output
)

class MetaspaceDatasetsWidget:
    """
    Widget to search and select Metaspace datasets via GraphQL.
    Uses custom components for consistent styling.
    """
    def __init__(self):
        self.datasets = None
        self.selected_dataset = None
        self.s3_uri = None
        self.current_page = 1
        self.limit = 20
        self.total_pages = 1

        # Configure GraphQL client for Metaspace
        transport = RequestsHTTPTransport(
            url='https://metaspace2020.eu/graphql',
            verify=True,
            retries=3,
        )
        self.client = Client(transport=transport, fetch_schema_from_transport=True)

        # Create custom search input and button
        self.search_input = create_custom_text(
            value="",
            placeholder="Search Metaspace datasets...",
            description="Search:"
        )
        self.search_input.add_class("custom-search")
        self.search_button = create_custom_button("Search", icon="search", width="auto")
        self.search_button.button_style = "info"
        self.search_button.tooltip = "Search Metaspace datasets"
        self.search_button.on_click(self.on_search_clicked)

        # Create custom dropdown for dataset suggestions
        self.dataset_dropdown = create_custom_dropdown(
            options=[], 
            description="Select Dataset:",
            tooltip="Select a dataset from the suggestions"
        )
        self.dataset_dropdown.disabled = True
        self.dataset_dropdown_id = self.dataset_dropdown.model_id

        # Create custom file key input
        self.file_key_input = create_custom_text(
            value="",
            placeholder="Enter file key (e.g., path/to/file.vcf.gz)...",
            description="File Key:"
        )
        self.file_key_input.disabled = True

        # Create custom AWS region dropdown
        self.region_dropdown = create_custom_dropdown(
            options=[
                "us-east-1", "us-east-2", "us-west-1", "us-west-2",
                "af-south-1", "ap-south-1", "ap-northeast-1", "ap-northeast-2",
                "eu-central-1", "eu-west-1", "eu-west-2", "eu-west-3"
            ],
            description="AWS Region:"
        )
        self.region_dropdown.value = "us-east-1"
        self.region_dropdown.disabled = True

        # Create custom batch size slider
        self.batch_slider = create_custom_slider(
            value=5,
            min_value=1,
            max_value=100,
            step=1,
            description="Batch Size:"
        )
        self.batch_slider.disabled = True

        # Create custom output area
        self.output = create_custom_output()

        # Results container
        self.results_box = widgets.VBox(
            layout=widgets.Layout(justify_content="center", align_items="center")
        )

        # Pagination controls
        self.prev_button = create_custom_button("Previous", icon="", width="auto")
        self.next_button = create_custom_button("Next", icon="", width="auto")
        self.prev_button.disabled = True
        self.next_button.disabled = True
        self.page_label = widgets.Label(value="Page 1/1")
        self.pagination_controls = widgets.HBox(
            [self.prev_button, self.page_label, self.next_button],
            layout=widgets.Layout(justify_content="center", align_items="center")
        )
        self.prev_button.on_click(self.on_prev_page)
        self.next_button.on_click(self.on_next_page)

        # Selected dataset label
        self.selected_label = widgets.HTML(
            value="<b>Selected Metaspace Dataset:</b> None",
            layout=widgets.Layout(justify_content="center", align_items="center", margin="10px")
        )

        # Main widget layout
        self.widget = widgets.VBox([
            self.search_input,
            self.search_button,
            self.results_box,
            self.pagination_controls,
            self.selected_label
        ], layout=widgets.Layout(align_items="center"))

        # Event connections
        self.search_input.observe(self.on_search_bar_change, names="value")
        self.dataset_dropdown.observe(self.on_dataset_selected, names="value")
        self.file_key_input.observe(self.on_file_key_change, names="value")

    def on_search_clicked(self, b):
        self.current_page = 1
        self.page_label.value = "Page 1/1"
        self.perform_search()

    def on_search_bar_change(self, change):
        input_text = change['new'].strip().lower()
        if not input_text:
            self.dataset_dropdown.options = []
            self.dataset_dropdown.disabled = True
            return

        suggestions = [
            (name, dataset['prefix']) for name, dataset in self.datasets.items()
            if input_text in name.lower() or any(input_text in tag.lower() for tag in dataset.get('tags', []))
        ]

        if suggestions:
            suggestions_sorted = sorted(suggestions, key=lambda x: x[0])
            self.dataset_dropdown.options = [f"{name} ({prefix})" for name, prefix in suggestions_sorted]
            self.dataset_dropdown.disabled = False

            js_code = f"""
            var dropdown = document.querySelector('[data-widget-id="{self.dataset_dropdown_id}"] button.dropdown-toggle');
            if(dropdown) {{
                dropdown.click();
            }}
            """
            display(Javascript(js_code))
        else:
            self.dataset_dropdown.options = ["No datasets found"]
            self.dataset_dropdown.disabled = True

    def on_dataset_selected(self, change):
        selected = change['new']
        if not selected or selected == "No datasets found":
            self.selected_dataset = None
            self.s3_uri = None
            self.file_key_input.value = ""
            self.file_key_input.disabled = True
            self.region_dropdown.value = "us-east-1"
            self.region_dropdown.disabled = True
            self.batch_slider.value = 5
            self.batch_slider.disabled = True
            with self.output:
                clear_output()
                display(HTML("<p style='color: red;'><strong>No valid dataset selected.</strong></p>"))
            return

        name = selected.split(" (")[0]
        self.selected_dataset = name
        dataset_info = self.datasets.get(name, {})
        prefix = dataset_info.get("prefix", "")
        url = dataset_info.get("url", "")

        with self.output:
            clear_output()
            display(HTML(f"<p style='font-size: 14px;'><strong>Selected Dataset:</strong> {name}</p>"))
            if url:
                display(HTML(f"<p style='font-size: 14px;'>More info: <a href='{url}' target='_blank'>{url}</a></p>"))
            else:
                display(HTML("<p style='font-size: 14px;'>No additional information available.</p>"))

        self.file_key_input.disabled = False
        self.region_dropdown.disabled = False
        self.batch_slider.disabled = False

    def on_file_key_change(self, change):
        key = change['new'].strip()
        if not key or not self.selected_dataset:
            self.s3_uri = None
            with self.output:
                clear_output()
                display(HTML("<p style='color: orange;'><strong>Please select a dataset and enter a valid file key.</strong></p>"))
            return

        dataset_info = self.datasets.get(self.selected_dataset, {})
        prefix = dataset_info.get("prefix", "")

        if prefix:
            try:
                # Parse S3 URI and construct full URI
                bucket, prefix_str = parse_s3_uri(prefix)
                if prefix_str.endswith('/') and key.startswith('/'):
                    prefix_str = prefix_str[:-1]
                self.s3_uri = f"s3://{bucket}/{prefix_str}{key}"
                with self.output:
                    clear_output()
                    display(HTML(f"<p style='font-size: 14px;'><strong>S3 URI set to:</strong> {self.s3_uri}</p>"))
            except Exception as e:
                self.s3_uri = None
                with self.output:
                    clear_output()
                    display(HTML(f"<p style='color: red;'><strong>Error parsing S3 URI:</strong> {e}</p>"))
        else:
            self.s3_uri = None
            with self.output:
                clear_output()
                display(HTML("<p style='color: red;'><strong>Invalid dataset prefix.</strong></p>"))

    def perform_search(self):
        query = gql("""
        query SearchDatasets($query: String, $offset: Int, $limit: Int) {
            allDatasets(simpleQuery: $query, offset: $offset, limit: $limit) {
                id
                name
                ionThumbnailUrl
                downloadLinkJson
            }
            countDatasets(simpleQuery: $query)
        }
        """)
        query_str = self.search_input.value.strip()
        params = {
            "query": query_str,
            "offset": (self.current_page - 1) * self.limit,
            "limit": self.limit
        }
        try:
            result = self.client.execute(query, variable_values=params)
            datasets = result.get("allDatasets", [])
            total_count = result.get("countDatasets", 0)
            self.total_pages = ceil(total_count / self.limit) if total_count else 1

            if not datasets:
                self.results_box.children = [widgets.HTML(value="No datasets found.")]
                self.next_button.disabled = True
                self.prev_button.disabled = (self.current_page == 1)
            else:
                items = []
                for ds in datasets:
                    img_html = widgets.HTML(
                        value=f"<img src='{ds.get('ionThumbnailUrl', '')}' style='width:100px;height:100px;margin-right:10px;border-radius:10px;'/>"
                    )
                    btn = create_custom_button(ds["name"], icon="", width="auto")
                    btn.tooltip = f"ID: {ds['id']}"
                    # Minimal comment: on_select sets selected dataset and its S3 URI.
                    def on_select(btn, ds=ds):
                        self.selected_dataset = ds["id"]
                        s3_uri = None
                        if ds.get("downloadLinkJson"):
                            try:
                                link_info = json.loads(ds["downloadLinkJson"])
                                s3_uri = link_info.get("s3_uri", ds["id"])
                            except Exception as e:
                                s3_uri = ds["id"]
                        else:
                            s3_uri = ds["id"]
                        self.s3_uri = s3_uri
                        self.selected_label.value = (
                            f"<b>Selected Metaspace Dataset:</b> {ds['name']} (ID: {ds['id']})"
                        )
                    btn.on_click(on_select)
                    
                    card = widgets.HBox(
                        [img_html, btn],
                        layout=widgets.Layout(padding="10px", margin="5px")
                    )
                    card.add_class("custom-card")
                    items.append(card)
                
                grid = widgets.GridBox(
                    children=items,
                    layout=widgets.Layout(
                        grid_template_columns="repeat(3, 1fr)",
                        grid_gap="10px"
                    )
                )
                self.results_box.children = [grid]

                self.prev_button.disabled = (self.current_page == 1)
                self.next_button.disabled = (self.current_page >= self.total_pages)
            self.page_label.value = f"Page {self.current_page}/{self.total_pages}"
        except Exception as e:
            self.results_box.children = [widgets.HTML(value=f"<p style='color:red;'>Error: {e}</p>")]

    def on_next_page(self, b):
        if self.current_page < self.total_pages:
            self.current_page += 1
            self.perform_search()

    def on_prev_page(self, b):
        if self.current_page > 1:
            self.current_page -= 1
            self.perform_search()

    def get_widget(self):
        return self.widget

    def reset(self):
        self.search_input.value = ""
        self.dataset_dropdown.options = []
        self.dataset_dropdown.value = None
        self.dataset_dropdown.disabled = True
        self.file_key_input.value = ""
        self.file_key_input.disabled = True
        self.region_dropdown.value = "us-east-1"
        self.region_dropdown.disabled = True
        self.batch_slider.value = 5
        self.batch_slider.min = 1
        self.batch_slider.max = 100
        self.batch_slider.disabled = True
        self.s3_uri = None
        self.selected_dataset = None
        with self.output:
            clear_output()
            display(HTML("<p style='color: blue;'><strong>Widget reset successfully.</strong></p>"))
