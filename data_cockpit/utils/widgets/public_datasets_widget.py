import ipywidgets as widgets
from IPython.display import display, clear_output, HTML, Javascript
from ..s3.s3_utils import parse_s3_uri  # Adjust this import as needed
import asyncio
import os

# Load CSS from custom_styles.css
css_path = os.path.join(os.path.dirname(__file__), "styles", "custom_styles.css")
with open(css_path, "r") as f:
    styles = f.read()
display(HTML(f"<style>{styles}</style>"))

# Import reusable components
from .custom_components import (
    create_custom_header,
    create_custom_dropdown,
    create_custom_text,
    create_custom_button,
    create_custom_slider,
    create_custom_output
)

class PublicDatasetsWidget:
    """
    Widget to select a public dataset using an enhanced dropdown, autocomplete search,
    and additional customization options.
    """
    def __init__(self, datasets: dict):
        self.datasets = datasets
        self.selected_dataset = None
        self.s3_uri = None

        self.header = create_custom_header("Browse Public Datasets", icon="📂", level=2)
        self.search_bar = create_custom_text(
            value="",
            placeholder="🔍 Search datasets by name or tag...",
            description="Search:",
            tooltip="Type to search datasets by name or tag",
        )
        self.dataset_dropdown = create_custom_dropdown(
            options=[], 
            description="Select Dataset:",
            tooltip="Select a dataset from the suggestions"
        )
        self.dataset_dropdown.disabled = True
        self.dataset_dropdown_id = self.dataset_dropdown.model_id

        self.file_key_input = create_custom_text(
            value="",
            placeholder="📁 Enter file key (e.g., path/to/file.vcf.gz)...",
            description="File Key:",
            tooltip="Enter the file key corresponding to the selected dataset"
        )
        self.file_key_input.disabled = True

        self.region_dropdown = create_custom_dropdown(
            options=[
                "us-east-1", "us-east-2", "us-west-1", "us-west-2",
                "af-south-1", "ap-south-1", "ap-northeast-1", "ap-northeast-2",
                "eu-central-1", "eu-west-1", "eu-west-2", "eu-west-3"
            ],
            description="AWS Region:",
            tooltip="Select the AWS region where the dataset is located"
        )
        self.region_dropdown.disabled = True

        self.batch_slider = create_custom_slider(
            value=5,
            min_value=1,
            max_value=100,
            step=1,
            description="Batch Size:"
        )
        self.batch_slider.disabled = True

        self.output = create_custom_output()

        self.container = widgets.GridspecLayout(7, 1, height='auto', width='100%', grid_gap='10px')
        self.container[0, 0] = self.header
        self.container[1, 0] = self.search_bar
        self.container[2, 0] = self.dataset_dropdown
        self.container[3, 0] = self.file_key_input
        self.container[4, 0] = self.region_dropdown
        self.container[5, 0] = self.batch_slider
        self.container[6, 0] = self.output

        self.search_bar.observe(self.on_search_bar_change, names="value")
        self.dataset_dropdown.observe(self.on_dataset_selected, names="value")
        self.file_key_input.observe(self.on_file_key_change, names="value")

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

    def get_widget(self):
        return self.container

    def get_region(self):
        return self.region_dropdown.value

    def get_batch_size(self):
        return self.batch_slider.value

    def update_batch_size(self, new_value, min_value=None, max_value=None):
        if min_value is not None:
            self.batch_slider.min = min_value
        if max_value is not None:
            self.batch_slider.max = max_value

        if new_value < self.batch_slider.min:
            validated_value = self.batch_slider.min
        elif new_value > self.batch_slider.max:
            validated_value = self.batch_slider.max
        else:
            validated_value = new_value

        asyncio.ensure_future(self.animate_slider(self.batch_slider, validated_value))

    async def animate_slider(self, slider, target_value):
        current_value = slider.value
        step = 1 if target_value > current_value else -1
        while current_value != target_value:
            current_value += step
            slider.value = current_value
            await asyncio.sleep(0.01)

    def reset(self):
        self.search_bar.value = ""
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
