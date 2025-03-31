import ipywidgets as widgets
from IPython.display import display, clear_output, HTML
import uuid
import io
import os
from ..s3.s3_utils import list_files  # Adjust this import as needed

# Load CSS from custom_styles.css
css_path = os.path.join(os.path.dirname(__file__), "styles", "custom_styles.css")
with open(css_path, "r") as f:
    styles = f.read()
display(HTML(f"<style>{styles}</style>"))

# Import reusable components
from .custom_components import (
    create_custom_header,
    create_custom_dropdown,
    create_custom_button,
    create_custom_slider,
    create_custom_output
)

class S3ExplorerWidget:
    """
    Widget to explore S3 buckets, select files, and set the batch size.
    Uses consistent styling from custom_styles.css and reusable components.
    """
    def __init__(self, s3_client):
        self.s3 = s3_client
        self.selected_uri = None

        self.header = create_custom_header("S3 Explorer", icon="📁", level=2)
        self.bucket_dropdown = create_custom_dropdown([], description="Bucket:", tooltip="Select an S3 bucket")
        self.refresh_button = create_custom_button("Refresh", icon="refresh", width="20%")
        self.refresh_button.button_style = 'info'
        self.file_dropdown = create_custom_dropdown([], description="Files:", tooltip="Select a file from the bucket")
        self.file_dropdown.disabled = True
        self.batch_slider = create_custom_slider(value=5, min_value=1, max_value=100, step=1, description="Batch Size:")
        self.batch_slider.disabled = True
        self.output = create_custom_output()

        self.container = widgets.GridspecLayout(6, 1, height='auto', width='100%', grid_gap='10px')
        self.container[0, 0] = self.header
        self.container[1, 0] = widgets.HBox([self.bucket_dropdown, self.refresh_button], layout=widgets.Layout(width='100%'))
        self.container[2, 0] = self.file_dropdown
        self.container[3, 0] = self.batch_slider
        self.container[4, 0] = self.output

        self.refresh_button.on_click(self.on_refresh)
        self.bucket_dropdown.observe(self.on_bucket_selected, names='value')
        self.file_dropdown.observe(self.on_file_selected, names='value')

    def set_buckets(self, bucket_list):
        """Sets the available buckets."""
        self.bucket_dropdown.options = bucket_list or ["No buckets available"]

    def on_bucket_selected(self, change):
        """Triggered when a bucket is selected; updates the file list."""
        bucket = change["new"]
        with self.output:
            clear_output()
            display(HTML(f"<p style='font-size: 14px;'><strong>Selected bucket:</strong> {bucket}</p>"))
        if bucket and bucket != "No buckets available":
            self.file_dropdown.disabled = False
            self.on_refresh(self.refresh_button)
        else:
            self.file_dropdown.options = ["No bucket selected"]
            self.file_dropdown.disabled = True

    def on_refresh(self, b):
        """Refreshes the file list for the selected bucket."""
        with self.output:
            clear_output()
            bucket = self.bucket_dropdown.value
            if not bucket or bucket == "No buckets available":
                self.file_dropdown.options = ["No bucket selected"]
                self.file_dropdown.disabled = True
                display(HTML("<p style='color: red;'><strong>No bucket selected.</strong></p>"))
                return
            try:
                files = list_files(bucket, prefix="")
                if files:
                    self.file_dropdown.options = files
                    self.file_dropdown.disabled = False
                    display(HTML(f"<p style='color: green;'><strong>{len(files)} files found in bucket '{bucket}'.</strong></p>"))
                else:
                    self.file_dropdown.options = ["No files found"]
                    self.file_dropdown.disabled = True
                    display(HTML("<p style='color: orange;'><strong>No files found in the selected bucket.</strong></p>"))
            except Exception as e:
                self.file_dropdown.options = ["Error listing files"]
                self.file_dropdown.disabled = True
                display(HTML(f"<p style='color: red;'><strong>Error refreshing files:</strong> {e}</p>"))

    def on_file_selected(self, change):
        """Triggered when a file is selected; displays the selected URI and enables the slider."""
        selected = change["new"]
        if not selected or selected.endswith("(folder)"):
            self.selected_uri = None
            self.batch_slider.disabled = True
            with self.output:
                clear_output()
                display(HTML("<p style='color: orange;'><strong>No valid file selected.</strong></p>"))
            return

        bucket = self.bucket_dropdown.value
        self.selected_uri = f"s3://{bucket}/{selected}"
        with self.output:
            clear_output()
            display(HTML(f"<p style='font-size: 14px;'><strong>Selected file:</strong> {self.selected_uri}</p>"))
        self.batch_slider.disabled = False

    def get_batch_size(self):
        """Returns the batch size if a file is selected; otherwise, None."""
        if not self.selected_uri:
            return None
        return self.batch_slider.value

    def get_widget(self):
        """Returns the main widget container."""
        return self.container

    def update_batch_size(self, new_value, min_value=None, max_value=None):
        """
        Updates the batch size slider.
        
        Args:
            new_value (int): New slider value.
            min_value (int, optional): New minimum value.
            max_value (int, optional): New maximum value.
        """
        if min_value is not None:
            self.batch_slider.min = min_value
        if max_value is not None:
            self.batch_slider.max = max_value

        if new_value < self.batch_slider.min:
            self.batch_slider.value = self.batch_slider.min
        elif new_value > self.batch_slider.max:
            self.batch_slider.value = self.batch_slider.max
        else:
            self.batch_slider.value = new_value

    def reset(self):
        """Resets all widget fields."""
        self.bucket_dropdown.value = None
        self.file_dropdown.value = None
        self.file_dropdown.options = []
        self.file_dropdown.disabled = True
        self.batch_slider.value = 5
        self.batch_slider.disabled = True
        self.selected_uri = None
        with self.output:
            clear_output()
            display(HTML("<p style='color: blue;'><strong>Widget reset successfully.</strong></p>"))
