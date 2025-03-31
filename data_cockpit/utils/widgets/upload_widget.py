import ipywidgets as widgets
from IPython.display import display, clear_output, HTML
import uuid
import io
import os
from ..s3.s3_utils import list_files  # Adjust this import as needed

# Import reusable components
from .custom_components import (
    create_custom_header,
    create_custom_dropdown,
    create_custom_text,
    create_custom_fileupload,
    create_custom_button,
    create_custom_slider,
    create_custom_output
)

# Load CSS from custom_styles.css
css_path = os.path.join(os.path.dirname(__file__), "styles", "custom_styles.css")
with open(css_path, "r") as f:
    styles = f.read()

css_injection = f"""
<script>
var head = document.getElementsByTagName('head')[0];
var style = document.createElement('style');
style.type = 'text/css';
style.innerHTML = `{styles}`;
head.appendChild(style);
</script>
"""
display(HTML(css_injection))

class UploadWidget:
    """
    Widget to upload files to S3, select a bucket, specify an optional folder, and set the batch size.
    Allows multiple file uploads. Uses consistent styling from custom_styles.css and reusable components.
    """
    def __init__(self, s3_client):
        self.s3 = s3_client
        self.last_s3_uri = None  # Stores a list of uploaded URIs

        self.header = create_custom_header("Upload to S3", icon="📤", level=2)
        self.bucket_dropdown = create_custom_dropdown([], description="Bucket:", tooltip="Select an S3 bucket")
        self.target_folder = create_custom_text(
            value="",
            placeholder="Enter target folder (optional)",
            description="S3 Folder:",
            tooltip="Optional: Enter the S3 folder for file upload"
        )
        self.file_upload = create_custom_fileupload(
            accept='',
            multiple=True,
            description="Select Files:",
            tooltip="Select one or more files to upload to S3"
        )
        self.upload_button = create_custom_button("Upload to S3", icon="cloud-upload", width="50%")
        self.upload_button.disabled = True
        self.batch_slider = create_custom_slider(value=5, min_value=1, max_value=100, step=1, description="Batch Size:")
        self.batch_slider.disabled = True
        self.output = create_custom_output()

        self.container = widgets.GridspecLayout(7, 1, height='auto', width='100%', grid_gap='10px')
        self.container[0, 0] = self.header
        self.container[1, 0] = self.bucket_dropdown
        self.container[2, 0] = self.target_folder
        self.container[3, 0] = self.file_upload
        self.container[4, 0] = self.upload_button
        self.container[5, 0] = self.batch_slider
        self.container[6, 0] = self.output

        self.file_upload.observe(self.on_file_selected, names='value')
        self.upload_button.on_click(self.on_upload_click)

    def set_buckets(self, bucket_list):
        """Set available buckets."""
        self.bucket_dropdown.options = bucket_list or ["No buckets available"]
        self.bucket_dropdown.disabled = not bucket_list

    def on_file_selected(self, change):
        """Enable upload button and slider when at least one file is selected."""
        if self.file_upload.value:
            self.upload_button.disabled = False
            self.batch_slider.disabled = False
            with self.output:
                clear_output()
                display(HTML("<p style='color: green;'><strong>File(s) selected. Ready to upload.</strong></p>"))
        else:
            self.upload_button.disabled = True
            self.batch_slider.disabled = True
            with self.output:
                clear_output()
                display(HTML("<p style='color: orange;'><strong>No file selected.</strong></p>"))

    def on_upload_click(self, b):
        """Upload files to S3 using the selected bucket and optional folder."""
        with self.output:
            clear_output()
            if not self.file_upload.value:
                display(HTML("<p style='color: red;'><strong>No file selected.</strong></p>"))
                return

            bucket = self.bucket_dropdown.value
            if not bucket or bucket.startswith("No"):
                display(HTML("<p style='color: red;'><strong>Please select a valid bucket.</strong></p>"))
                return

            s3_uris = []
            try:
                if isinstance(self.file_upload.value, dict):
                    files = list(self.file_upload.value.values())
                elif isinstance(self.file_upload.value, (list, tuple)):
                    files = list(self.file_upload.value)
                else:
                    raise TypeError("Unrecognized FileUpload format.")

                for file_info in files:
                    filename = file_info['name']
                    file_content = file_info['content']
                    unique_filename = f"{uuid.uuid4().hex}_{filename}"
                    folder = self.target_folder.value.strip()
                    if folder and not folder.endswith('/'):
                        folder += '/'
                    s3_key = f"{folder}{unique_filename}" if folder else unique_filename

                    self.s3.upload_fileobj(io.BytesIO(file_content), bucket, s3_key)
                    s3_uris.append(f"s3://{bucket}/{s3_key}")

                if s3_uris:
                    self.last_s3_uri = s3_uris
                    display(HTML(f"<p style='color: green;'><strong>Files uploaded successfully to:</strong><br>{'<br>'.join(s3_uris)}</p>"))
                else:
                    display(HTML("<p style='color: red;'><strong>No file was uploaded.</strong></p>"))
            except Exception as e:
                display(HTML(f"<p style='color: red;'><strong>Error uploading file:</strong> {e}</p>"))

    def get_batch_size(self):
        """Return the selected batch size, or None if no file was uploaded."""
        if not self.last_s3_uri:
            return None
        return self.batch_slider.value

    def get_widget(self):
        """Return the main widget container."""
        return self.container

    def update_batch_size(self, new_value, min_value=None, max_value=None):
        """
        Update the batch size slider.

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
        """Reset all widget fields."""
        self.bucket_dropdown.value = None
        self.file_upload.value.clear()
        self.target_folder.value = ""
        self.upload_button.disabled = True
        self.batch_slider.value = 5
        self.batch_slider.disabled = True
        self.last_s3_uri = None
        with self.output:
            clear_output()
            display(HTML("<p style='color: blue;'><strong>Widget reset successfully.</strong></p>"))
