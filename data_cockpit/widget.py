import ipywidgets as widgets
from IPython.display import display, clear_output, HTML, Javascript
import boto3
import os
import json
import asyncio

# For GraphQL queries to Metaspace
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

# Existing sub-widgets
from .utils.widgets.upload_widget import UploadWidget
from .utils.widgets.s3_explorer_widget import S3ExplorerWidget
from .utils.widgets.public_datasets_widget import PublicDatasetsWidget
from .utils.widgets.metaspace_widget import MetaspaceDatasetsWidget

# Dataplug (for partitioning)
from dataplug.formats.generic.csv import CSV, partition_num_chunks as csv_partition_num_chunks
from dataplug.formats.genomics.fastq import FASTQGZip, partition_reads_batches
from dataplug.formats.genomics.fasta import FASTA, partition_chunks_strategy as fasta_partition_chunks_strategy
from dataplug.formats.geospatial.laspc import LiDARPointCloud, square_split_strategy as lidar_square_split_strategy
from dataplug.formats.genomics.vcf import VCF, partition_num_chunks as vcf_partition_num_chunks
from dataplug.formats.geospatial.copc import CloudOptimizedPointCloud, square_split_strategy as copc_square_split_strategy
from dataplug.formats.geospatial.cog import CloudOptimizedGeoTiff, grid_partition_strategy
from dataplug.cloudobject import CloudObject

# Load CSS from custom_styles.css (relative path)
css_path = os.path.join(os.path.dirname(__file__), "utils", "widgets", "styles", "custom_styles.css")
with open(css_path, "r") as f:
    styles = f.read()
display(HTML(f"<style>{styles}</style>"))

class DataLoaderWidget:
    """
    Main widget that composes the sub-widgets:
      - UploadWidget         (to upload files to S3)
      - S3ExplorerWidget     (to explore and select files in S3)
      - PublicDatasetsWidget (for public datasets with prefix + URL)
      - MetaspaceDatasetsWidget (to explore datasets from Metaspace)
    
    It also includes benchmarking capabilities to determine the ideal batch size.
    """
    def __init__(self, benchmarking_fn=None, aws_access_key_id=None, aws_secret_access_key=None, aws_region='us-east-1'):
        # Set AWS credentials in environment variables if provided
        if aws_access_key_id:
            os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
        if aws_secret_access_key:
            os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key
        os.environ["AWS_DEFAULT_REGION"] = aws_region

        # 1) Initialize S3 client
        self.s3 = boto3.client('s3', region_name='us-east-1')

        # 2) Get available buckets
        self.bucket_options = self.get_buckets()

        # 3) Instantiate sub-widgets
        public_datasets_dict = self.load_public_datasets()
        self.upload_widget = UploadWidget(self.s3)
        self.upload_widget.set_buckets(self.bucket_options)
        self.s3_explorer_widget = S3ExplorerWidget(self.s3)
        self.s3_explorer_widget.set_buckets(self.bucket_options)
        self.public_datasets_widget = PublicDatasetsWidget(public_datasets_dict)
        self.metaspace_datasets_widget = MetaspaceDatasetsWidget()

        # 4) Benchmarking toggle button
        self.benchmark_toggle = widgets.ToggleButtons(
            options=['Disabled', 'Enabled'],
            value='Disabled',
            description='Benchmarking:',
            disabled=False,
            button_style='',
            tooltips=['Disable Benchmarking', 'Enable Benchmarking'],
            layout=widgets.Layout(width='auto')
        )
        self.benchmark_toggle.add_class("benchmark-toggle")

        # 5) Benchmarking parameter widgets
        self.min_batch_size_input = widgets.BoundedIntText(
            value=1,
            min=1,
            max=1000000,
            step=1,
            description='Minimum:',
            disabled=False,
            layout=widgets.Layout(width='240px')
        )
        self.max_batch_size_input = widgets.BoundedIntText(
            value=100,
            min=1,
            max=1000000,
            step=1,
            description='Maximum:',
            disabled=False,
            layout=widgets.Layout(width='240px')
        )
        self.step_batch_size_input = widgets.BoundedIntText(
            value=10,
            min=1,
            max=1000000,
            step=1,
            description='Step:',
            disabled=False,
            layout=widgets.Layout(width='240px')
        )

        self.benchmark_params_layout = widgets.GridspecLayout(3, 2, height='auto', width='auto')
        self.benchmark_params_layout[0, 0] = widgets.HTML(value="<strong>Minimum Batch Size:</strong>")
        self.benchmark_params_layout[0, 1] = self.min_batch_size_input
        self.benchmark_params_layout[1, 0] = widgets.HTML(value="<strong>Maximum Batch Size:</strong>")
        self.benchmark_params_layout[1, 1] = self.max_batch_size_input
        self.benchmark_params_layout[2, 0] = widgets.HTML(value="<strong>Step Size:</strong>")
        self.benchmark_params_layout[2, 1] = self.step_batch_size_input

        # 6) Run Benchmarking button
        self.run_benchmark_button = widgets.Button(
            description="Run Benchmarking",
            button_style="warning",
            icon='play',
            layout=widgets.Layout(width="200px", margin="10px auto")
        )
        self.run_benchmark_button.add_class("benchmark-button")
        self.run_benchmark_output = widgets.Output(layout=widgets.Layout(width="100%"))

        self.benchmarking_box = widgets.VBox([
            self.benchmark_params_layout,
            self.run_benchmark_button,
            self.run_benchmark_output
        ])
        self.benchmarking_box.layout.display = 'none'  # Hidden by default

        # 7) Process Data button and output
        self.run_button = widgets.Button(
            description="Process Data",
            button_style="primary",
            icon='cogs',
            layout=widgets.Layout(width="200px", margin="10px auto")
        )
        self.run_button.add_class("custom-button")
        self.run_output = widgets.Output(layout=widgets.Layout(width="100%"))

        # 8) Title with icon
        title_html = """
        <div class="data-loader-title" style="text-align:center; margin-bottom:20px;">
            <h1 style="color: #2C3E50; font-weight: bold;">
                <i class="fa fa-database" style="font-size:36px; margin-right:10px;"></i>
                Data Cockpit
            </h1>
            <p class="data-loader-description" style="color: #7F8C8D; font-size:16px;">
                Efficiently manage and explore your data
            </p>
        </div>
        """
        self.title_widget = widgets.HTML(value=title_html)

        # 9) Build tabs
        self.tabs = widgets.Tab()
        self.tabs.children = [
            self.upload_widget.get_widget(),
            self.s3_explorer_widget.get_widget(),
            self.public_datasets_widget.get_widget(),
            self.metaspace_datasets_widget.get_widget()
        ]
        self.tabs.set_title(0, "Upload File")
        self.tabs.set_title(1, "S3 Explorer")
        self.tabs.set_title(2, "Public Datasets")
        self.tabs.set_title(3, "Metaspace")

        # 10) Label for Ideal Batch Size
        self.ideal_batch_size_label = widgets.HTML(value="<strong>Ideal Batch Size:</strong> Not determined.")

        # 11) Progress bar for decoding
        self.decoding_progress = widgets.IntProgress(
            value=0,
            min=0,
            max=100,
            step=1,
            description='Decoding:',
            bar_style='info',
            style={'description_width': 'initial'},
            layout=widgets.Layout(width='100%', margin='10px 0px'),
            visible=False
        )

        # 12) Main layout for DataLoaderWidget
        self.ui = widgets.VBox([
            self.title_widget,
            self.benchmark_toggle,
            self.benchmarking_box,
            self.tabs,
            self.ideal_batch_size_label,
            self.run_button,
            self.run_output,
            self.decoding_progress,
        ], layout=widgets.Layout(width='100%', padding='20px'))
        self.ui.add_class("data-loader-widget")

        # 13) Connect events
        self.run_button.on_click(self.on_run_clicked)
        self.run_benchmark_button.on_click(self.on_run_benchmark_clicked)
        self.benchmark_toggle.observe(self.on_benchmark_toggle, names='value')
        self.tabs.observe(self.on_tab_change, names='selected_index')

        # 14) Storage for partitions and decoding
        self._data_slices = {}
        self._decoded_data_slices = {}

        # 15) Storage for ideal batch sizes
        self.ideal_batch_sizes = {}

        # 16) Benchmarking function
        self.benchmarking_fn = benchmarking_fn

        # 17) Additional styles for DataLoaderWidget (optional)
        display(widgets.HTML("""
        <style>
            .data-loader-widget {
                background-color: #FFFFFF;
            }
        </style>
        """))

    def load_public_datasets(self):
        json_path = os.path.join(os.path.dirname(__file__), 'data', 'public_datasets_dict.json')
        if not os.path.exists(json_path):
            print(f"Error: The file {json_path} does not exist.")
            return {}
        try:
            with open(json_path, 'r') as f:
                datasets = json.load(f)
            return datasets
        except Exception as e:
            print(f"Error reading {json_path}: {e}")
            return {}
        
    def get_buckets(self):
        try:
            response = self.s3.list_buckets()
            return [b['Name'] for b in response.get('Buckets', [])]
        except Exception as e:
            return [f"Error: {e}"]

    def on_benchmark_toggle(self, change):
        if change['new'] == 'Enabled':
            self.benchmarking_box.layout.display = 'flex'
            self.benchmark_toggle.description = 'Benchmarking: Enabled'
        else:
            self.benchmarking_box.layout.display = 'none'
            self.benchmark_toggle.description = 'Benchmarking: Disabled'

    def on_run_benchmark_clicked(self, b):
        with self.run_benchmark_output:
            self.run_benchmark_output.clear_output()
            spinner = widgets.HTML(value="<p><i class='fa fa-spinner fa-spin'></i> Running Benchmarking...</p>")
            display(spinner)

            upload_s3_uri   = self.upload_widget.last_s3_uri
            explorer_s3_uri = self.s3_explorer_widget.selected_uri
            public_s3_uri   = self.public_datasets_widget.s3_uri
            metaspace_s3_uri= self.metaspace_datasets_widget.s3_uri

            s3_uri = None
            active_subwidget = None

            if upload_s3_uri:
                s3_uri = upload_s3_uri
                active_subwidget = self.upload_widget
            elif explorer_s3_uri:
                s3_uri = explorer_s3_uri
                active_subwidget = self.s3_explorer_widget
            elif public_s3_uri:
                s3_uri = public_s3_uri
                active_subwidget = self.public_datasets_widget
            elif metaspace_s3_uri:
                s3_uri = metaspace_s3_uri
                active_subwidget = self.metaspace_datasets_widget

            if not s3_uri:
                clear_output(wait=True)
                display(HTML("<p style='color: orange;'><strong>No dataset selected for benchmarking.</strong></p>"))
                spinner.value = "<p style='color: orange;'><strong>No dataset available for benchmarking.</strong></p>"
                return

            if s3_uri in self.ideal_batch_sizes:
                print(f"Existing Ideal Batch Size: {self.ideal_batch_sizes[s3_uri]}")
                print("Re-running Benchmarking will overwrite the existing Ideal Batch Size.")

            if not self.benchmarking_fn:
                clear_output(wait=True)
                display(HTML("<p style='color: red;'><strong>No benchmarking function provided.</strong></p>"))
                spinner.value = "<p style='color: red;'><strong>Benchmarking failed!</strong></p>"
                return

            min_bs = self.min_batch_size_input.value
            max_bs = self.max_batch_size_input.value
            step_bs = self.step_batch_size_input.value

            if min_bs > max_bs:
                clear_output(wait=True)
                display(HTML("<p style='color: red;'><strong>Error: Minimum batch size cannot be greater than maximum batch size.</strong></p>"))
                spinner.value = "<p style='color: red;'><strong>Benchmarking failed!</strong></p>"
                return

            batch_sizes = list(range(min_bs, max_bs + 1, step_bs))
            benchmark_results = {}

            print("Initiating Benchmarking...")
            for bs in batch_sizes:
                try:
                    result = self.benchmarking_fn(s3_uri, bs)
                    benchmark_results[bs] = result
                    print(f"Batch Size {bs}: {result:.4f} seconds")
                except Exception as e:
                    print(f"Batch Size {bs}: Benchmarking failed with error: {e}")

            if not benchmark_results:
                print("Benchmarking did not produce any valid results.")
                spinner.value = "<p style='color: red;'><strong>Benchmarking failed!</strong></p>"
                return

            ideal_batch_size = min(benchmark_results, key=benchmark_results.get)
            self.ideal_batch_sizes[s3_uri] = ideal_batch_size
            print(f"Ideal Batch Size determined: {ideal_batch_size}")

            self.ideal_batch_size_label.value = f"<strong>Ideal Batch Size:</strong> {ideal_batch_size}"

            if hasattr(active_subwidget, "update_batch_size"):
                active_subwidget.update_batch_size(ideal_batch_size, min_value=min_bs, max_value=max_bs)

            spinner.value = "<p style='color: green;'><strong>Benchmarking completed successfully!</strong></p>"

    def on_run_clicked(self, b):
        with self.run_output:
            self.run_output.clear_output()
            spinner = widgets.HTML(value="<p><i class='fa fa-spinner fa-spin'></i> Processing data...</p>")
            display(spinner)

            upload_s3_uri   = self.upload_widget.last_s3_uri
            explorer_s3_uri = self.s3_explorer_widget.selected_uri
            public_s3_uri   = self.public_datasets_widget.s3_uri
            metaspace_s3_uri= self.metaspace_datasets_widget.s3_uri

            s3_uri = None
            active_subwidget = None

            if upload_s3_uri:
                s3_uri = upload_s3_uri
                active_subwidget = self.upload_widget
            elif explorer_s3_uri:
                s3_uri = explorer_s3_uri
                active_subwidget = self.s3_explorer_widget
            elif public_s3_uri:
                s3_uri = public_s3_uri
                active_subwidget = self.public_datasets_widget
            elif metaspace_s3_uri:
                s3_uri = metaspace_s3_uri
                active_subwidget = self.metaspace_datasets_widget

            if not s3_uri:
                clear_output(wait=True)
                display(HTML("<p style='color: orange;'><strong>No dataset selected for processing.</strong></p>"))
                spinner.value = "<p style='color: orange;'><strong>No dataset available for processing.</strong></p>"
                return

            ideal_batch_size = self.ideal_batch_sizes.get(s3_uri, None)
            benchmarking_active = self.benchmark_toggle.value == 'Enabled'
            if benchmarking_active and not ideal_batch_size:
                clear_output(wait=True)
                display(HTML("<p style='color: orange;'><strong>Benchmarking is enabled but has not been executed. Please run Benchmarking first.</strong></p>"))
                spinner.value = "<p style='color: orange;'><strong>Benchmarking not executed.</strong></p>"
                return

            batch_size = ideal_batch_size if ideal_batch_size else active_subwidget.batch_slider.value
            print(f"Using batch size: {batch_size}")

            format_map = {
                ".csv":   (CSV, csv_partition_num_chunks),
                ".fastq": (FASTQGZip, partition_reads_batches),
                ".gz":    (FASTQGZip, partition_reads_batches),
                ".fasta": (FASTA, fasta_partition_chunks_strategy),
                ".las":   (LiDARPointCloud, lidar_square_split_strategy),
                ".laz":   (CloudOptimizedPointCloud, copc_square_split_strategy),
                ".vcf":   (VCF, vcf_partition_num_chunks),
                ".tif":   (CloudOptimizedGeoTiff, grid_partition_strategy),
            }

            self._data_slices.clear()

            ext = os.path.splitext(s3_uri)[1].lower()
            if ext not in format_map:
                clear_output(wait=True)
                display(HTML(f"<p style='color: red;'><strong>Unsupported format or missing extension: {s3_uri}</strong></p>"))
                spinner.value = "<p style='color: red;'><strong>Data processing failed!</strong></p>"
                return

            format_cls, partition_fn = format_map[ext]

            try:
                cloud_obj = CloudObject.from_s3(
                    format_cls,
                    s3_uri,
                    s3_config={"region_name": "us-east-1"}
                )
            except Exception as e:
                clear_output(wait=True)
                display(HTML(f"<p style='color: red;'><strong>Error creating CloudObject:</strong> {e}</p>"))
                spinner.value = "<p style='color: red;'><strong>Data processing failed!</strong></p>"
                return

            try:
                if ext == ".fasta":
                    chunk_size = max(1, cloud_obj.size // 4)
                    cloud_obj.preprocess(debug=True, chunk_size=chunk_size)
                else:
                    cloud_obj.preprocess(debug=True)
            except Exception as e:
                clear_output(wait=True)
                display(HTML(f"<p style='color: red;'><strong>Error during preprocessing:</strong> {e}</p>"))
                spinner.value = "<p style='color: red;'><strong>Data processing failed!</strong></p>"
                return

            try:
                if ext == ".tif":
                    data_slices = cloud_obj.partition(partition_fn, n_splits=batch_size)
                else:
                    if ext == ".gz":
                        data_slices = cloud_obj.partition(partition_fn, num_batches=batch_size)
                    else:
                        data_slices = cloud_obj.partition(partition_fn, num_chunks=batch_size)
            except Exception as e:
                clear_output(wait=True)
                display(HTML(f"<p style='color: red;'><strong>Error during partitioning:</strong> {e}</p>"))
                spinner.value = "<p style='color: red;'><strong>Data processing failed!</strong></p>"
                return

            # Save the data slices (without decoding)
            self._data_slices[s3_uri] = data_slices
            self._decoded_data_slices = data_slices

            clear_output(wait=True)
            display(HTML("<p style='color: green;'><strong>Data processing completed successfully!</strong></p>"))
            spinner.value = "<p style='color: green;'><strong>Data processing completed successfully!</strong></p>"

    def on_tab_change(self, change):
        selected_index = change['new']
        if selected_index == 0:
            s3_uri = self.upload_widget.last_s3_uri
            active_subwidget = self.upload_widget
        elif selected_index == 1:
            s3_uri = self.s3_explorer_widget.selected_uri
            active_subwidget = self.s3_explorer_widget
        elif selected_index == 2:
            s3_uri = self.public_datasets_widget.s3_uri
            active_subwidget = self.public_datasets_widget
        elif selected_index == 3:
            s3_uri = self.metaspace_datasets_widget.s3_uri
            active_subwidget = self.metaspace_datasets_widget
        else:
            s3_uri = None
            active_subwidget = None

        if s3_uri and s3_uri in self.ideal_batch_sizes:
            ideal_bs = self.ideal_batch_sizes[s3_uri]
            self.ideal_batch_size_label.value = f"<strong>Ideal Batch Size:</strong> {ideal_bs}"
            if active_subwidget and hasattr(active_subwidget, "update_batch_size"):
                active_subwidget.update_batch_size(ideal_bs)
        else:
            self.ideal_batch_size_label.value = "<strong>Ideal Batch Size:</strong> Not determined."

    def get_data_slices(self):
        return self._decoded_data_slices

    def display(self):
        display(self.ui)

    def get_dataset_prefix(self):
        """
        Returns the prefix of the selected dataset. The search order is:
          - UploadWidget (last_s3_uri)
          - S3ExplorerWidget (selected_uri)
          - PublicDatasetsWidget (s3_uri)
          - MetaspaceDatasetsWidget (s3_uri)
        If the URI follows the format 's3://bucket/path/to/dataset', the prefix is 'path/to/dataset'.
        If no dataset is selected, returns None.
        """
        s3_uri = None
        if self.upload_widget.last_s3_uri:
            s3_uri = self.upload_widget.last_s3_uri
        elif self.s3_explorer_widget.selected_uri:
            s3_uri = self.s3_explorer_widget.selected_uri
        elif self.public_datasets_widget.s3_uri:
            s3_uri = self.public_datasets_widget.s3_uri
        elif self.metaspace_datasets_widget.s3_uri:
            s3_uri = self.metaspace_datasets_widget.s3_uri

        if not s3_uri:
            return None

        if s3_uri.startswith("s3://"):
            s3_uri_no_scheme = s3_uri[5:]
            parts = s3_uri_no_scheme.split('/', 1)
            if len(parts) == 2:
                prefix = parts[1]
            else:
                prefix = ""
        else:
            prefix = s3_uri.rsplit('/', 1)[0]

        return prefix

    def get_batch_size(self):
        selected_index = self.tabs.selected_index
        if selected_index == 0:
            active_subwidget = self.upload_widget
        elif selected_index == 1:
            active_subwidget = self.s3_explorer_widget
        elif selected_index == 2:
            active_subwidget = self.public_datasets_widget
        elif selected_index == 3:
            active_subwidget = self.metaspace_datasets_widget
        else:
            return None

        if hasattr(active_subwidget, "get_batch_size"):
            return active_subwidget.get_batch_size()
        else:
            return None
