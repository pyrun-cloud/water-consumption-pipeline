"""
custom_components.py

This module defines reusable components for ipywidgets with consistent styling.
Ensure that your custom_styles.css file defines these CSS classes:
    - custom-button
    - custom-slider
    - custom-dropdown
    - custom-text-input
    - custom-fileupload
    - custom-output
    - header-text
"""

import ipywidgets as widgets
from IPython.display import HTML

def create_custom_header(text: str, icon: str = "", level: int = 2) -> widgets.HTML:
    # The header text is wrapped in a div with the "header-text" class.
    html_value = f"""
    <div class="header-text" style="text-align: center; margin-bottom: 20px;">
        <h{level}>{icon} {text}</h{level}>
    </div>
    """
    return widgets.HTML(value=html_value, layout=widgets.Layout(margin="10px 0px"))

def create_custom_button(description: str, icon: str, width: str = "50%", button_style: str = 'success') -> widgets.Button:
    button = widgets.Button(
        description=description,
        button_style=button_style,
        icon=icon,
        layout=widgets.Layout(width=width, margin="10px auto")
    )
    button.add_class("custom-button")
    return button

def create_custom_slider(value: int = 5, min_value: int = 1, max_value: int = 100, step: int = 1, description: str = "Batch Size:") -> widgets.IntSlider:
    slider = widgets.IntSlider(
        value=value,
        min=min_value,
        max=max_value,
        step=step,
        description=description,
        continuous_update=False,
        orientation='horizontal',
        readout=True,
        readout_format='d',
        layout=widgets.Layout(width="100%", margin="10px 0px")
    )
    slider.add_class("custom-slider")
    return slider

def create_custom_dropdown(options, description: str = "", width: str = "100%", tooltip: str = "") -> widgets.Dropdown:
    dropdown = widgets.Dropdown(
        options=options,
        description=description,
        layout=widgets.Layout(width=width, margin="10px 0px"),
        style={'description_width': 'initial'},
        tooltip=tooltip
    )
    dropdown.add_class("custom-dropdown")
    return dropdown

def create_custom_text(value: str = "", placeholder: str = "", description: str = "", width: str = "100%", tooltip: str = "") -> widgets.Text:
    text = widgets.Text(
        value=value,
        placeholder=placeholder,
        description=description,
        layout=widgets.Layout(width=width, margin="10px 0px"),
        style={'description_width': 'initial'},
        tooltip=tooltip
    )
    text.add_class("custom-text-input")
    return text

def create_custom_fileupload(accept: str = "", multiple: bool = True, description: str = "Select Files:", width: str = "100%", tooltip: str = "") -> widgets.FileUpload:
    fileupload = widgets.FileUpload(
        accept=accept,
        multiple=multiple,
        description=description,
        layout=widgets.Layout(width=width, margin="10px 0px"),
        tooltip=tooltip
    )
    fileupload.add_class("custom-fileupload")
    return fileupload

def create_custom_output() -> widgets.Output:
    output = widgets.Output(
        layout=widgets.Layout(
            border='1px solid #ccc',
            padding='10px',
            margin="10px 0px",
            width='100%',
            background_color='#FAFAFA',
            border_radius='5px'
        )
    )
    output.add_class("custom-output")
    return output
