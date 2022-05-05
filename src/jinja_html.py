import jinja2
import base64


def get_template(template):
    #     template = "template.html"
    template_loader = jinja2.FileSystemLoader(searchpath="./")
    template_env = jinja2.Environment(loader=template_loader)
    return template_env.get_template(template)


def render_html_text(data, input_template_filename):
    template = get_template(template=input_template_filename)
    output_text = template.render(**data)
    return output_text


def render_html(data, output_filename, input_template_filename):
    OUTPUT_HTML = f"./{output_filename}"
    template = get_template(template=input_template_filename)
    output_text = template.render(**data)
    with open(OUTPUT_HTML, "w") as ofile:
        ofile.write(output_text)


def encode_graph(fig):
    val = fig.to_image(format="png")
    encoded = base64.b64encode(val).decode("utf-8")
    fig_html = "<img src='data:image/png;base64,{}'>".format(encoded)

    return fig_html
