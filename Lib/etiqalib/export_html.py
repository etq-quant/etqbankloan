from jinja2 import Environment, BaseLoader
from io import BytesIO
import plotly
import base64

'''
export = ExportHTML('testclass.html')
export.render()
'''

class ExportHTML:
    __template_vars = {'title':'Hello World','body':'Hello World !!!'}
    __template_html = '''
        <html>
        <head lang="en">
            <meta charset="UTF-8">
            <title>{{ title }}</title>
            <style>

                table {
                  border-collapse: collapse;
                  width: 100%;
                }

                th {
                      text-align: center;
                      background-color: #ffd700;
                      color: black;
                    }
                tr:nth-child(even) {background-color: #f2f2f2;}
                tr {
                    text-align: right;
                    page-break-inside: avoid;
                }

                thead { display: table-header-group; }
                tfoot { display: table-row-group; }

                .break-before {
                    page-break-before: always;
                }


            </style>
        </head>
        <body>
            <h1>Header</h1>
             {{ body }}
            <h2 class="break-before">Next Page</h2>
        </body>
        </html>
        '''
        
    def encode_graph(self, fig):
        tmpfile = BytesIO()
        fig.savefig(tmpfile, format='png', bbox_inches='tight')
        encoded = base64.b64encode(tmpfile.getvalue()).decode('utf-8')

        fig_html = '<img src=\'data:image/png;base64,{}\'>'.format(encoded) 
        return fig_html
    
    def plotly_img_uri(self, fig, height=300, width=1200, orca_path='C:/Users/Administrator/anaconda3/orca_app/orca.exe'):
        plotly.io.orca.config.executable = orca_path
        
        img_uri = base64.b64encode(plotly.io.to_image(fig, width=width, height=height)).decode('ascii')
        return '<img style="width: {width}; height: {height}" '\
                'src="data:image/png;base64,{img_uri}" />'.format(width=width, height=height, img_uri=img_uri)

    @property
    def template_vars(self):
        return self.__template_vars
    
    @template_vars.setter
    def template_vars(self, var_dict):
        self.__template_vars = var_dict
    
    @property
    def template_html(self):
        return self.__template_html
    
    @template_html.setter
    def template_html(self, htmlString):
        self.__template_html = htmlString
    
    
    def render(self, output_file):
        template = Environment(loader=BaseLoader()).from_string(self.template_html)
        template_vars = self.template_vars
        html_out = template.render(template_vars)

        with open(output_file, "w") as fh:
            fh.write(html_out)
            
