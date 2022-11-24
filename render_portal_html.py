import jinja2
import sys
import os
import pdb
from distutils.dir_util import copy_tree







def main():
   
    # make settings global
    global root_dir, web_root, portal_root, proj_list, environment

    # get arguemnts
    root_dir = sys.argv[1]
    web_root = sys.argv[2]

    # init
    portal_root = f"{os.path.dirname(os.path.realpath(__file__))}/portal"
    environment = jinja2.Environment(loader=jinja2.FileSystemLoader(f"{portal_root}/page_templates/"))


    # create folder structure
    copy_tree(f"{portal_root}/site_template", f"{web_root}")

    # get proj list
    proj_list = get_projids()

    # render the main page
    render_main_page()

    # render all projects
    for proj in proj_list:
        render_project_page(proj)






def render_page(template_name, output_file, data=None):
    """
    Function to render html and write to an output file.
    """
    # create template
    template = environment.get_template(template_name)

    # vars

    html = template.render(data)

    # write to file
    with open(output_file, 'w') as html_file:
        html_file.write(html)





def get_projids():
    """
    Get a list of all project ids from the root dir
    """

    # get all unique projids
    projids = set()
    for csv_file in os.listdir(f"{root_dir}/csv/"): # TODO change to json dir
        projids.add(csv_file.split(".")[0])

    return list(projids)


def render_main_page():
    """
    Function to render the main page.
    """
    # create data object
    data = {'title' : "UPPMAX Project Portal",
            'projid' : "snic2022-6-147",
            'web_root' : ".",
            'proj_list' : proj_list,
           }

    render_page("main_page.html", f"{web_root}/index.html", data)





def render_project_page(proj_id):
    """
    Function to render a project's page.
    """
    # create data object
    data = {'title' : "UPPMAX Project Portal",
            'projid' : proj_id,
            'web_root' : "../../",
            'subtitle' : f' - {proj_id}',
           }
    
    os.makedirs(f"{web_root}/projects/{proj_id}", exist_ok=True)
    render_page("project_page.html", f"{web_root}/projects/{proj_id}/index.html", data)

if __name__ == "__main__":
    main()
