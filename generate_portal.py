#!/bin/env python


import sys
import sqlite3
#from IPython.core.debugger import Tracer
import subprocess
from jinja2 import Environment, FileSystemLoader
import datetime
import os






usage = "python %s <output dir> <cluster>" % sys.argv[0]


def generate_plots(projects, storage_period, cluster):

    # construct command
    cmd = '/sw/apps/R/x86_64/3.5.2/rackham/bin/Rscript {4}/generate_plots.r {0} {1} {2} {3}'.format(output_dir, storage_period, cluster, " ".join(projects), os.path.dirname(os.path.realpath(__file__)))
    print(cmd)

    # generate all plot images and return the process handle
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)



def generate_html(projects, corehour_period, user_efficiency, cluster):

    # generate html for each project
    j2_env = Environment(loader=FileSystemLoader('{0}/templates'.format(os.path.dirname(os.path.realpath(__file__)))), trim_blocks=True)
    j2_template = j2_env.get_template('project.html')
    for proj in projects:

        print proj

        title = proj
        
        jumbotron = """
          <div class="container">
            <h1>{0}</h1>
            <p>This page contains a summary of the resources used by {0} as of {1}</p>
          </div>
        """.format(proj, datetime.datetime.now().strftime('%Y-%m-%d'))

        corehour_usage = """
              <img src="img/corehour_{0}.svg" onerror="this.src='img/corehour_{0}.png'", width=262, height=262>
              <center><svg height="10" width="10"><circle cx="5" cy="5" r="4"  fill="black" /></svg> = used&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<svg height="10" width="10"><circle cx="5" cy="5" r="4"  fill="red" /></svg> = quota limit</center>
        """.format(cluster)

        efficiency = """
              <img src="img/efficiency_{0}.svg" onerror="this.src='img/efficiency_{0}.png'", width=262, height=262>
              <center><svg height="10" width="10"><circle cx="5" cy="5" r="4"  fill="red" /></svg> = this project&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<svg height="10" width="10"><circle cx="5" cy="5" r="4"  fill="grey" /></svg> = other project</center>
        """.format(cluster)

        proj_usage = """
              <img src="img/storage_proj.svg" onerror="this.src='img/storage_proj.png'", width=262, height=262>
              <center><svg height="10" width="10"><circle cx="5" cy="5" r="4"  fill="black" /></svg> = used&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<svg height="10" width="10"><circle cx="5" cy="5" r="4"  fill="red" /></svg> = quota limit</center>
        """

        nobackup_usage = """
              <img src="img/storage_nobackup.svg" onerror="this.src='img/storage_nobackup.png'", width=262, height=262>
              <center><svg height="10" width="10"><circle cx="5" cy="5" r="4"  fill="black" /></svg> = used&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<svg height="10" width="10"><circle cx="5" cy="5" r="4"  fill="red" /></svg> = quota limit</center>
        """


# <div class="tooltip">Hover over me
#   <span class="tooltiptext">Tooltip text</span>
# </div>

        # print user info if there has been any jobs run
        user_report = """
            <table class='table'>
              <thead>
                <tr>
                  <th>User</th>
                  <th>Used ch</th>
                  <th>Efficiency <a href="#" data-toggle="tooltip" title="The maximum of the CPU and memory efficiency of each job run by the user during the last {0} days are averaged, normalized after the number of corehours the job consumed."><span class="glyphicon glyphicon-info-sign" aria-hidden="true" style="color:dimgrey"></span></a></th>
                </tr>
              </thead>
              <tbody>""".format(corehour_period)

        if proj in user_efficiency:

            project_corehours = sum(value[1] for value in user_efficiency[proj].values())
            project_efficiency = sum(value[0]*value[1]/project_corehours for value in user_efficiency[proj].values())

            for user,values in user_efficiency[proj].items():
                user_report += """
                <tr>
                  <td>{0}</td>
                  <td>{1}</td>
                  <td><meter min="0" low="33" optimum="90" high="60" max="100" value="{2}"></meter> {2}%</td>
                </tr>
              """.format(user, "{0:.2f}".format(values[1]), "{0:.2f}".format(values[0]))


            user_report += """
                <tr>
                  <td><b>In total</b></td>
                  <td><b>{0}</b></td>
                  <td><meter min="0" low="33" optimum="90" high="60" max="100" value="{1}"></meter> <b>{1}%</b></td>
                </tr>
              """.format("{0:.2f}".format(project_corehours), "{0:.2f}".format(project_efficiency))

            user_report += """</tbody>
              </table>"""
            
        else:
            user_report += """</tbody>
              </table>
              No jobs were run during the last {} days
            """.format(corehour_period)









        # Tracer()()

        html = j2_template.render(title=title, jumbotron=jumbotron, corehour_usage=corehour_usage, proj_usage=proj_usage, nobackup_usage=nobackup_usage, efficiency=efficiency, user_report=user_report)

        try:
            with open("{}/{}/index.html".format(output_dir, proj), 'w') as html_file:

                html_file.write(html)

        except:
            print("Unable to open {}/{}/index.html for writing, skipping.".format(output_dir, proj))




# settings
try:
    output_dir = sys.argv[1]
    cluster = sys.argv[2]
except IndexError:
    sys.exit(usage)

corehour_period = 30
storage_period = 180
active_period = 100



# connect to the db
# db = sqlite3.connect('/proj/b2013023/statistics/general/general.sqlite')
db = sqlite3.connect('/sw/share/compstore/db_files/general/general.sqlite')
cur = db.cursor()
db_eff = sqlite3.connect('/sw/share/compstore/db_files/job_efficiency/job_efficiency.sqlite')
cur_eff = db_eff.cursor()



# get all current and recently active projects
query = "SELECT DISTINCT(proj_id) FROM projects WHERE end>={0}".format((datetime.datetime.today() - datetime.timedelta(days=active_period)).strftime('%Y%m%d'))
cur.execute(query)
results = cur.fetchall()

# repackage
projects = []
for res in results:
    projects.append(res[0])


# Tracer()()

# devel
# projects = projects[0:5]

# generate all plot images
threads = 4
processes = []
for part in [ projects[i::threads] for i in range(threads) ]:

    # process this part
    processes.append(generate_plots(part, storage_period, cluster))


# wait for all parts to finish
for process in processes:
    process.wait()

# generate_plots(projects, storage_period, cluster)



# fetch job data
query = "SELECT * FROM jobs WHERE date_finished>='{0}' AND cluster='{1}'".format((datetime.datetime.today() - datetime.timedelta(days=corehour_period)).strftime('%Y-%m-%d'), cluster)
cur_eff.execute(query)
results = cur_eff.fetchall()
# print query

# repackage
user_efficiency = {}
for res in results:

    # skip jobs that only have 1 count
    if res[10] <= 1:
        continue

    # initiate if missing
    # [max(cpu usage, mem_usage)*corecount, corecount, corehours]
    if res[2] not in user_efficiency:
        user_efficiency[res[2]] = {}
        user_efficiency[res[2]][res[3]] = [max(res[4], 100*res[7]/res[9])*res[6]*res[10], res[6]*res[10], res[6]*res[10]*5/60] 

    elif res[3] not in user_efficiency[res[2]]:
        user_efficiency[res[2]][res[3]] = [max(res[4], 100*res[7]/res[9])*res[6]*res[10], res[6]*res[10], res[6]*res[10]*5/60] 

    else:
        user_efficiency[res[2]][res[3]][0] += max(res[4], 100*res[7]/res[9])*res[6]*res[10]
        user_efficiency[res[2]][res[3]][1] += res[6]*res[10]
        user_efficiency[res[2]][res[3]][2] += res[6]*res[10]*5/60

        
        

# calculate as averages again
for proj in user_efficiency:
    for user in user_efficiency[proj]:
        user_efficiency[proj][user] = [user_efficiency[proj][user][0] / user_efficiency[proj][user][1], user_efficiency[proj][user][2]]
    
# Tracer()()

# generate all html
generate_html(projects, corehour_period, user_efficiency, cluster)


