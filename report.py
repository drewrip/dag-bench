import os
import json
import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
from jinja2 import Template
import base64
from io import BytesIO
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def find_dbt_projects(root_dir):
    """
    Recursively find all dbt projects in the root_dir.
    A dbt project is identified by a dbt_project.yml file.
    Expects manifest.json to be in the target/ directory.
    """
    projects = []
    for root, dirs, files in os.walk(root_dir):
        if 'dbt_project.yml' in files:
            manifest_path = os.path.join(root, 'target', 'manifest.json')
            if os.path.exists(manifest_path):
                projects.append({
                    'name': os.path.basename(root),
                    'path': root,
                    'manifest': manifest_path
                })
            else:
                logging.warning(f"Project found at {root} but manifest.json is missing in target/")
    return projects

def analyze_project(project):
    """
    Analyze the DAG topology of a single dbt project.
    """
    logging.info(f"Analyzing project: {project['name']}")
    try:
        with open(project['manifest'], 'r') as f:
            manifest = json.load(f)
    except Exception as e:
        logging.error(f"Failed to load manifest for {project['name']}: {e}")
        return None

    nodes = manifest.get('nodes', {})
    sources = manifest.get('sources', {})
    
    # Combined nodes (for the DAG)
    all_nodes = {**nodes, **sources}
    
    G = nx.DiGraph()
    
    # Resource types we care about for the topology
    VISUAL_RESOURCES = ['model', 'seed', 'source', 'snapshot']
    METRIC_RESOURCES = ['model', 'seed', 'snapshot']
    
    # Add nodes
    for node_id, node in all_nodes.items():
        res_type = node.get('resource_type')
        if res_type in VISUAL_RESOURCES:
            G.add_node(node_id, name=node.get('name'), type=res_type)
            
    # Add edges
    for node_id, node in all_nodes.items():
        if node.get('resource_type') in VISUAL_RESOURCES:
            depends_on = node.get('depends_on', {}).get('nodes', [])
            for dep in depends_on:
                if dep in G.nodes:
                    G.add_edge(dep, node_id)
                    
    # Metrics - calculated on a subgraph excluding sources
    G_metrics = G.subgraph([n for n, d in G.nodes(data=True) if d['type'] in METRIC_RESOURCES])
    
    num_nodes = G_metrics.number_of_nodes()
    num_edges = G_metrics.number_of_edges()
    
    if num_nodes > 0:
        avg_out_degree = sum(dict(G_metrics.out_degree()).values()) / num_nodes
        # Depth: longest path in a DAG
        try:
            # dag_longest_path_length returns number of edges in longest path
            depth = nx.dag_longest_path_length(G_metrics)
        except nx.NetworkXUnfeasible:
            # Not a DAG (unlikely in dbt unless there are cycles)
            logging.warning(f"Project {project['name']} is not a DAG!")
            depth = -1
    else:
        avg_out_degree = 0
        depth = 0
        
    # Node types distribution (includes sources for completeness)
    types = [d['type'] for n, d in G.nodes(data=True)]
    type_counts = pd.Series(types).value_counts().to_dict()
    
    # Prepare data for interactive visualization
    viz_nodes = []
    viz_edges = []
    
    # Simple layering for initial position
    try:
        layers = {}
        for node in nx.topological_sort(G):
            level = 0
            for pred in G.predecessors(node):
                level = max(level, layers[pred] + 1)
            layers[node] = level
    except:
        layers = {n: 0 for n in G.nodes}

    for n, d in G.nodes(data=True):
        color = '#ff9800' if d['type'] == 'source' else '#2196f3'
        viz_nodes.append({
            'id': n,
            'label': d['name'],
            'title': f"Type: {d['type']}<br>ID: {n}",
            'color': color,
            'level': layers.get(n, 0)
        })
        
    for u, v in G.edges():
        viz_edges.append({'from': u, 'to': v})

    # Count source nodes (source type in visualization graph)
    source_nodes = [n for n, d in G.nodes(data=True) if d['type'] == 'source']
    source_node_count = len(source_nodes)
    
    # Count sink nodes (nodes with no outgoing edges in METRIC_RESOURCES subgraph)
    sink_nodes = []
    for node in G_metrics.nodes():
        if G_metrics.out_degree(node) == 0:
            sink_nodes.append(node)
    sink_node_count = len(sink_nodes)
    
    return {
        'id': project['path'].replace('/', '_').replace('.', '_'),
        'name': project['name'],
        'path': project['path'],
        'num_nodes': num_nodes,
        'num_edges': num_edges,
        'avg_out_degree': round(avg_out_degree, 3),
        'depth': depth,
        'source_node_count': source_node_count,
        'sink_node_count': sink_node_count,
        'type_counts': type_counts,
        'viz_nodes': viz_nodes,
        'viz_edges': viz_edges
    }

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>dbt Projects Topology Report</title>
    <script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; color: #333; max-width: 1200px; margin: 0 auto; padding: 20px; background-color: #f4f7f6; }
        header { background-color: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 30px; text-align: center; }
        .summary-table { width: 100%; border-collapse: collapse; margin-bottom: 40px; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
        .summary-table th, .summary-table td { padding: 12px 15px; text-align: left; border-bottom: 1px solid #eee; }
        .summary-table th { background-color: #34495e; color: white; }
        .summary-table tr:hover { background-color: #f1f1f1; }
        .project-card { background: white; border-radius: 8px; padding: 25px; margin-bottom: 30px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
        .project-header { display: flex; justify-content: space-between; align-items: baseline; border-bottom: 2px solid #3498db; margin-bottom: 20px; padding-bottom: 10px; }
        .project-title { color: #2980b9; margin: 0; }
        .project-path { color: #7f8c8d; font-size: 0.9em; font-family: monospace; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 20px; }
        .stat-item { background: #ecf0f1; padding: 15px; border-radius: 6px; text-align: center; }
        .stat-value { font-size: 1.5em; font-weight: bold; color: #2c3e50; display: block; }
        .stat-label { font-size: 0.8em; color: #7f8c8d; text-transform: uppercase; letter-spacing: 1px; }
        .visualization { height: 500px; border: 1px solid #ddd; border-radius: 4px; margin-top: 20px; background: #fff; }
        .type-tag { display: inline-block; background: #3498db; color: white; padding: 2px 8px; border-radius: 12px; font-size: 0.8em; margin-right: 5px; margin-bottom: 5px; }
        footer { text-align: center; margin-top: 50px; color: #7f8c8d; font-size: 0.8em; }
    </style>
</head>
<body>
    <header>
        <h1>dbt Projects Topology Report</h1>
        <p>Analysis of {{ projects|length }} dbt projects</p>
    </header>

    <section>
        <h2>Global Summary</h2>
        <table class="summary-table">
            <thead>
                <tr>
                    <th>Project Name</th>
                    <th>Nodes</th>
                    <th>Edges</th>
                    <th>Depth</th>
                    <th>Avg Out-Degree</th>
                    <th>Source Nodes</th>
                    <th>Sink Nodes</th>
                </tr>
            </thead>
            <tbody>
                {% for p in projects %}
                <tr>
                    <td><strong>{{ p.name }}</strong></td>
                    <td>{{ p.num_nodes }}</td>
                    <td>{{ p.num_edges }}</td>
                    <td>{{ p.depth }}</td>
                    <td>{{ p.avg_out_degree }}</td>
                    <td>{{ p.source_node_count }}</td>
                    <td>{{ p.sink_node_count }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </section>

    <section>
        <h2>Project Details</h2>
        {% for p in projects %}
        <div class="project-card">
            <div class="project-header">
                <h3 class="project-title">{{ p.name }}</h3>
                <span class="project-path">{{ p.path }}</span>
            </div>
            
            <div class="stats-grid">
                <div class="stat-item">
                    <span class="stat-value">{{ p.num_nodes }}</span>
                    <span class="stat-label">Nodes</span>
                </div>
                <div class="stat-item">
                    <span class="stat-value">{{ p.num_edges }}</span>
                    <span class="stat-label">Edges</span>
                </div>
                <div class="stat-item">
                    <span class="stat-value">{{ p.depth }}</span>
                    <span class="stat-label">Max Depth</span>
                </div>
                <div class="stat-item">
                    <span class="stat-value">{{ p.avg_out_degree }}</span>
                    <span class="stat-label">Avg Out-Degree</span>
                </div>
                <div class="stat-item">
                    <span class="stat-value">{{ p.source_node_count }}</span>
                    <span class="stat-label">Source Nodes</span>
                </div>
                <div class="stat-item">
                    <span class="stat-value">{{ p.sink_node_count }}</span>
                    <span class="stat-label">Sink Nodes</span>
                </div>
            </div>

            <div>
                <strong>Resource Types:</strong><br>
                {% for type, count in p.type_counts.items() %}
                <span class="type-tag">{{ type }}: {{ count }}</span>
                {% endfor %}
            </div>

            <div id="viz_{{ p.id }}" class="visualization"></div>
            <script type="text/javascript">
                (function() {
                    const nodes = new vis.DataSet({{ p.viz_nodes | tojson }});
                    const edges = new vis.DataSet({{ p.viz_edges | tojson }});
                    const container = document.getElementById('viz_{{ p.id }}');
                    const data = { nodes: nodes, edges: edges };
                    const options = {
                        layout: {
                            hierarchical: {
                                direction: 'LR',
                                sortMethod: 'directed',
                                levelSeparation: 150,
                                nodeSpacing: 100
                            }
                        },
                        edges: {
                            arrows: { to: { enabled: true, scaleFactor: 1, type: 'arrow' } },
                            color: '#848484',
                            width: 0.5
                        },
                        nodes: {
                            shape: 'dot',
                            size: 10,
                            font: { size: 12, color: '#333' }
                        },
                        physics: false
                    };
                    new vis.Network(container, data, options);
                })();
            </script>
        </div>
        {% endfor %}
    </section>

    <footer>
        <p>Generated on {{ date }} | dbt Topology Analyzer</p>
    </footer>
</body>
</html>
"""

def disambiguate_names(projects):
    """
    If multiple projects have the same name, prepend parent directory names
    until they are unique.
    """
    from collections import defaultdict
    
    # Group by current name
    name_groups = defaultdict(list)
    for p in projects:
        name_groups[p['name']].append(p)
    
    needs_work = True
    while needs_work:
        needs_work = False
        new_groups = defaultdict(list)
        
        for name, group in name_groups.items():
            if len(group) > 1:
                # Disambiguate this group
                for p in group:
                    path_parts = p['path'].strip(os.sep).split(os.sep)
                    # Find how many parts of the name we are already using
                    # By default 'name' is just the last part.
                    # We want to prepend the next part.
                    current_name_parts = p['name'].split(' / ')
                    if len(path_parts) > len(current_name_parts):
                        parent_idx = -(len(current_name_parts) + 1)
                        p['name'] = f"{path_parts[parent_idx]} / {p['name']}"
                        needs_work = True
                    new_groups[p['name']].append(p)
            else:
                new_groups[name].append(group[0])
        
        name_groups = new_groups
        
    return [p for group in name_groups.values() for p in group]

def main():
    import datetime
    root_dir = 'projects'
    project_configs = find_dbt_projects(root_dir)
    
    if not project_configs:
        print("No dbt projects with manifest.json found in projects/ directory.")
        return

    # Disambiguate names before processing
    project_configs = disambiguate_names(project_configs)

    summaries = []
    for p in project_configs:
        res = analyze_project(p)
        if res:
            summaries.append(res)
    
    # Sort summaries by name
    summaries.sort(key=lambda x: x['name'])
    
    template = Template(HTML_TEMPLATE)
    html_output = template.render(
        projects=summaries,
        date=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    
    with open('report.html', 'w') as f:
        f.write(html_output)
    
    print(f"\nSuccess! Report generated for {len(summaries)} projects.")
    print(f"Output: {os.path.abspath('report.html')}")

if __name__ == "__main__":
    main()
