import sys

from jinja2 import Environment, FileSystemLoader

# Check if the environment is passed as an argument
if len(sys.argv) != 2:
    print("Usage: python generate_workflow.py <environment>")
    print("Example: python generate_workflow.py prod")
    sys.exit(1)

environment = sys.argv[1]

# Load the template
file_loader = FileSystemLoader(".")
env = Environment(loader=file_loader)
template = env.get_template("/conf/wf_template.yml")

# Render the template with the environment variable
output = template.render(environment=environment)

# Save the rendered YAML to a file
output_file = "./conf/workflow.yml"
with open(output_file, "w") as f:
    f.write(output)

print(f"Generated {output_file}")
