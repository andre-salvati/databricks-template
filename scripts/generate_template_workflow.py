import sys

from jinja2 import Environment, FileSystemLoader

if len(sys.argv) not in [2, 3]:
    print("Usage: python generate_workflow.py <environment> [serverless]")
    print("Example: python generate_workflow.py prod")
    print("Example: python generate_workflow.py prod serverless")
    sys.exit(1)

serverless = len(sys.argv) == 3 and sys.argv[2].lower() == "--serverless"
print(sys.argv[2].lower())
print(f"Serverless mode: {serverless}")

environment = sys.argv[1]

file_loader = FileSystemLoader(".")
env = Environment(loader=file_loader)
if serverless:
    template = env.get_template("/conf/wf_template_serverless.yml")
else:
    template = env.get_template("/conf/wf_template.yml")

# Render the template with the environment variable
output = template.render(environment=environment)

# Save the rendered YAML to a file
output_file = "./conf/workflow.yml"
with open(output_file, "w") as f:
    f.write(output)

print(f"Generated {output_file}")
