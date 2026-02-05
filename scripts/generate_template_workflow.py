import sys
import subprocess
import argparse
from jinja2 import Environment, FileSystemLoader


def get_git_branch():
    """Get current git branch name, fallback to 'unknown' if not available."""
    try:
        branch = (
            subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], stderr=subprocess.DEVNULL)
            .decode("utf-8")
            .strip()
        )
        return branch
    except Exception:
        return "unknown"


def get_git_user():
    """Get git user name, fallback to 'unknown' if not available."""
    try:
        user = (
            subprocess.check_output(["git", "config", "user.name"], stderr=subprocess.DEVNULL).decode("utf-8").strip()
        )
        return user
    except Exception:
        return "unknown"


def main():
    parser = argparse.ArgumentParser(
        description="Generate Databricks workflow YAML from Jinja2 template",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python generate_template_workflow.py dev --serverless
  python generate_template_workflow.py staging --serverless --branch main --developer john --pr-number 123
        """,
    )

    parser.add_argument("environment", help="Target environment (dev, staging, prod)")
    parser.add_argument("--serverless", action="store_true", help="Use serverless workflow template")
    parser.add_argument("--branch", help="Git branch name (auto-detected if not provided)")
    parser.add_argument("--developer", help="Developer/deployer name (auto-detected if not provided)")
    parser.add_argument("--pr-number", help="Pull request number (optional)")

    args = parser.parse_args()

    # Get or auto-detect git metadata
    branch = args.branch if args.branch else get_git_branch()
    developer = args.developer if args.developer else get_git_user()
    pr_number = args.pr_number if args.pr_number else ""

    print(f"Environment: {args.environment}")
    print(f"Serverless mode: {args.serverless}")
    print(f"Git branch: {branch}")
    print(f"Developer: {developer}")
    print(f"PR number: {pr_number if pr_number else 'N/A'}")

    # Load and render template
    file_loader = FileSystemLoader(".")
    env = Environment(loader=file_loader)

    if args.serverless:
        template = env.get_template("/resources/wf_template_serverless.yml")
    else:
        template = env.get_template("/resources/wf_template.yml")

    # Render the template with all variables
    output = template.render(environment=args.environment, branch=branch, developer=developer, pr_number=pr_number)

    # Save the rendered YAML to a file
    output_file = "./resources/workflow.yml"
    with open(output_file, "w") as f:
        f.write(output)

    print(f"\nGenerated {output_file}")


if __name__ == "__main__":
    main()
