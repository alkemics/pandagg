Map config = [
    "app_root": "pandagg",
    "ut_create_database": false,
    "ut_push_config": false,
    "ut_check_coverage": true,
    "ut_python_3": true,
    "ut_on_each_pr": true
]
timestamps {
    node("master") {
        fileLoader.withGit('git@github.com:alkemics/lib-groovy-jenkins.git', 'master', 'github-read', '') {
            workflow = fileLoader.load("Workflow")
        }
    }

    workflow.launch(config)
}
