resource "aws_ecr_repository" "docker-create-metrics-data-batch" {
  name = "docker-create-metrics-data-batch"
  tags = merge(
    local.common_tags,
    { DockerHub : "dwpdigital/docker-create-metrics-data-batch" }
  )
}

resource "aws_ecr_repository_policy" "docker-create-metrics-data-batch" {
  repository = aws_ecr_repository.docker-create-metrics-data-batch.name
  policy     = data.terraform_remote_state.management.outputs.ecr_iam_policy_document
}

output "ecr_example_url" {
  value = aws_ecr_repository.docker-create-metrics-data-batch.repository_url
}
