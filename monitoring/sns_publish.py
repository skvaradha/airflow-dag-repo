import boto3

sns = boto3.client("sns", region_name="us-east-1")


def sns_msg(arn: str, msg: str, subject: str) -> sns.publish:
    return sns.publish(
        TopicArn=arn,
        Message=msg,
        Subject=subject
    )