from datetime import datetime, timezone

from croniter import CroniterBadCronError, croniter


class InvalidCronExpressionError(ValueError):
    pass


def next_cron_run(expression: str, after: datetime) -> datetime:
    base = after if after.tzinfo else after.replace(tzinfo=timezone.utc)
    try:
        return croniter(expression, base.astimezone(timezone.utc)).get_next(datetime).astimezone(timezone.utc)
    except (CroniterBadCronError, ValueError, KeyError) as exc:
        raise InvalidCronExpressionError(f"invalid cron expression: {expression}") from exc
