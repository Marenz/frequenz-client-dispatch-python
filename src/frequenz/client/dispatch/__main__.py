# License: All rights reserved
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""CLI and interactive client for the dispatch service."""

import asyncio
import os
from datetime import datetime, timezone
from pprint import pformat
from typing import Any, List

import asyncclick as click
import grpc
from frequenz.channels import Receiver
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import NestedCompleter
from prompt_toolkit.history import FileHistory
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.shortcuts import CompleteStyle

from frequenz.client.common.microgrid import MicrogridId

from ._cli_types import (
    FuzzyDateTime,
    FuzzyIntRange,
    FuzzyTimeDelta,
    JsonDictParamType,
    TargetComponentParamType,
)
from ._client import DispatchApiClient
from .recurrence import EndCriteria, Frequency, RecurrenceRule, Weekday
from .types import Dispatch, DispatchEvent, DispatchId


def format_datetime(dt: datetime | None) -> str:
    """Format datetime object to a readable string, or return 'N/A' if None."""
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z") if dt else "N/A"


def format_recurrence(recurrence: RecurrenceRule) -> str:
    """Format the recurrence rule, omitting empty or unspecified fields."""
    parts: List[str] = []
    # Since frequency is not UNSPECIFIED here (we check before calling this function)
    parts.append(f"Frequency: {recurrence.frequency.name}")
    if recurrence.interval:
        parts.append(f"Interval: {recurrence.interval}")
    if recurrence.end_criteria:
        parts.append(f"End Criteria: {recurrence.end_criteria}")
    # Include only non-empty lists
    if recurrence.byminutes:
        parts.append(f"Minutes: {', '.join(map(str, recurrence.byminutes))}")
    if recurrence.byhours:
        parts.append(f"Hours: {', '.join(map(str, recurrence.byhours))}")
    if recurrence.byweekdays:
        weekdays = ", ".join(day.name for day in recurrence.byweekdays)
        parts.append(f"Weekdays: {weekdays}")
    if recurrence.bymonthdays:
        parts.append(f"Month Days: {', '.join(map(str, recurrence.bymonthdays))}")
    if recurrence.bymonths:
        months = ", ".join(map(month_name, recurrence.bymonths))
        parts.append(f"Months: {months}")
    return "\n".join(parts)


def month_name(month: int) -> str:
    """Return the name of the month."""
    return datetime(2000, month, 1).strftime("%B")


# pylint: disable=too-many-statements, too-many-locals
def print_dispatch(dispatch: Dispatch) -> None:
    """Print the dispatch details in a nicely formatted way with colors."""
    # Determine the status and color
    status: str = "running" if dispatch.started else "not running"
    status_color: str = "green" if dispatch.started else "red"
    status_str: str = click.style(status, fg=status_color, bold=True)

    # Format the next start time with color
    next_start_time_str: str = format_datetime(dispatch.next_run)
    next_start_time_colored: str = click.style(next_start_time_str, fg="cyan")

    start_in_timedelta = (
        dispatch.next_run - datetime.now(timezone.utc) if dispatch.next_run else None
    )
    start_in_timedelta_str = str(start_in_timedelta) if start_in_timedelta else "N/A"
    start_in_timedelta_colored = click.style(start_in_timedelta_str, fg="yellow")

    # Format the target
    if dispatch.target:
        if len(dispatch.target) == 1:
            (first_element,) = dispatch.target
            target_str: str = str(first_element)
        else:
            target_str = ", ".join(str(s) for s in dispatch.target)
    else:
        target_str = "None"

    # Prepare the dispatch details
    lines: List[str] = []
    # Define the keys for alignment
    keys = [
        "ID",
        "Type",
        "Start Time",
        "Duration",
        "Target",
        "Active",
        "Dry Run",
        "Payload",
        "Recurrence",
        "Create Time",
        "Update Time",
    ]
    max_key_length: int = max(len(k) for k in keys)

    # Helper function to format each line
    def format_line(key: str, value: str, color: str = "cyan") -> str:
        key_str = click.style(f"{key}:", fg=color)
        val_color = "white"

        if value in ("None", "False"):
            val_color = "red"
        elif value == "True":
            val_color = "green"

        val_str: str = click.style(value, fg=val_color)
        return f"{key_str:<{max_key_length + 2}} {val_str}"

    lines.append(click.style("Dispatch Details:", bold=True, underline=True))
    lines.append(format_line("ID", str(dispatch.id)))
    lines.append(format_line("Type", str(dispatch.type)))
    lines.append(format_line("Start Time", format_datetime(dispatch.start_time)))
    if dispatch.duration is not None:
        lines.append(format_line("Duration", str(dispatch.duration)))
    else:
        lines.append(format_line("Duration", "Indefinite"))
    lines.append(format_line("Target", target_str))
    lines.append(format_line("Active", str(dispatch.active)))
    lines.append(format_line("Dry Run", str(dispatch.dry_run)))
    if dispatch.payload:
        lines.append(format_line("Payload", str(dispatch.payload)))
    # Only include recurrence if frequency is not UNSPECIFIED
    if dispatch.recurrence and dispatch.recurrence.frequency != Frequency.UNSPECIFIED:
        recurrence_str = format_recurrence(dispatch.recurrence)
        # Indent recurrence details for better readability
        indented_recurrence = "\n    " + recurrence_str.replace("\n", "\n    ")
        lines.append(format_line("Recurrence", indented_recurrence, "green"))
    else:
        lines.append(format_line("Recurrence", "None"))
    lines.append(format_line("Create Time", format_datetime(dispatch.create_time)))
    lines.append(format_line("Update Time", format_datetime(dispatch.update_time)))
    lines.append(format_line("End    Time", format_datetime(dispatch.end_time)))

    # Combine all lines
    dispatch_info: str = "\n".join(lines)

    # Output the formatted dispatch details
    click.echo(f"{dispatch_info}\n")
    click.echo(f"Dispatch is currently {status_str}")
    click.echo(
        f"Next start in: {start_in_timedelta_colored} ({next_start_time_colored})\n"
    )


# Click command groups
@click.group(invoke_without_command=True)
@click.option(
    "--url",
    help="Dispatch API Url",
    envvar="DISPATCH_API_URL",
    show_envvar=True,
)
@click.option(
    "--key",
    help="API key for authentication",
    envvar="DISPATCH_API_KEY",
    show_envvar=True,
    required=True,
)
@click.option(
    "--raw",
    is_flag=True,
    help="Print output raw instead of formatted and colored",
    required=False,
    default=False,
)
@click.pass_context
async def cli(ctx: click.Context, url: str, key: str, raw: bool) -> None:
    """Dispatch Service CLI."""
    if ctx.obj is None:
        ctx.obj = {}

    click.echo(f"Using API URL: {url}", err=True)

    ctx.obj["client"] = DispatchApiClient(
        server_url=url,
        key=key,
        connect=True,
    )

    ctx.obj["params"] = {
        "url": url,
        "key": key,
    }

    ctx.obj["raw"] = raw

    # Check if a subcommand was given
    if ctx.invoked_subcommand is None:
        await interactive_mode(url, key)


@cli.command("list")
@click.pass_context
@click.argument("microgrid-id", required=True, type=int)
@click.option("--target", "-t", type=TargetComponentParamType(), multiple=True)
@click.option("--start-from", type=FuzzyDateTime())
@click.option("--start-to", type=FuzzyDateTime())
@click.option("--end-from", type=FuzzyDateTime())
@click.option("--end-to", type=FuzzyDateTime())
@click.option("--active", type=bool)
@click.option("--dry-run", type=bool)
@click.option("--page-size", type=int)
@click.option("--running", type=bool)
@click.option("--type", "-T", type=str)
async def list_(ctx: click.Context, /, **filters: Any) -> None:
    """List dispatches.

    Lists all dispatches for MICROGRID_ID that match the given filters.

    The target option can be given multiple times.
    """
    filter_running: bool = filters.pop("running", False)
    filter_type: str | None = filters.pop("type", None)

    if "target" in filters:
        target = filters.pop("target")
        # Name of the parameter in client.list()
        filters["target_components"] = target

    num_dispatches = 0
    num_filtered = 0
    async for page in ctx.obj["client"].list(**filters):
        for dispatch in page:
            if filter_running and not dispatch.started:
                num_filtered += 1
                continue

            if filter_type and dispatch.type != filter_type:
                num_filtered += 1
                continue

            if ctx.obj["raw"]:
                click.echo(pformat(dispatch, compact=True))
            else:
                print_dispatch(dispatch)
            num_dispatches += 1

    click.echo(f"{num_dispatches} dispatches, {num_filtered} filtered out.")


@cli.command("stream")
@click.pass_context
@click.argument("microgrid-id", required=True, type=int)
async def stream(ctx: click.Context, microgrid_id: MicrogridId) -> None:
    """Stream dispatches."""
    event_stream: Receiver[DispatchEvent] = ctx.obj["client"].stream(
        microgrid_id=microgrid_id
    )
    async for message in event_stream:
        if ctx.obj["raw"]:
            click.echo(pformat(message, compact=True))
        else:
            print(f"Dispatch {message.event.name.lower()}")
            print_dispatch(message.dispatch)


def parse_recurrence(kwargs: dict[str, Any]) -> RecurrenceRule | None:
    """Parse recurrence rule from kwargs."""
    interval = kwargs.pop("interval", 0)
    by_minute = list(kwargs.pop("by_minute", []))
    by_hour = list(kwargs.pop("by_hour", []))
    by_weekday = [Weekday[weekday.upper()] for weekday in kwargs.pop("by_weekday", [])]
    by_monthday = list(kwargs.pop("by_monthday", []))

    if not kwargs.get("frequency"):
        return None

    return RecurrenceRule(
        frequency=Frequency[kwargs.pop("frequency")],
        interval=interval,
        end_criteria=(
            EndCriteria(
                count=kwargs.pop("count", None),
                until=kwargs.pop("until", None),
            )
            if kwargs.get("count") or kwargs.get("until")
            else None
        ),
        byminutes=by_minute,
        byhours=by_hour,
        byweekdays=by_weekday,
        bymonthdays=by_monthday,
    )


def validate_reccurance(ctx: click.Context, param: click.Parameter, value: Any) -> Any:
    """Validate recurrence rule."""
    if param.name == "frequency":
        return value

    count_param = param.name == "count" and value
    until_param = param.name == "until" and value

    if (
        count_param
        and ctx.params.get("until") is not None
        or until_param
        and ctx.params.get("count") is not None
    ):
        raise click.BadArgumentUsage("Only count or until can be set, not both.")

    if value and ctx.params.get("frequency") is None:
        raise click.BadArgumentUsage(f"Frequency must be set to use {param.name}.")

    return value


recurrence_options: list[click.Parameter] = [
    click.Option(
        ["--frequency", "-f"],
        type=click.Choice(
            [
                frequency.name
                for frequency in Frequency
                if frequency != Frequency.UNSPECIFIED
            ],
            case_sensitive=False,
        ),
        help="Frequency of the dispatch",
        callback=validate_reccurance,
        is_eager=True,
    ),
    click.Option(
        ["--interval"],
        type=int,
        help="Interval of the dispatch, based on frequency",
        default=1,
    ),
    click.Option(
        ["--count"],
        type=int,
        help="Number of occurrences of the dispatch",
        callback=validate_reccurance,
    ),
    click.Option(
        ["--until"],
        type=FuzzyDateTime(),
        help="End time of the dispatch",
        callback=validate_reccurance,
    ),
    click.Option(
        ["--by-minute"],
        type=int,
        help="Minute of the hour for the dispatch",
        multiple=True,
        callback=validate_reccurance,
    ),
    click.Option(
        ["--by-hour"],
        type=int,
        help="Hour of the day for the dispatch",
        multiple=True,
        callback=validate_reccurance,
    ),
    click.Option(
        ["--by-weekday"],
        type=click.Choice(
            [weekday.name for weekday in Weekday if weekday != Weekday.UNSPECIFIED],
            case_sensitive=False,
        ),
        help="Day of the week for the dispatch",
        multiple=True,
        callback=validate_reccurance,
    ),
    click.Option(
        ["--by-monthday"],
        type=int,
        help="Day of the month for the dispatch",
        multiple=True,
        callback=validate_reccurance,
    ),
]


@cli.command()
@click.argument("microgrid-id", required=True, type=int)
@click.argument(
    "type",
    required=True,
    type=str,
)
@click.argument("target", required=True, type=TargetComponentParamType())
@click.argument("start-time", required=True, type=FuzzyDateTime())
@click.argument("duration", required=False, type=FuzzyTimeDelta())
@click.option("--active", "-a", type=bool, default=True)
@click.option("--dry-run", "-d", type=bool, default=False)
@click.option(
    "--payload", "-p", type=JsonDictParamType(), help="JSON payload for the dispatch"
)
@click.pass_context
async def create(
    ctx: click.Context,
    /,
    **kwargs: Any,
) -> None:
    """Create a dispatch.

    Creates a new dispatch for MICROGRID_ID of type TYPE running for DURATION seconds
    starting at START_TIME.

    TARGET is a comma-separated list of either component categories or component IDs.
    Possible component categories: "BATTERY, GRID, METER, INVERTER, EV_CHARGER, CHP".
    """
    # Remove keys with `None` value
    kwargs = {k: v for k, v in kwargs.items() if v is not None}

    # Required for client.create
    kwargs.setdefault("duration", None)

    dispatch = await ctx.obj["client"].create(
        recurrence=parse_recurrence(kwargs),
        **kwargs,
    )
    if ctx.obj["raw"]:
        click.echo(pformat(dispatch, compact=True))
    else:
        print_dispatch(dispatch)
    click.echo("Dispatch created.")


@cli.command()
@click.argument("microgrid-id", required=True, type=int)
@click.argument("dispatch_id", type=int)
@click.option("--start-time", type=FuzzyDateTime())
@click.option("--duration", type=FuzzyTimeDelta())
@click.option("--no-duration", is_flag=True)
@click.option("--target", type=TargetComponentParamType())
@click.option("--active", type=bool)
@click.option(
    "--payload", "-p", type=JsonDictParamType(), help="JSON payload for the dispatch"
)
@click.pass_context
async def update(
    ctx: click.Context,
    /,
    microgrid_id: MicrogridId,
    dispatch_id: DispatchId,
    **new_fields: dict[str, Any],
) -> None:
    """Update a dispatch."""

    def skip_field(value: Any) -> bool:
        return value is None or value == [] or value == ()

    # Every field is initialized with `None`, repeatable ones with `()` and `[]`.
    # We want to filter these out to not send them to the server.
    new_fields = {k: v for k, v in new_fields.items() if not skip_field(v)}
    recurrence = parse_recurrence(new_fields)

    # Convert recurrence fields to nested fields as expected by update()
    for key in recurrence.__dict__ if recurrence else []:
        val = getattr(recurrence, key)
        if val is not None and val != []:
            new_fields[f"recurrence.{key}"] = val

    if len(new_fields) == 0:
        raise click.BadArgumentUsage("At least one field must be given to update.")

    if new_fields.get("no_duration"):
        if new_fields.get("duration"):
            raise click.BadArgumentUsage("Cannot set both no_duration and duration.")
        new_fields["duration"] = None  # type: ignore

    new_fields.pop("no_duration")

    try:
        changed_dispatch = await ctx.obj["client"].update(
            microgrid_id=microgrid_id, dispatch_id=dispatch_id, new_fields=new_fields
        )
        click.echo("Dispatch updated:")
        if ctx.obj["raw"]:
            click.echo(pformat(changed_dispatch, compact=True))
        else:
            print_dispatch(changed_dispatch)
    except grpc.RpcError as e:
        raise click.ClickException(f"Update failed: {e}")


@cli.command()
@click.argument("microgrid-id", required=True, type=int)
@click.argument("dispatch_ids", type=int, nargs=-1)  # Allow multiple IDs
@click.pass_context
async def get(
    ctx: click.Context, microgrid_id: MicrogridId, dispatch_ids: List[int]
) -> None:
    """Get one or multiple dispatches."""
    num_failed = 0

    for dispatch_id in dispatch_ids:
        try:
            dispatch = await ctx.obj["client"].get(
                microgrid_id=microgrid_id,
                dispatch_id=DispatchId(dispatch_id),
            )
            if ctx.obj["raw"]:
                click.echo(pformat(dispatch, compact=True))
            else:
                print_dispatch(dispatch)
        except grpc.RpcError as e:
            click.echo(f"Error getting dispatch {dispatch_id}: {e}", err=True)
            num_failed += 1

    if num_failed == len(dispatch_ids):
        raise click.ClickException("All gets failed.")
    if num_failed > 0:
        raise click.ClickException("Some gets failed.")


@cli.command()
@click.pass_obj
async def repl(
    obj: dict[str, Any],
) -> None:
    """Start an interactive interface."""
    click.echo(f"Parameters: {obj}")
    await interactive_mode(obj["params"]["url"], obj["params"]["key"])


@cli.command()
@click.argument("microgrid-id", required=True, type=int)
@click.argument("dispatch_ids", type=FuzzyIntRange(), nargs=-1)  # Allow multiple IDs
@click.pass_context
async def delete(
    ctx: click.Context, microgrid_id: MicrogridId, dispatch_ids: list[list[int]]
) -> None:
    """Delete multiple dispatches.

    Possible formats: "1", "1,2,3", "1-3", "1..3"
    """
    # Flatten the list of lists
    flat_ids = [dispatch_id for sublist in dispatch_ids for dispatch_id in sublist]
    failed_ids = []
    success_ids = []

    for dispatch_id in flat_ids:
        try:
            await ctx.obj["client"].delete(
                microgrid_id=microgrid_id, dispatch_id=dispatch_id
            )
            success_ids.append(dispatch_id)
        except grpc.RpcError as e:
            click.echo(f"Error deleting dispatch {dispatch_id}: {e}", err=True)
            failed_ids.append(dispatch_id)

    if success_ids:
        click.echo(f"Dispatches deleted: {success_ids}")  # Feedback on deleted IDs
    if failed_ids:
        click.echo(f"Failed to delete: {failed_ids}", err=True)

    if failed_ids:
        if not success_ids:
            raise click.ClickException("All deletions failed.")
        raise click.ClickException("Some deletions failed.")


async def interactive_mode(url: str, key: str) -> None:
    """Interactive mode for the CLI."""
    hist_file = os.path.expanduser("~/.dispatch_cli_history.txt")
    session: PromptSession[str] = PromptSession(history=FileHistory(filename=hist_file))

    user_commands = [
        "list",
        "stream",
        "create",
        "update",
        "get",
        "delete",
        "exit",
        "help",
    ]

    async def display_help() -> None:
        await cli.main(args=["--help"], standalone_mode=False)

    completer = NestedCompleter.from_nested_dict(
        {command: None for command in user_commands}
    )

    while True:
        with patch_stdout():
            try:
                user_input = await session.prompt_async(
                    "> ",
                    completer=completer,
                    complete_style=CompleteStyle.READLINE_LIKE,
                )
            except EOFError:
                break

        if user_input == "help" or not user_input:
            await display_help()
        elif user_input == "exit":
            break
        else:
            # Split, but keep quoted strings together
            params = [
                "--url",
                url,
                "--key",
                key,
            ] + click.parser.split_arg_string(user_input)

            try:
                await cli.main(args=params, standalone_mode=False)
            except click.ClickException as e:
                click.echo(e)


# Add recurrence options to the create command
create.params += recurrence_options
# Add recurrence options to the update command
update.params += recurrence_options


def main() -> None:
    """Entrypoint for the CLI."""
    asyncio.run(cli.main())


if __name__ == "__main__":
    main()
