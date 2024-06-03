# License: All rights reserved
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""CLI and interactive client for the dispatch service."""

import asyncio
import os
import sys
from pprint import pformat
from typing import Any, List

import asyncclick as click
import grpc
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import NestedCompleter
from prompt_toolkit.history import FileHistory
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.shortcuts import CompleteStyle

from frequenz.client.dispatch.types import (
    EndCriteria,
    Frequency,
    RecurrenceRule,
    Weekday,
)

from ._cli_types import (
    FuzzyDateTime,
    FuzzyIntRange,
    FuzzyTimeDelta,
    JsonDictParamType,
    SelectorParamType,
)
from ._client import Client

DEFAULT_DISPATCH_API_HOST = "88.99.25.81"
DEFAULT_DISPATCH_API_PORT = 50051


def get_client(host: str, port: int) -> Client:
    """Get a new client instance.

    Args:
        host: The host of the dispatch service.
        port: The port of the dispatch service.

    Returns:
        Client: A new client instance.
    """
    channel = grpc.aio.insecure_channel(f"{host}:{port}")
    return Client(channel, f"{host}:{port}")


# Click command groups
@click.group()
@click.option(
    "--host",
    default=DEFAULT_DISPATCH_API_HOST,
    help="Dispatch API host",
    envvar="DISPATCH_API_HOST",
    show_envvar=True,
    show_default=True,
)
@click.option(
    "--port",
    default=DEFAULT_DISPATCH_API_PORT,
    help="Dispatch API port",
    envvar="DISPATCH_API_PORT",
    show_envvar=True,
    show_default=True,
)
@click.pass_context
async def cli(ctx: click.Context, host: str, port: int) -> None:
    """Dispatch Service CLI."""
    ctx.ensure_object(dict)
    ctx.obj["client"] = get_client(host, port)


@cli.command("list")
@click.pass_context
@click.argument("microgrid-id", required=True, type=int)
@click.option("--selector", "-s", type=SelectorParamType(), multiple=True)
@click.option("--start-from", type=FuzzyDateTime())
@click.option("--start-to", type=FuzzyDateTime())
@click.option("--end-from", type=FuzzyDateTime())
@click.option("--end-to", type=FuzzyDateTime())
@click.option("--active", type=bool)
@click.option("--dry-run", type=bool)
async def list_(ctx: click.Context, /, **filters: Any) -> None:
    """List dispatches.

    Lists all dispatches for MICROGRID_ID that match the given filters.

    The selector option can be given multiple times.
    """
    if "selector" in filters:
        selector = filters.pop("selector")
        filters["component_selectors"] = selector

    num_dispatches = 0
    async for dispatch in ctx.obj["client"].list(**filters):
        click.echo(pformat(dispatch, compact=True))
        num_dispatches += 1

    click.echo(f"{num_dispatches} dispatches total.")


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
        default=0,
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
@click.argument("start-time", required=True, type=FuzzyDateTime())
@click.argument("duration", required=True, type=FuzzyTimeDelta())
@click.argument("selector", required=True, type=SelectorParamType())
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

    SELECTOR is either one of the following: BATTERY, GRID, METER, INVERTER,
    EV_CHARGER, CHP or a list of component IDs separated by commas, e.g. "1,2,3".
    """
    # Remove keys with `None` value
    kwargs = {k: v for k, v in kwargs.items() if v is not None}

    dispatch = await ctx.obj["client"].create(
        _type=kwargs.pop("type"),
        recurrence=parse_recurrence(kwargs),
        **kwargs,
    )
    click.echo(pformat(dispatch, compact=True))
    click.echo("Dispatch created.")


# We could fix the mypy error by using ", /", but this causes issues with
# the click decorators. We can ignore the error here.
@cli.command()  # type: ignore[arg-type]
@click.argument("dispatch_id", type=int)
@click.option("--start-time", type=FuzzyDateTime())
@click.option("--duration", type=FuzzyTimeDelta())
@click.option("--selector", type=SelectorParamType())
@click.option("--active", type=bool)
@click.option(
    "--payload", "-p", type=JsonDictParamType(), help="JSON payload for the dispatch"
)
@click.pass_context
async def update(
    ctx: click.Context, dispatch_id: int, **new_fields: dict[str, Any]
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

    try:
        await ctx.obj["client"].update(dispatch_id=dispatch_id, new_fields=new_fields)
        click.echo("Dispatch updated:")
        click.echo(pformat(await ctx.obj["client"].get(dispatch_id), compact=True))
    except grpc.RpcError as e:
        raise click.ClickException(f"Update failed: {e}")


@cli.command()
@click.argument("dispatch_ids", type=int, nargs=-1)  # Allow multiple IDs
@click.pass_context
async def get(ctx: click.Context, dispatch_ids: List[int]) -> None:
    """Get one or multiple dispatches."""
    num_failed = 0

    for dispatch_id in dispatch_ids:
        try:
            dispatch = await ctx.obj["client"].get(dispatch_id)
            click.echo(pformat(dispatch, compact=True))
        except grpc.RpcError as e:
            click.echo(f"Error getting dispatch {dispatch_id}: {e}", err=True)
            num_failed += 1

    if num_failed == len(dispatch_ids):
        raise click.ClickException("All gets failed.")
    if num_failed > 0:
        raise click.ClickException("Some gets failed.")


@cli.command()
@click.argument("dispatch_ids", type=FuzzyIntRange(), nargs=-1)  # Allow multiple IDs
@click.pass_context
async def delete(ctx: click.Context, dispatch_ids: list[list[int]]) -> None:
    """Delete multiple dispatches.

    Possible formats: "1", "1,2,3", "1-3", "1..3"
    """
    # Flatten the list of lists
    flat_ids = [dispatch_id for sublist in dispatch_ids for dispatch_id in sublist]
    failed_ids = []
    success_ids = []

    for dispatch_id in flat_ids:
        try:
            await ctx.obj["client"].delete(dispatch_id)
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


async def interactive_mode() -> None:
    """Interactive mode for the CLI."""
    hist_file = os.path.expanduser("~/.dispatch_cli_history.txt")
    session: PromptSession[str] = PromptSession(history=FileHistory(filename=hist_file))

    user_commands = ["list", "create", "update", "get", "delete", "exit", "help"]

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
            params = click.parser.split_arg_string(user_input)
            try:
                await cli.main(args=params, standalone_mode=False)
            except click.ClickException as e:
                click.echo(e)


# Add recurrence options to the create command
create.params += recurrence_options  # pylint: disable=no-member
# Add recurrence options to the update command
update.params += recurrence_options  # pylint: disable=no-member


def main() -> None:
    """Entrypoint for the CLI."""
    if len(sys.argv) > 1:
        asyncio.run(cli.main())
    else:
        asyncio.run(interactive_mode())


if __name__ == "__main__":
    main()