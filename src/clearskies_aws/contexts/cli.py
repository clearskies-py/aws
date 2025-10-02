from __future__ import annotations

from clearskies import contexts

from clearskies_aws.contexts import context


class Cli(context.Context, contexts.Cli):
    """
    Run an application via a CLI command.

    Extend from the core CLI context,
    but with an override of the DI to use clearskies_aws.di.Di().
    """
