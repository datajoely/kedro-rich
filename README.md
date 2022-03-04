# kedro-rich

## Make your Kedro snazzy

This is a very very work in progress Kedro plugin that utilises the awesome rich library.

![terminal output](static/rich-kedro.gif)

The intention with this piece of work is to battle test the idea, iron out the creases potentially to integrate this as a 1st class plugin hosted at kedro-org/plugins or if we're lucky, native functionality within Kedro itself.

I'm very much looking for help developing/testing this project so if you want to get involved please get in touch.

## Current Functionality

### `kedro rrun` command

- Does exactly the same as a regular Kedro run but kicks the progress bars into account.
- The load/save progress tasks focus purely on persisted data and ignore ephemeral `MemoryDataSets`.
- The progress bars are not enabled since `MultiProcessing` is causing issues between `kedro` and `rich`. Further investigation if some sort of `Lock()` mechanism will allow for this to work.
- I'm not wedded to the command name, so open to suggestions.

### Logging via `rich.logging.RichHandler`

- This plugin changes the default `stdout` console logging handler in place of the class provided by `rich` .
- This is actually required to make the progress bars work without being broken onto new lines every time a new log message appears.
- In order enable this purely plug-in side (i.e. not making the user change `logging.yml`) I've had to do an ugly bit of monkey patching. Keen to come up with a better solution here.

# Install the plug-in

The plug-in is in very early days so it will be a while before (if) this makes it to pypi
1. Run `pip install git+https://github.com/datajoely/kedro-rich` to install this to your environment.
2. Go to any Kedro 0.17.x project and see if it works! (Please let me know if it doesn't).

## Run end to end example:

Running `make test-project` will...

- Install the `kedro-rich` package into the environment
- Pull the 'spaceflights' `kedro-starter`
- Install requirements
- Execute `kedro rrun`
---------------------

## Potential future ideas

### `kedro catalog list` and `kedro registry list`

- The current catalog functionality prints out a long list of DataSets grouped by pipeline and DataSet type

    ```text

    DataSets in '__default__' pipeline:
    Datasets mentioned in pipeline:
        CSVDataSet:
        - reviews
        - companies
        DefaultDataSet:
        - data_science.candidate_modelling_pipeline.regressor
        ...
    ```

- I'm certain there is a better, rich powered implementation of this!

### `kedro jupyter` and  `kedro ipython`

- We could change the current init process that as well as making the `catalog`, `session` and other objects available to the user we could also overload the `print` statement.
- That way the user gets [pretty notebooks](https://www.willmcgugan.com/blog/tech/post/rich-adds-support-for-jupyter-notebooks/) for free!]

    ```python
    from rich.jupyter import print
    ```
