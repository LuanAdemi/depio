from .BuildMode import BuildMode
from .Pipeline import Pipeline
from .Task import Task


def task(name: str | None, pipeline: Pipeline | None = None, buildmode: BuildMode = BuildMode.IF_MISSING,
         max_age: float = None, track_code: bool = False):
    def wrapper(func):
        def decorator(*func_args, **func_kwargs):
            # Build the Task object; do NOT call the underlying function here.
            t = Task(name, func=func, buildmode=buildmode, max_age=max_age, track_code=track_code,
                     func_args=func_args, func_kwargs=func_kwargs)

            if pipeline:
                pipeline.add_task(t)

            return t

        return decorator

    return wrapper


__all__ = ["task"]
