from loguru import logger
import pathlib
import shutil

try:
    isinstance("", basestring)

    def is_string(s):
        return isinstance(s, basestring)  # Python 3

except NameError:

    def is_string(s):
        return isinstance(s, str)  # Python 2


def write_model(prob, result_dir):
    prob.write(
        str(pathlib.Path(result_dir, "model.lp")),
        io_options={"symbolic_solver_labels": True},
    )


def delete_folder(result_dir):
    logger.remove()
    return shutil.rmtree(result_dir)
