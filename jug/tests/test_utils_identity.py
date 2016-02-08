from jug.utils import identity
from .task_reset import task_reset

@task_reset
def test_utils_identity():
    t = identity(2)
    t.run()
    assert t.value() == 2
