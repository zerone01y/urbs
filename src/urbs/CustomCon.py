import pyomo.core as pyomo


def add_custom_constraints(self, *args) -> tuple:
    """usage: add constraint to an existing model

    Args:
        self:

    usage:
    >>> from ubbs.features import add_custom_constraints
    >>> pyomo_model.add_custom_cosntraints(custom_constraint_functions)

    Returns:
        tuples of custom constraints

    """

    if args:
        for i in args:
            try:
                i(self)
            except TypeError:
                raise TypeError(f"Custom constraint {i} undefined")
    return ()


pyomo.ConcreteModel.add_custom_constraints = add_custom_constraints


def example_constraint(self):
    self.example_constraint = pyomo.Constraint(
        doc="""
    pyomo document available at 
    https://pyomo.readthedocs.io/en/stable/pyomo_modeling_components/Constraints.html
    """
    )
    return
