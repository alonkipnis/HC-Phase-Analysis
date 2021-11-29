"""
This is a boilerplate pipeline 'illustrate'
generated using Kedro 0.17.0
"""

from kedro.pipeline import Pipeline, node
from .nodes import (
    compute_success_probability,
    prepare_results,
    illustrate_phase_diagram,
illustrate_fixed_x
)


def create_pipeline(**kwargs):
    return Pipeline([node(func=prepare_results,
                          inputs=["evaluation_res", "params:evaluate_probs"],
                          outputs="filtered_res",
                          name="read_results"
                          ),
                     node(func=compute_success_probability,
                          inputs=["filtered_res", "params:evaluate_probs"],
                          outputs="success_probs",
                          name="compute_success_probability"
                          ),
                     node(func=illustrate_fixed_x,
                          inputs=["success_probs", "params:illustrate"],
                          outputs=None,
                          name="illustrate_fixed_x"
                          ),
                     node(func=illustrate_phase_diagram,
                          inputs=["success_probs", "params:illustrate"],
                          outputs=None,
                          name="illustrate_phase_diagram"
                          )
                     ], tags="Illustrate")
