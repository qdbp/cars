from dash.dependencies import Input, Output, State

from ..layout import (
    ALERT_MM_PICKER,
    INPID_MM_PICKER,
    INPID_OPTS_BODY,
    INPID_OPTS_DRIVETRAIN,
    INPID_OPTS_FUEL,
    INPID_OPTS_TRANS,
    IVAL_TRIGGER_LOAD,
    SLIDER_INPUTS,
    STORE_ALL_CARS,
    STORE_FILTERED_CARS,
    ToggleButtonGroup,
)
from . import deferred_clientside_callback

ERR_INSANE_SELECTORS = "insane-selectors"
ERR_NO_CARS_MATCH = "no-cars-match"


deferred_clientside_callback(
    "filter-ymmt-by-selection",
    # language=js
    """
    function(
        _trigger,
        year_range, // [int, int]
        mpg_range, // [int, int]
        want_trans, // [bool, ...]
        want_dvt, // [bool, ...]
        want_fuel, // [bool, ...]
        want_body, // [bool, ...] 
        all_data
    ) {
    
        const [ymin, ymax] = year_range
        const [mpgmin, mpgmax] = mpg_range
        
        const cars = all_data['attrs']
        const prop_to_ix = all_data['prop_to_ix']
        
        let check_props = {
            'is_auto': want_trans,
            'drivetrain': want_dvt,
            'fuel_type': want_fuel,
            'body': want_body
        };
        
        for (const [_, val] of Object.entries(check_props)) {
            if (val.every(it => !it)) {
                return "INSANE_OPTIONS"
            }
        }
        
        for (const key in check_props) {
            if (check_props[key].every(Boolean)) {
                delete check_props[key];
            }
        }
        
        return cars.filter(
            car => (
                (car['mpg'] >= mpgmin) && (car['mpg'] <= mpgmax) &&
                (car['year'] >= ymin) && (car['year'] <= ymax) &&
                Object.entries(check_props).map(
                    it => {
                        const [name, want] = it;
                        return want[prop_to_ix[name][car[name]]]
                    }
                ).every(Boolean)
            )
        )
    }
    """,
    Output(STORE_FILTERED_CARS, "data"),
    [
        Input(IVAL_TRIGGER_LOAD, "n_intervals"),
        SLIDER_INPUTS["year"],
        SLIDER_INPUTS["mpg"],
        Input(ToggleButtonGroup.selector(input=INPID_OPTS_TRANS), "active"),
        Input(
            ToggleButtonGroup.selector(input=INPID_OPTS_DRIVETRAIN), "active"
        ),
        Input(ToggleButtonGroup.selector(input=INPID_OPTS_FUEL), "active"),
        Input(ToggleButtonGroup.selector(input=INPID_OPTS_BODY), "active"),
    ],
    [
        State(STORE_ALL_CARS, "data"),
    ],
    prevent_initial_call=True,
)


deferred_clientside_callback(
    "restrict-mm-picker-options",
    # language=js
    """
    function(
        timestamp, // int... oh wait lol, "number" or whatever
        filtered_cars // [car_dict, ...]
    ) {
        // this is an initialization call we should skip
        if (!filtered_cars) {
            return;
        }
        
        if (typeof filtered_cars == "string") {
            return [[], "Your selected options exclude all cars.", "danger"]
        }
        let ids = {};
        filtered_cars.forEach( car => {
            const make = car['make'];
            const model = car['model'];
            const id = `${make};;;${model}`;
            if (!(id in ids)) {
                ids[id] = {
                    label: `${make} ${model}`,
                    value: id
                };
            }
        })
        if (Object.keys(ids).length === 0) {
            return [[], "No makes and models meet your criteria", "danger"];
        }
        return [
            Object.values(ids),
            "Please select your makes and models.",
            "primary"
        ];
    }
    """,
    [
        Output(INPID_MM_PICKER, "options"),
        Output(ALERT_MM_PICKER, "children"),
        Output(ALERT_MM_PICKER, "color"),
    ],
    Input(STORE_FILTERED_CARS, "modified_timestamp"),
    State(STORE_FILTERED_CARS, "data"),
    prevent_initial_call=True,
)
