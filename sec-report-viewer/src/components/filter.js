import {KeyboardDatePicker, MuiPickersUtilsProvider} from '@material-ui/pickers';
import React from "react";
import DateFnsUtils from '@date-io/date-fns';
import styled from "styled-components";
import {useGlobalState} from "../state";
import TextField from "@material-ui/core/TextField";
import {DelayInput} from "react-delay-input";

export const FilterBar = () => {
    const [{filter}, dispatch] = useGlobalState()
    return (<Styles>
            <MuiPickersUtilsProvider utils={DateFnsUtils}>
                <KeyboardDatePicker
                    margin="normal"
                    id="date-picker-dialog"
                    label="Show filings before"
                    format="MM/dd/yyyy"
                    value={filter.startDate}
                    onChange={(date) => {
                        dispatch({type:'updateFilter', updatedFilter: {...filter, startDate: date.getTime()}})
                    }}
                    KeyboardButtonProps={{
                        'aria-label': 'change date',
                    }}
                />
            </MuiPickersUtilsProvider>
            <DelayInput element={TextField}
                delayTimeout={300}
                margin="normal"
                className="company-filter"
                id="company-field"
                label="Company"
                value={filter.company}
                onChange={(event) => {
                    dispatch({type:'updateFilter', updatedFilter: {...filter, company: event.target.value}})
                }}
            />
        </Styles>
    )
};

const Styles = styled.div`
    padding-left: 1em;
    
    .company-filter {
        margin-left: 1em
    }
`;


