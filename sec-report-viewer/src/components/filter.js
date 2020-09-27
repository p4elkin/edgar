import {KeyboardDatePicker, MuiPickersUtilsProvider} from '@material-ui/pickers';
import React from "react";
import DateFnsUtils from '@date-io/date-fns';
import styled from "styled-components";
import {useGlobalState} from "../state";
import TextField from "@material-ui/core/TextField";
import {DelayInput} from "react-delay-input";
import FormControlLabel from "@material-ui/core/FormControlLabel";
import Checkbox from "@material-ui/core/Checkbox";

const DatePicker = ({value, onChange}) => {
    return (<KeyboardDatePicker
        margin="normal"
        id="date-picker-dialog"
        label="Show filings before"
        format="MM/dd/yyyy"
        value={value}
        onChange={(arg) => onChange(arg)}
        KeyboardButtonProps={{
            'aria-label': 'change date',
        }}
    />)

}

export const FilterBar = () => {
    const [{filter}, dispatch] = useGlobalState()
    return (<Styles>
            <MuiPickersUtilsProvider utils={DateFnsUtils}>
                <DatePicker
                    value={filter.startDate || new Date(2011, 1, 1).getTime()}
                    onChange={(date) => {
                        dispatch({type:'updateFilter', updatedFilter: {...filter, startDate: date.getTime()}})
                    }}/>
                <DatePicker
                    value={filter.endDate || new Date().getTime()}
                    onChange={(date) => {
                        dispatch({type:'updateFilter', updatedFilter: {...filter, endDate: date.getTime()}})
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
            <FormControlLabel
                value="end"
                control={
                    <Checkbox
                        color="primary"
                        margin="normal"
                    />}
                margin="normal"
                label="10-K only"
                labelPlacement="end"
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


