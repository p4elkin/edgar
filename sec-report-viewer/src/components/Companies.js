import React, {useEffect, useState} from "react";
import {location} from "../commons";
import Autocomplete from '@material-ui/lab/Autocomplete';
import TextField from "@material-ui/core/TextField";
import {CompanyInfo} from "./companyInfo";
import Autosuggest from 'react-autosuggest';
import IsolatedScroll from "react-isolated-scroll";
import styled from "styled-components";

export const Companies = () => {
    const [tickers, setTickers] = useState([])
    const [suggestedTickers, setSuggestedTickers] = useState([])
    const [selectedTicker, setSelectedTicker] = useState('AAPL')
    const loadData =  async () => {
        let fetchedFilings = await fetch(`${location}/tickers`);
        let json = await fetchedFilings.json()
        setTickers(json)
    }

    useEffect(() => {
        loadData()
    }, []);

    const updateTicker = (event, {newValue}) => {
        setSelectedTicker(newValue)
    }
    // Autosuggest will call this function every time you need to update suggestions.
    // You already implemented this logic above, so just use it.
    const onSuggestionsFetchRequested = ({ value }) => {
        setSuggestedTickers(tickers.filter((ticker) => ticker.startsWith(value)))
    };
    // Autosuggest will call this function every time you need to clear suggestions.
    const onSuggestionsClearRequested = () => {
        setSuggestedTickers([])
    };

    const inputProps = {
        placeholder: 'Enter ticker',
        value: selectedTicker,
        onChange: updateTicker
    };

    const renderSuggestion = suggestion => (
        <div>
            {suggestion}
        </div>
    );

    const renderSuggestionsContainer = ({ containerProps, children }) => {
        const { ref, ...restContainerProps } = containerProps;
        const callRef = isolatedScroll => {
            if (isolatedScroll !== null) {
                ref(isolatedScroll.component);
            }
        };

        return (
            <IsolatedScroll ref={callRef} {...restContainerProps}>
                {children}
            </IsolatedScroll>
        );
    }

    return (<>
        <Styled>
            <Autosuggest
                suggestions={suggestedTickers}
                onSuggestionsFetchRequested={onSuggestionsFetchRequested}
                onSuggestionsClearRequested={onSuggestionsClearRequested}
                getSuggestionValue={v => v}
                renderSuggestion={renderSuggestion}
                renderSuggestionsContainer={renderSuggestionsContainer}
                inputProps={inputProps}
            />
            {selectedTicker ? <CompanyInfo ticker={selectedTicker}/> : <></>}
        </Styled>
    </>)
};

const Styled = styled.div`
.react-autosuggest__container {
  position: relative;
}

.react-autosuggest__input {
  width: 240px;
  height: 30px;
  padding: 10px 20px;
  font-family: Helvetica, sans-serif;
  font-weight: 300;
  font-size: 16px;
  border: 1px solid #aaa;
  border-radius: 4px;
}

.react-autosuggest__input--focused {
  outline: none;
}

.react-autosuggest__input--open {
  border-bottom-left-radius: 0;
  border-bottom-right-radius: 0;
}

.react-autosuggest__suggestions-container {
  display: none;
}

.react-autosuggest__suggestions-container--open {
  display: block;
  position: absolute;
  top: 51px;
  width: 280px;
  border: 1px solid #aaa;
  background-color: #fff;
  font-family: Helvetica, sans-serif;
  font-weight: 300;
  font-size: 16px;
  border-bottom-left-radius: 4px;
  border-bottom-right-radius: 4px;
  z-index: 2;
}

.react-autosuggest__suggestions-list {
  margin: 0;
  padding: 0;
  list-style-type: none;
}

.react-autosuggest__suggestion {
  cursor: pointer;
  padding: 10px 20px;
}

.react-autosuggest__suggestion--highlighted {
  background-color: #ddd;
}
`

