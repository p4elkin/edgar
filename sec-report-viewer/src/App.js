import React from "react";
import "./App.css";
import {Filings} from "./components/Grid";
import {StateProvider} from "./state";
import {FilterBar} from "./components/filter";

function App() {
  return (
      <>
        <FilterBar/>
        <Filings/>
      </>);
}

const AppWithStateProvider = () => {

  const initState = {
    filter: {
      minRevenue: 1000000000,
      startDate: new Date().getTime(),
      company: "aapl",
      industryCode: null
    }
  };

  const appStateReducer = ({filter}, action) => ({
    filter: filterReducer(filter, action),
  });

  const filterReducer = (filter, action) => {
    return action.updatedFilter || filter;
  };

  return <StateProvider initialState={initState} reducer={appStateReducer}>
      <App/>
  </StateProvider>
};

export default AppWithStateProvider;
