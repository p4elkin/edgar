import React from "react";
import "./App.css";
import {Filings} from "./components/Filings";
import {StateProvider} from "./state";
import {FilterBar} from "./components/filter";
import {MissingRevenueList} from "./components/Errors";

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
      startDate: new Date().getTime() - 7 * 24 * 60 * 60 * 1000,
      endDate: new Date().getTime(),
      company: null,
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
