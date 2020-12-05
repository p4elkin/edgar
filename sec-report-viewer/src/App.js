import React, {useEffect, useState} from "react";
import "./App.css";
import {StateProvider} from "./state";
import {FilterBar} from "./components/filter";
import {MissingRevenueList} from "./components/Errors";
import {Filings} from "./components/Filings";
import {location} from "./commons";
import {FilingAmendmentPopup} from "./components/FilingPopup";
import {CompanyInfo} from "./components/companyInfo";
import {
    BrowserRouter as Router,
    Switch,
    Route,
    Link
} from "react-router-dom";
import {Companies} from "./components/Companies";

const App = () => {
    return (
        <>
            <Router>
                <div>
                    <Switch>
                        <Route path="/companies">
                            <Companies/>
                        </Route>
                        <Route path="/">
                            <FilterBar/>
                            <Filings/>
                        </Route>
                    </Switch>
                </div>
            </Router>
        </>);
}

const AppWithStateProvider = () => {

    const initState = {
        filter: {
            revenueThreshold: 1000000000,
            startDate: new Date().getTime() - 7 * 24 * 60 * 60 * 1000,
            endDate: new Date().getTime(),
            company: null,
            industryCode: null,
            withMissingRevenue: false,
            annualOnly: false,
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
