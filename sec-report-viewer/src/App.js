import React, {useEffect, useState} from "react";
import "./App.css";
import {StateProvider} from "./state";
import {FilterBar} from "./components/filter";
import {MissingRevenueList} from "./components/Errors";
import {Filings} from "./components/Filings";
import {location} from "./commons";
import {FilingAmendmentPopup} from "./components/FilingPopup";

const App = () => {
    // const [filing, setFiling] = useState(null)
    // useEffect(() => {
    //     async function loadData() {
    //         let fetchedFilings = await fetch(`${location}/filing?id=5f39ab783db3f9b3cc9aa929`, {
    //             headers: {
    //                 'Content-Type': 'application/json'
    //             }
    //         });
    //         if (fetchedFilings.ok) {
    //             let json = await fetchedFilings.json();
    //             setFiling(json)
    //         }
    //     }
    //
    //     return loadData()
    // }, []);
    //
    // if (filing !== null) {
    //     return (<FilingAmendmentPopup filing={filing}/>)
    // } else {
        return (
            <>
                <FilterBar/>
                <Filings/>
            </>);
    // }

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
