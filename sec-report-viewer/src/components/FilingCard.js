import React, {useEffect, useState} from "react";
import {toPercents} from "../converters";

export const FilingCard = () => {
    const [filings, setFilings] = useState([])

    useEffect(() => {
        async function loadFilings() {
            let fetchedFilings = await fetch("http://localhost:8888/latestFilings");
            let filingsData = await fetchedFilings.json();
            setFilings(filingsData)
        }

        loadFilings()
    }, []);

    return <>{
            filings.map(filing => (
                <div key={filing.ticker}>
                    <span style={{display: 'inline-block', width: '400px'}}>
                        <b style={{paddingRight: '10px'}}>{filing.name}</b>
                        <span>({filing.type})</span>
                        <span>({toPercents(filing.epsYY)})%</span>
                    </span>
                    <a href={filing.interactiveData}>interactive data</a>
                </div>
            ))
    }</>
}
