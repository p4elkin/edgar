import React, {useEffect, useState} from "react";
import _ from "lodash";

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

    const groupedByDate = _.groupBy(filings, 'date');
    return <>{groupedByDate.map((date, filings) => (
        <div>
            {filings.map(filing =>
                <div style={{margin: '5px'}}>
                    <span style={{display: 'inline-block', width: '500px'}}>
                        <b style={{paddingRight: '10px'}}>{filing.name}</b>
                        <span>({filing.type})</span>
                    </span>
                    <a href={filing.interactiveData}>interactive data</a>
                </div>
            )}
        </div>
        ))
    }</>
}
